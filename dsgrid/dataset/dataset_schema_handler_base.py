import abc
import logging
import os
from collections import defaultdict

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimensionMapping,
    DSGInvalidField,
    DSGInvalidParameter,
)
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.spark import check_for_nulls
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """define interface/required behaviors per dataset schema"""

    def __init__(
        self,
        config,
        dimension_mgr,
        dimension_mapping_mgr,
        mapping_references=None,
        project_time_dim=None,
    ):
        self._config = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references = mapping_references
        self._project_time_dim = project_time_dim

    @classmethod
    @abc.abstractmethod
    def load(cls, config: DatasetConfig):
        """Create a dataset schema handler by loading the data tables from files.

        Parameters
        ----------
        config: DatasetConfig

        Returns
        -------
        DatasetSchemaHandlerBase

        """

    @abc.abstractmethod
    def check_consistency(self):
        """
        Check all data consistencies, including data columns, dataset to dimension records, and time
        """

    @abc.abstractmethod
    def get_unique_dimension_rows(self):
        """Return a dataframe containing unique dimension combinations that exist in the rows of
        the data table.

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @property
    def dataset_id(self):
        return self._config.config_id

    @property
    def config(self):
        """Returns the DatasetConfig.

        Returns
        -------
        DatasetConfig

        """
        return self._config

    def get_pivot_dimension_type(self):
        """Return the DimensionType of the columns pivoted in the load_data table.

        Returns
        -------
        DimensionType

        """
        return self._config.model.data_schema.load_data_column_dimension

    def get_pivot_dimension_columns(self):
        """Get cols for the dimension that is pivoted in load_data.

        Returns
        -------
        List: List of column names.

        """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return sorted(list(self._config.get_dimension(dim_type).get_unique_ids()))

    @track_timing(timer_stats_collector)
    def get_pivot_dimension_columns_mapped_to_project(self):
        """Get columns for the dimension that is pivoted in load_data and remap them to the
        project's record names. The returned dict will not include columns that the project does
        not care about.

        Returns
        -------
        dict
            Mapping of pivoted dimension column names to project record names.

        """
        columns = set(self.get_pivot_dimension_columns())
        dim_type = self.get_pivot_dimension_type()
        for ref in self._mapping_references:
            if ref.from_dimension_type == dim_type:
                mapping_config = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                )
                mapping = {
                    x.from_id: x.to_id for x in mapping_config.model.records if x.to_id is not None
                }

                diff = set(mapping.keys()).difference(columns)
                if diff:
                    raise DSGInvalidDataset(
                        f"Dimension_mapping={mapping_config.config_id} has more from_id records than the dataset pivoted {dim_type.value} dimension: {diff}"
                    )
                return set(mapping.values())

        return columns

    @track_timing(timer_stats_collector)
    def get_columns_for_unique_arrays(self, time_dim, load_data_df):
        """Returns the list of dimension columns aginst which the number of timestamps is checked.

        Returns
        -------
        List: List of column names.

        """
        time_col = time_dim.get_timestamp_load_data_columns()
        pivot_cols = self.get_pivot_dimension_columns()
        groupby_cols = list(set(load_data_df.columns).difference(set(pivot_cols + time_col)))

        return groupby_cols

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency(self, load_data_df):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        """
        if os.environ.get("__DSGRID_SKIP_DATASET_TIME_CONSISTENCY_CHECKS__"):
            logger.warning("Skip dataset time consistency checks.")
            return

        logger.info("Check dataset time consistency.")
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
        if time_dim.model.time_type != TimeDimensionType.NOOP:
            self._check_dataset_time_consistency_by_time_array(time_dim, load_data_df)

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency_by_time_array(self, time_dim, load_data_df):
        """Check that each unique time array has the same timestamps."""
        logger.info("Check dataset time consistency by time array.")
        unique_array_cols = self.get_columns_for_unique_arrays(time_dim, load_data_df)
        time_cols = time_dim.get_timestamp_load_data_columns()
        counts = load_data_df.groupBy(*time_cols).count().select("count")
        distinct_counts = counts.select("count").distinct().collect()
        if len(distinct_counts) != 1:
            raise DSGInvalidDataset(
                f"All time arrays must have the same times: unique timestamp counts = {len(distinct_counts)}"
            )
        unique_ta_counts = load_data_df.select(*unique_array_cols).distinct().count()
        if unique_ta_counts != distinct_counts[0]["count"]:
            raise DSGInvalidDataset(
                f"dataset has invalid timestamp counts, expected {unique_ta_counts} count for "
                f"each timestamp in {time_cols} but found {distinct_counts[0]['count']} instead"
            )

    @track_timing(timer_stats_collector)
    def _filter_project_dimensions(self, df, query_context):
        for dim_filter in query_context.model.dimension_filters:
            dim_type = dim_filter.dimension_type
            for ref in self._mapping_references:
                if ref.from_dimension_type == dim_type and ref.to_dimension_type == dim_type:
                    mapping_config = self._dimension_mapping_mgr.get_by_id(
                        ref.mapping_id, version=ref.version
                    )
                    records = (
                        mapping_config.get_records_dataframe()
                        .filter(dim_filter.where_clause(column="to_id"))
                        .collect()
                    )
                    if not records:
                        raise DSGInvalidDimensionMapping(
                            f"No dataset mapping exists. dataset_id={self.dataset_id} {dim_filter}"
                        )
                    if len(records) == 1:
                        df = df.filter(f"{dim_type.value} == '{records[0].from_id}'")
                    else:
                        df = df.filter(f"{dim_type.value} in {[x.from_id for x in records]}")
        return df

    @track_timing(timer_stats_collector)
    def _remap_dimension_columns(self, df):
        # This will likely become a common activity when running queries.
        # May need to cache the result. But that will likely be in Project, not here.
        # This method could be moved elsewhere.

        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            column = dim_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version
            )
            records = mapping_config.get_records_dataframe()
            pivot_columns = set(self.get_pivot_dimension_columns())
            if column in df.columns:
                df = self._map_and_reduce_non_pivot_dimension(df, records, column)
            elif column == self.get_pivot_dimension_type().value and not pivot_columns.difference(
                df.columns
            ):
                df = self._map_and_reduce_pivot_dimension(df, records, pivot_columns)
            elif column == self.get_pivot_dimension_type().value and pivot_columns.intersection(
                df.columns
            ):
                raise Exception(
                    f"Unhandled case: column={column} pivot_columns={pivot_columns} "
                    f"df.columns={df.columns}"
                )
            # else nothing to do

        return df

    @track_timing(timer_stats_collector)
    def _map_and_reduce_non_pivot_dimension(self, df, records, column):
        if "fraction" not in df.columns:
            df = df.withColumn("fraction", F.lit(1))
        # map and consolidate from_fraction only
        # TODO DT: can remove this if we fix it at reg time
        records = records.filter("to_id IS NOT NULL")
        df = (
            df.join(records, on=df[column] == records.from_id, how="inner")
            .drop("from_id")
            .drop(column)
            .withColumnRenamed("to_id", column)
        ).filter(f"{column} IS NOT NULL")
        # After remapping, rows in load_data_lookup for standard_handler and rows in load_data for one_table_handler may not be unique;
        # imagine 5 subsectors being remapped/consolidated to 2 subsectors.
        nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
        df = df.fillna(1, subset=["from_fraction"]).selectExpr(
            *nonfraction_cols, "fraction*from_fraction AS fraction"
        )
        return df

    @track_timing(timer_stats_collector)
    def _map_and_reduce_pivot_dimension(
        self, df, records, pivot_columns, operation=None, keep_individual_fields=False
    ):
        if operation is None:
            operation = "sum"
        assert not keep_individual_fields, "keep_individual_fields is not supported yet"
        diff = pivot_columns.difference(df.columns)
        assert not diff, diff
        nonvalue_cols = list(set(df.columns).difference(pivot_columns))
        columns = set(df.columns)

        records_dict = defaultdict(dict)
        for row in records.collect():
            if row.to_id is not None and row.from_id in columns:
                records_dict[row.to_id][row.from_id] = row.from_fraction

        to_ids = sorted(records_dict)
        value_operations = []
        ops = {"sum": "+"}
        op = ops.get(operation)
        if op is None:
            allowed = sorted(ops.keys())
            raise DSGInvalidParameter(f"operation={operation} is not allowed. Allowed={allowed}")

        for tid in to_ids:
            operation = op.join(
                [f"{from_id}*{fraction}" for from_id, fraction in records_dict[tid].items()]
            )
            operation += f" AS {tid}"
            value_operations.append(operation)

        return df.selectExpr(*nonvalue_cols, *value_operations)

    @track_timing(timer_stats_collector)
    def _convert_time_dimension(self, load_data_df):
        # This needs to convert the time format as well as time zone (TODO).
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_dim.convert_dataframe(load_data_df, self._project_time_dim)
        return load_data_df

    @staticmethod
    @track_timing(timer_stats_collector)
    def _check_null_value_in_unique_dimension_rows(dim_table):
        if os.environ.get("__DSGRID_SKIP_NULL_UNIQUE_DIMENSION_CHECK__"):
            # This has intermittently caused GC-related timeouts for TEMPO.
            # Leave a backdoor to skip these checks, which may eventually be removed.
            logger.warning("Skip _check_null_value_in_unique_dimension_rows")
            return

        try:
            check_for_nulls(dim_table, exclude_columns={"id"})
        except DSGInvalidField as exc:
            raise DSGInvalidDimensionMapping(
                "Invalid dimension mapping application. "
                "Combination of remapped dataset dimensions contain NULL value(s) for "
                f"dimension(s): \n{str(exc)}"
            )
