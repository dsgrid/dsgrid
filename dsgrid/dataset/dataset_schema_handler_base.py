import abc
import logging
import os
from collections import defaultdict

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimensionMapping
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.spark import models_to_dataframe
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """ define interface/required behaviors per dataset schema """

    def __init__(self, config, dimension_mgr, dimension_mapping_mgr, mapping_references=None):
        self._config = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references = mapping_references

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
    def _remap_dimension_columns(self, df):
        # This will likely become a common activity when running queries.
        # May need to cache the result. But that will likely be in Project, not here.
        # This method could be moved elsewhere.
        for ref in self._mapping_references:
            column = ref.from_dimension_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version
            )
            records = mapping_config.model.records
            df = self._map_and_reduce_dimension(df, column, records)

        return df

    @track_timing(timer_stats_collector)
    def _map_and_reduce_dimension(self, df, column, records):
        """ Map and partially reduce a dimension """
        if column not in df.columns:
            return df

        if column == self.get_pivot_dimension_type().value:
            # map and consolidate pivoted dimension columns (need pytest)
            # this is only for dataset_schema_handler_one_table, b/c handlder_standard._remap_dimension_columns() only takes in load_data_lookup
            nonvalue_cols = list(
                set(df.columns).difference(set(self.get_pivot_dimension_columns()))
            )

            records_dict = defaultdict(dict)
            for row in records:
                if row.to_id is not None:
                    records_dict[row.to_id][row.from_id] = row.from_fraction

            to_ids = sorted(records_dict)
            value_operations = []
            for tid in to_ids:
                operation = "+".join(
                    [f"{from_id}*{fraction}" for from_id, fraction in records_dict[tid].items()]
                )  # assumes reduce by summation
                operation += f" AS {tid}"
                value_operations.append(operation)

            df = df.selectExpr(*nonvalue_cols, *value_operations)

        else:
            # map and consolidate from_fraction only
            records = models_to_dataframe(records).filter("to_id IS NOT NULL")
            df = df.withColumn("fraction", F.lit(1))
            df = (
                df.join(records, on=df[column] == records.from_id, how="cross")
                .drop("from_id")
                .drop(column)
                .withColumnRenamed("to_id", column)
            ).filter(f"{column} IS NOT NULL")
            nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
            df = df.fillna(1, subset=["from_fraction"]).selectExpr(
                *nonfraction_cols, "fraction*from_fraction AS fraction"
            )

            # After remapping, rows in load_data_lookup for standard_handler and rows in load_data for one_table_handler may not be unique;
            # imagine 5 subsectors being remapped/consolidated to 2 subsectors.

        return df

    @staticmethod
    @track_timing(timer_stats_collector)
    def _check_null_value_in_unique_dimension_rows(dim_table):
        if os.environ.get("__DSGRID_SKIP_NULL_UNIQUE_DIMENSION_CHECK__"):
            # This has intermittently caused GC-related timeouts for TEMPO.
            # Leave a backdoor to skip these checks, which may eventually be removed.
            logger.warning("Skip _check_null_value_in_unique_dimension_rows")
            return

        dim_with_null = set()
        cols_to_check = {x for x in dim_table.columns if x != "id"}

        for col in cols_to_check:
            if not dim_table.select(col).filter(f"{col} is NULL").rdd.isEmpty():
                dim_with_null.add(col)

        if dim_with_null:
            raise DSGInvalidDimensionMapping(
                "Invalid dimension mapping application. "
                f"Combination of remapped dataset dimensions contain NULL value(s) for dimension(s): \n{dim_with_null}"
            )
