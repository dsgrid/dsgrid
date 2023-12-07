import abc
import logging
import os

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

import dsgrid.units.energy as energy
from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DatasetSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidQuery
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.dataset import (
    is_noop_mapping,
    map_and_reduce_stacked_dimension,
    map_and_reduce_pivoted_dimension,
    add_time_zone,
    ordered_subset_columns,
)
from dsgrid.utils.spark import get_unique_values
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

    @abc.abstractmethod
    def filter_data(self, dimensions: list[DatasetSimpleModel]):
        """Filter the load data by dimensions and rewrite the files.

        dimensions : list[DimensionSimpleModel]
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

    def get_value_columns_mapped_to_project(self) -> set[str]:
        """Return the columns that contain values."""
        match self._config.get_table_format_type():
            case TableFormatType.PIVOTED:
                return self.get_pivoted_dimension_columns_mapped_to_project()
            case TableFormatType.UNPIVOTED:
                return {VALUE_COLUMN}
            case _:
                raise NotImplementedError(str(self._config.get_table_format_type()))

    def get_pivoted_dimension_columns_mapped_to_project(self) -> set[str]:
        """Get columns for the dimension that is pivoted in load_data and remap them to the
        project's record names. The returned set will not include columns that the project does
        not care about.
        """
        if self._config.get_table_format_type() != TableFormatType.PIVOTED:
            return set()

        columns = set(self._config.get_pivoted_dimension_columns())
        dim_type = self._config.get_pivoted_dimension_type()
        for ref in self._mapping_references:
            if ref.from_dimension_type == dim_type:
                mapping_config = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                )
                records = mapping_config.get_records_dataframe()
                from_ids = get_unique_values(records, "from_id")
                to_ids = get_unique_values(
                    records.select("to_id").filter("to_id IS NOT NULL"), "to_id"
                )
                diff = from_ids.symmetric_difference(columns)
                if diff:
                    raise DSGInvalidDataset(
                        f"Dimension_mapping={mapping_config.config_id} does not have the same "
                        f"record IDs as the dataset={self._config.config_id} columns: {diff}"
                    )
                return to_ids

        return columns

    def get_columns_for_unique_arrays(self, load_data_df):
        """Returns the list of dimension columns aginst which the number of timestamps is checked.

        Returns
        -------
        list[str]
            List of column names.

        """
        time_cols = self._get_time_dimension_columns()
        value_cols = self._config.get_value_columns()
        return list(set(load_data_df.columns).difference(set(value_cols + time_cols)))

    @abc.abstractmethod
    def make_project_dataframe(self, project_config):
        """Return a load_data dataframe with dimensions mapped to the project's.

        Parameters
        ----------
        project_config: ProjectConfig

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @abc.abstractmethod
    def make_project_dataframe_from_query(self, context, project_config):
        """Return a load_data dataframe with dimensions mapped to the project's with filters
        as specified by the QueryContext.

        Parameters
        ----------
        context : QueryContext
        project_config : ProjectConfig

        Returns
        -------
        pyspark.sql.DataFrame

        """

    def _check_aggregations(self, context):
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            pivoted_type = self._config.get_pivoted_dimension_type()
            for agg in context.model.result.aggregations:
                if not getattr(agg.dimensions, pivoted_type.value):
                    raise DSGInvalidQuery(
                        f"Pivoted dimension type {pivoted_type.value} is not included in an aggregation"
                    )

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency(self, load_data_df):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        """
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__"):
            logger.warning("Skip dataset time consistency checks.")
            return

        logger.info("Check dataset time consistency.")
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_cols = self._get_time_dimension_columns()
        time_dim.check_dataset_time_consistency(load_data_df, time_cols)
        if time_dim.model.time_type != TimeDimensionType.NOOP:
            self._check_dataset_time_consistency_by_time_array(time_cols, load_data_df)

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency_by_time_array(self, time_cols, load_data_df):
        """Check that each unique time array has the same timestamps."""
        logger.info("Check dataset time consistency by time array.")
        unique_array_cols = self.get_columns_for_unique_arrays(load_data_df)
        for col in time_cols:
            load_data_df = load_data_df.filter(f"{col} is not null")
        counts = load_data_df.groupBy(*time_cols).count().select("count")
        distinct_counts = counts.select("count").distinct().collect()
        if len(distinct_counts) != 1:
            raise DSGInvalidDataset(
                "All time arrays must be repeated the same number of times: "
                f"unique timestamp repeats = {len(distinct_counts)}"
            )
        ta_counts = load_data_df.groupBy(*unique_array_cols).count().select("count")
        distinct_ta_counts = ta_counts.select("count").distinct().collect()
        if len(distinct_ta_counts) != 1:
            raise DSGInvalidDataset(
                "All combinations of non-time dimensions must have the same time array length: "
                f"unique time array lengths = {len(distinct_ta_counts)}"
            )

    def _check_load_data_unpivoted_columns(self, df):
        logger.info("Check load data unpivoted columns.")
        if self._config.model.data_schema.table_format.value_column not in df.columns:
            raise DSGInvalidDataset(
                f"value_column={self._config.model.data_schema.table_format.value_column} is not in columns={df.columns}"
            )

    def _convert_units(self, df, project_metric_records, value_columns):
        if not self._config.model.enable_unit_conversion:
            return df

        # Note that a dataset could have the same dimension record IDs as the project,
        # no mappings, but then still have different units.
        mapping_records = None
        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            if dim_type == DimensionType.METRIC:
                mapping_records = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                ).get_records_dataframe()
                break

        dataset_records = self._config.get_dimension(DimensionType.METRIC).get_records_dataframe()
        match self._config.get_table_format_type():
            case TableFormatType.PIVOTED:
                df = energy.convert_units_pivoted(
                    df, value_columns, dataset_records, mapping_records, project_metric_records
                )
            case TableFormatType.UNPIVOTED:
                df = energy.convert_units_unpivoted(
                    df,
                    DimensionType.METRIC.value,
                    dataset_records,
                    mapping_records,
                    project_metric_records,
                )
            case _:
                raise NotImplementedError(
                    str(self._config.model.data_schema.table_format.format_type)
                )
        return df

    def _get_dataset_to_project_mapping_records(self, dimension_type: DimensionType):
        config = self._get_dataset_to_project_mapping_config(dimension_type)
        if config is None:
            return config
        return config.get_records_dataframe()

    def _get_dataset_to_project_mapping_config(self, dimension_type: DimensionType):
        ref = self._get_dataset_to_project_mapping_reference(dimension_type)
        if ref is None:
            return ref
        return self._dimension_mapping_mgr.get_by_id(ref.mapping_id, version=ref.version)

    def _get_dataset_to_project_mapping_reference(self, dimension_type: DimensionType):
        for ref in self._mapping_references:
            if ref.from_dimension_type == dimension_type:
                return ref
        return

    def _get_time_dimension_columns(self):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_cols = time_dim.get_load_data_time_columns()
        return time_cols

    def _handle_unpivot_column_rename(self, df: DataFrame):
        if (
            self._config.get_table_format_type() == TableFormatType.UNPIVOTED
            and self._config.model.table_format.value_column != VALUE_COLUMN
        ):
            df = df.withColumnRenamed(
                self._config.model.data_schema.table_format.value_column, VALUE_COLUMN
            )
        return df

    def _iter_dataset_record_ids(self, context: QueryContext):
        for dim_type, project_record_ids in context.get_record_ids().items():
            dataset_mapping = self._get_dataset_to_project_mapping_records(dim_type)
            if dataset_mapping is None:
                dataset_record_ids = project_record_ids
            else:
                dataset_record_ids = (
                    dataset_mapping.withColumnRenamed("from_id", "dataset_record_id")
                    .join(
                        project_record_ids,
                        on=dataset_mapping.to_id == project_record_ids.id,
                    )
                    .selectExpr("dataset_record_id AS id")
                    .distinct()
                )
            yield dim_type, dataset_record_ids

    def _prefilter_pivoted_dimensions(self, context: QueryContext, df):
        for dim_type, dataset_record_ids in self._iter_dataset_record_ids(context):
            if dim_type == self._config.get_pivoted_dimension_type():
                # Drop columns that don't match requested project record IDs.
                cols_to_keep = {x.id for x in dataset_record_ids.collect()}
                cols_to_drop = set(self._config.get_pivoted_dimension_columns()).difference(
                    cols_to_keep
                )
                if cols_to_drop:
                    df = df.drop(*cols_to_drop)

        return df

    def _prefilter_stacked_dimensions(self, context: QueryContext, df):
        format_type = self._config.get_table_format_type()
        for dim_type, dataset_record_ids in self._iter_dataset_record_ids(context):
            if (
                format_type == TableFormatType.PIVOTED
                and dim_type == self._config.get_pivoted_dimension_type()
            ):
                continue
            # Drop rows that don't match requested project record IDs.
            tmp = dataset_record_ids.withColumnRenamed("id", "dataset_record_id")
            df = df.join(tmp, on=df[dim_type.value] == tmp.dataset_record_id).drop(
                "dataset_record_id"
            )

        return df

    def _prefilter_time_dimension(self, context: QueryContext, df):
        # TODO #196:
        return df

    def _remap_dimension_columns(self, df, filtered_records=None):
        pivoted_dim_type = self._config.get_pivoted_dimension_type()
        pivoted_columns = set(df.columns).intersection(
            self._config.get_pivoted_dimension_columns()
        )
        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            column = dim_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version
            )
            records = mapping_config.get_records_dataframe()
            if filtered_records is not None and dim_type in filtered_records:
                records = records.join(
                    filtered_records[dim_type], on=records.to_id == filtered_records[dim_type].id
                ).drop("id")

            if is_noop_mapping(records):
                logger.info("Skip no-op mapping %s.", ref.mapping_id)
                continue
            if column in df.columns:
                df = map_and_reduce_stacked_dimension(df, records, column)
            elif (
                pivoted_dim_type is not None
                and column == pivoted_dim_type.value
                and not pivoted_columns.difference(df.columns)
            ):
                # The dataset might have columns unwanted by the project.
                columns_to_remove = get_unique_values(records.filter("to_id IS NULL"), "from_id")
                if columns_to_remove:
                    df = df.drop(*columns_to_remove)
                    pivoted_columns.difference_update(columns_to_remove)
                # TODO #197: Do we want operation to be configurable?
                operation = "sum"
                df, _, _ = map_and_reduce_pivoted_dimension(
                    df,
                    records,
                    pivoted_columns,
                    operation,
                    rename=False,
                )
            elif (
                pivoted_dim_type is not None
                and column == pivoted_dim_type.value
                and pivoted_columns.intersection(df.columns)
            ):
                raise NotImplementedError(
                    f"Unhandled case: column={column} pivoted_columns={pivoted_columns} "
                    f"df.columns={df.columns}"
                )
            # else nothing to do

        return df

    def _apply_fraction(self, df, value_columns, agg_func=None):
        if "fraction" not in df.columns:
            return df
        agg_func = agg_func or F.sum
        # Maintain column order.
        agg_ops = [
            agg_func(F.col(x) * F.col("fraction")).alias(x)
            for x in [y for y in df.columns if y in value_columns]
        ]
        gcols = set(df.columns) - value_columns - {"fraction"}
        df = df.groupBy(*ordered_subset_columns(df, gcols)).agg(*agg_ops)
        return df.drop("fraction")

    def _convert_time_before_project_mapping(self):
        # The only expected case for this returning True is representative time with a custom
        # geography.
        time_dim = self._config.get_dimension(DimensionType.TIME)
        val = (
            time_dim.model.is_time_zone_required_in_geography()
            and not self._config.model.use_project_geography_time_zone
        )
        return val

    @track_timing(timer_stats_collector)
    def _convert_time_dimension(
        self, load_data_df, project_config, model_years=None, value_columns=None
    ):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.model.is_time_zone_required_in_geography():
            if self._config.model.use_project_geography_time_zone:
                geography_dim = project_config.get_base_dimension(DimensionType.GEOGRAPHY)
            else:
                geography_dim = self._config.get_dimension(DimensionType.GEOGRAPHY)
            load_data_df = add_time_zone(load_data_df, geography_dim)

        if model_years is not None:
            model_years = sorted(int(x) for x in model_years)
        load_data_df = time_dim.convert_dataframe(
            load_data_df,
            self._project_time_dim,
            model_years=model_years,
            value_columns=value_columns,
        )

        if time_dim.model.is_time_zone_required_in_geography():
            load_data_df = load_data_df.drop("time_zone")

        return load_data_df
