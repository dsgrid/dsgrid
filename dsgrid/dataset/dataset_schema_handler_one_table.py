import logging
from pathlib import Path

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.spark.types import (
    DataFrame,
    StringType,
)
from dsgrid.utils.spark import (
    read_dataframe,
    get_unique_values,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.scratch_dir_context import ScratchDirContext

logger = logging.getLogger(__name__)


class OneTableDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """define interface/required behaviors for ONE_TABLE dataset schema"""

    def __init__(self, load_data_df, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df

    @classmethod
    def load(cls, config: DatasetConfig, *args, **kwargs):
        df = read_dataframe(config.load_data_path)
        load_data_df = config.add_trivial_dimensions(df)
        time_dim = config.get_dimension(DimensionType.TIME)
        load_data_df = time_dim.convert_time_format(load_data_df)
        return cls(load_data_df, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self):
        self._check_one_table_data_consistency()
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.supports_chronify():
            self._check_dataset_time_consistency_with_chronify()
        else:
            self._check_dataset_time_consistency(self._load_data)

    @track_timing(timer_stats_collector)
    def _check_one_table_data_consistency(self):
        """Dimension check in load_data, excludes time:
        * check that data matches record for each dimension.
        * check that all data dimension combinations exist. Time is handled separately.
        * Check for any NULL values in dimension columns.
        """
        logger.info("Check one table dataset consistency.")
        dimension_types = set()
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_load_data_time_columns())
        assert (
            self._config.get_table_format_type() == TableFormatType.UNPIVOTED
        ), self._config.get_table_format_type()
        self._check_load_data_unpivoted_value_column(self._load_data)
        allowed_columns = DimensionType.get_allowed_dimension_column_names().union(time_columns)
        allowed_columns.add(VALUE_COLUMN)

        schema = self._load_data.schema
        for column in self._load_data.columns:
            if column not in allowed_columns:
                msg = f"{column=} is not expected in load_data"
                raise DSGInvalidDataset(msg)
            if not (column in time_columns or column == VALUE_COLUMN):
                dim_type = DimensionType.from_column(column)
                if not schema[column].dataType is StringType():
                    msg = f"dimension column {column} must have data type = StringType"
                    raise DSGInvalidDataset(msg)
                dimension_types.add(dim_type)

        expected_dimensions = DimensionType.get_dimension_types_allowed_as_columns()
        missing_dimensions = expected_dimensions.difference(dimension_types)
        if missing_dimensions:
            raise DSGInvalidDataset(
                f"load_data is missing dimensions: {missing_dimensions}. "
                "If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = self._config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            data_records = get_unique_values(self._load_data, name)
            if None in data_records:
                raise DSGInvalidDataset(
                    f"{self._config.config_id} has a NULL value for {dimension_type}"
                )
            if dim_records != data_records:
                logger.error(
                    "Mismatch in load_data records. dimension=%s mismatched=%s",
                    name,
                    data_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data records do not match dimension records for {name}"
                )

    def make_dimension_association_table(self) -> DataFrame:
        dim_cols = self._list_dimension_columns(self._load_data)
        df = self._load_data.select(*dim_cols).distinct()
        df = self._remap_dimension_columns(df)
        return self._remove_non_dimension_columns(df).distinct()

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel]):
        assert (
            self._config.get_table_format_type() == TableFormatType.UNPIVOTED
        ), self._config.get_table_format_type()
        load_df = self._load_data
        df_columns = set(load_df.columns)
        stacked_columns = set()
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in df_columns:
                load_df = load_df.filter(getattr(load_df, column).isin(dim.record_ids))
                stacked_columns.add(column)

        drop_columns = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = load_df.select(col).distinct().count()
            assert count == 1, f"{dim}: {count}"
            drop_columns.append(col)
        load_df = load_df.drop(*drop_columns)

        path = Path(self._config.load_data_path)
        write_dataframe_and_auto_partition(load_df, path)
        logger.info("Rewrote simplified %s", self._config.load_data_path)

    def make_project_dataframe(self, project_config, scratch_dir_context: ScratchDirContext):
        ld_df = self._load_data
        # TODO: This might need to handle data skew in the future.
        ld_df = self._remap_dimension_columns(ld_df, scratch_dir_context=scratch_dir_context)
        ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN})
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, {VALUE_COLUMN})
        return self._convert_time_dimension(
            ld_df, project_config, VALUE_COLUMN, scratch_dir_context
        )

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        ld_df = self._load_data
        ld_df = self._prefilter_stacked_dimensions(context, ld_df)
        ld_df = self._prefilter_time_dimension(context, ld_df)
        ld_df = self._remap_dimension_columns(
            ld_df,
            filtered_records=context.get_record_ids(),
            handle_data_skew=True,
            scratch_dir_context=context.scratch_dir_context,
        )
        ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN})
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, {VALUE_COLUMN})
        ld_df = self._convert_time_dimension(
            ld_df, project_config, VALUE_COLUMN, context.scratch_dir_context
        )
        return self._finalize_table(context, ld_df, project_config)
