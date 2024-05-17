import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.utils.spark import (
    read_dataframe,
    get_unique_values,
    overwrite_dataframe_file,
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
        load_data_df = config.add_trivial_dimensions(read_dataframe(config.load_data_path))
        time_dim = config.get_dimension(DimensionType.TIME)
        load_data_df = time_dim.convert_time_format(load_data_df)
        return cls(load_data_df, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self):
        self._check_one_table_data_consistency()
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
        pivoted_cols = set()

        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = time_dim.get_load_data_time_columns()
        match self._config.get_table_format_type():
            case TableFormatType.PIVOTED:
                expected_pivoted_columns = self._config.get_pivoted_dimension_columns()
                pivoted_dim = self._config.get_pivoted_dimension_type()
            case _:
                expected_pivoted_columns = None
                pivoted_dim = None
                self._check_load_data_unpivoted_value_column(self._load_data)

        pivoted_dim_found = False
        schema = self._load_data.schema
        for col in self._load_data.columns:
            if col in time_columns:
                continue
            elif (
                self._config.get_table_format_type() == TableFormatType.UNPIVOTED
                and col == VALUE_COLUMN
            ):
                continue
            elif expected_pivoted_columns is not None and col in expected_pivoted_columns:
                pivoted_dim_found = True
                pivoted_cols.add(col)
            else:
                dim_type = DimensionType.from_column(col)
                if not schema[col].dataType is StringType():
                    msg = f"dimension column {col} must have data type = StringType"
                    raise DSGInvalidDataset(msg)
                dimension_types.add(dim_type)

        if pivoted_dim_found:
            dimension_types.add(pivoted_dim)

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
            if dimension_type == pivoted_dim:
                data_records = set(pivoted_cols)
            else:
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
        df = self._remap_dimension_columns(df, True)
        return self._remove_non_dimension_columns(df).distinct()

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel]):
        load_df = self._load_data
        time_columns = set(
            self._config.get_dimension(DimensionType.TIME).get_load_data_time_columns()
        )
        df_columns = set(load_df.columns)
        stacked_columns = set()
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in df_columns:
                load_df = load_df.filter(load_df[column].isin(dim.record_ids))
                stacked_columns.add(column)

        pivoted_columns_to_remove = set()
        pivoted_dimension_type = self._config.get_pivoted_dimension_type()
        if pivoted_dimension_type is not None:
            pivoted_columns = set(load_df.columns) - time_columns - stacked_columns
            for dim in dimensions:
                if dim.dimension_type == pivoted_dimension_type:
                    pivoted_columns_to_remove = pivoted_columns.difference(dim.record_ids)

        drop_columns = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = load_df.select(col).distinct().count()
            assert count == 1, f"{dim}: {count}"
            drop_columns.append(col)
        load_df = load_df.drop(*drop_columns)

        load_df = load_df.drop(*pivoted_columns_to_remove)
        path = Path(self._config.load_data_path)
        if path.suffix == ".csv":
            # write_dataframe_and_auto_partition doesn't support CSV yet
            overwrite_dataframe_file(path, load_df)
        else:
            write_dataframe_and_auto_partition(load_df, path)
        logger.info("Rewrote simplified %s", self._config.load_data_path)

    def make_project_dataframe(self, project_config, scratch_dir_context: ScratchDirContext):
        ld_df = self._load_data
        # TODO: This might need to handle data skew in the future.
        ld_df = self._remap_dimension_columns(ld_df, True, scratch_dir_context=scratch_dir_context)
        value_columns = {VALUE_COLUMN}
        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)

        ld_df = self._convert_time_dimension(
            ld_df, project_config, value_columns, scratch_dir_context
        )

        return self._handle_unpivot_column_rename(ld_df)

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        ld_df = self._load_data
        ld_df = self._prefilter_stacked_dimensions(context, ld_df)
        ld_df = self._prefilter_time_dimension(context, ld_df)
        ld_df = self._remap_dimension_columns(
            ld_df,
            True,
            filtered_records=context.get_record_ids(),
            handle_data_skew=True,
            scratch_dir_context=context.scratch_dir_context,
        )
        value_columns = {VALUE_COLUMN}
        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)

        return self._finalize_table(context, ld_df, project_config)
