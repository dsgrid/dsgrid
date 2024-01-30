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

logger = logging.getLogger(__name__)


class OneTableDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """define interface/required behaviors for ONE_TABLE dataset schema"""

    def __init__(self, load_data_df, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df

    @classmethod
    def load(cls, config: DatasetConfig, *args, **kwargs):
        load_data_df = config.add_trivial_dimensions(read_dataframe(config.load_data_path))
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

        expected_dimensions = {d for d in DimensionType if d != DimensionType.TIME}
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
        time_cols = set(self._get_time_dimension_columns())
        pivoted_cols = set(self._config.get_pivoted_dimension_columns())
        exclude = time_cols.union(pivoted_cols)
        if self._config.get_table_format_type() == TableFormatType.UNPIVOTED:
            exclude.add(VALUE_COLUMN)
        dim_cols = [x for x in self._load_data.columns if x not in exclude]
        df = self._load_data.select(*dim_cols).distinct()
        return self._remap_dimension_columns(df, True).distinct()

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

    def make_project_dataframe(self, project_config):
        ld_df = self._load_data
        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            # There is currently no case that needs model years or value columns.
            ld_df = self._convert_time_dimension(ld_df, project_config)

        ld_df = self._remap_dimension_columns(ld_df, True)
        value_columns = set(ld_df.columns).intersection(self.get_value_columns_mapped_to_project())
        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)

        if not convert_time_before_project_mapping:
            model_year_dim = project_config.get_base_dimension(DimensionType.MODEL_YEAR)
            model_years = get_unique_values(model_year_dim.get_records_dataframe(), "id")
            ld_df = self._convert_time_dimension(
                ld_df, project_config, model_years=model_years, value_columns=value_columns
            )

        return self._handle_unpivot_column_rename(ld_df)

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        ld_df = self._load_data

        self._check_aggregations(context)
        ld_df = self._prefilter_stacked_dimensions(context, ld_df)
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            ld_df = self._prefilter_pivoted_dimensions(context, ld_df)
        ld_df = self._prefilter_time_dimension(context, ld_df)

        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            # There is currently no case that needs model years or value columns.
            ld_df = self._convert_time_dimension(ld_df, project_config)

        ld_df = self._remap_dimension_columns(
            ld_df, True, filtered_records=context.get_record_ids()
        )
        value_columns = set(ld_df.columns).intersection(self.get_value_columns_mapped_to_project())
        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)
        if not convert_time_before_project_mapping:
            m_year_df = context.try_get_record_ids_by_dimension_type(DimensionType.MODEL_YEAR)
            if m_year_df is None:
                model_years = project_config.get_base_dimension(
                    DimensionType.MODEL_YEAR
                ).get_unique_ids()
            else:
                model_years = get_unique_values(m_year_df, "id")
            ld_df = self._convert_time_dimension(
                ld_df, project_config, model_years=model_years, value_columns=value_columns
            )

        return self._finalize_table(context, ld_df, value_columns, project_config)
