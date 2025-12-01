import logging
from typing import Self

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.models import TableFormatType
from dsgrid.query.models import DatasetQueryModel
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.spark.types import (
    DataFrame,
    StringType,
)
from dsgrid.utils.dataset import (
    convert_types_if_necessary,
)
from dsgrid.config.file_schemas import read_data_file
from dsgrid.utils.spark import check_for_nulls
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
    def load(
        cls,
        config: DatasetConfig,
        *args,
        store: DataStoreInterface | None = None,
        **kwargs,
    ) -> Self:
        if store is None:
            if config.data_file_schema is None:
                msg = "Cannot load dataset without data file schema or store"
                raise DSGInvalidDataset(msg)
            df = read_data_file(config.data_file_schema)
        else:
            df = store.read_table(config.model.dataset_id, config.model.version)
        load_data_df = config.add_trivial_dimensions(df)
        load_data_df = convert_types_if_necessary(load_data_df)
        time_dim = config.get_time_dimension()
        if time_dim is not None:
            load_data_df = time_dim.convert_time_format(load_data_df)
        return cls(load_data_df, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self, missing_dimension_associations: dict[str, DataFrame]) -> None:
        self._check_one_table_data_consistency()
        self._check_dimension_associations(missing_dimension_associations)

    @track_timing(timer_stats_collector)
    def check_time_consistency(self):
        time_dim = self._config.get_time_dimension()
        if time_dim is not None:
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
        time_dim = self._config.get_time_dimension()
        time_columns: set[str] = set()
        if time_dim is not None:
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
                if schema[column].dataType != StringType():
                    msg = f"dimension column {column} must have data type = StringType"
                    raise DSGInvalidDataset(msg)
                dimension_types.add(dim_type)
        check_for_nulls(self._load_data)

    def _get_load_data_table(self) -> DataFrame:
        return self._load_data

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel], store: DataStoreInterface):
        assert (
            self._config.get_table_format_type() == TableFormatType.UNPIVOTED
        ), self._config.get_table_format_type()
        load_df = self._load_data
        df_columns = set(load_df.columns)
        stacked_columns = set()
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in df_columns:
                load_df = load_df.filter(load_df[column].isin(dim.record_ids))
                stacked_columns.add(column)

        drop_columns = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = load_df.select(col).distinct().count()
            assert count == 1, f"{dim}: {count}"
            drop_columns.append(col)
        load_df = load_df.drop(*drop_columns)

        store.replace_table(load_df, self.dataset_id, self._config.model.version)
        logger.info("Rewrote simplified %s", self._config.model.dataset_id)

    def make_project_dataframe(
        self, context: QueryContext, project_config: ProjectConfig
    ) -> DataFrame:
        plan = context.model.project.get_dataset_mapping_plan(self.dataset_id)
        if plan is None:
            plan = self.build_default_dataset_mapping_plan()
        with context.dataset_mapping_manager(self.dataset_id, plan) as mapping_manager:
            ld_df = mapping_manager.try_read_checkpointed_table()
            if ld_df is None:
                ld_df = self._load_data
                ld_df = self._prefilter_stacked_dimensions(context, ld_df)
                ld_df = self._prefilter_time_dimension(context, ld_df)

            ld_df = self._remap_dimension_columns(
                ld_df,
                mapping_manager,
                filtered_records=context.get_record_ids(),
            )
            ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN}, mapping_manager)
            project_metric_records = self._get_project_metric_records(project_config)
            ld_df = self._convert_units(ld_df, project_metric_records, mapping_manager)
            input_dataset = project_config.get_dataset(self._config.model.dataset_id)
            ld_df = self._convert_time_dimension(
                load_data_df=ld_df,
                to_time_dim=project_config.get_base_time_dimension(),
                value_column=VALUE_COLUMN,
                mapping_manager=mapping_manager,
                wrap_time_allowed=input_dataset.wrap_time_allowed,
                time_based_data_adjustment=input_dataset.time_based_data_adjustment,
                to_geo_dim=project_config.get_base_dimension(DimensionType.GEOGRAPHY),
            )
            return self._finalize_table(context, ld_df, project_config)

    def make_mapped_dataframe(
        self, context: QueryContext, time_dimension: TimeDimensionBaseConfig | None = None
    ) -> DataFrame:
        query = context.model
        assert isinstance(query, DatasetQueryModel)
        plan = query.mapping_plan
        if plan is None:
            plan = self.build_default_dataset_mapping_plan()
        geography_dimension = self._get_mapping_to_dimension(DimensionType.GEOGRAPHY)
        metric_dimension = self._get_mapping_to_dimension(DimensionType.METRIC)
        with context.dataset_mapping_manager(self.dataset_id, plan) as mapping_manager:
            ld_df = mapping_manager.try_read_checkpointed_table()
            if ld_df is None:
                ld_df = self._load_data

            ld_df = self._remap_dimension_columns(ld_df, mapping_manager)
            ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN}, mapping_manager)
            if metric_dimension is not None:
                metric_records = metric_dimension.get_records_dataframe()
                ld_df = self._convert_units(ld_df, metric_records, mapping_manager)
            if time_dimension is not None:
                ld_df = self._convert_time_dimension(
                    load_data_df=ld_df,
                    to_time_dim=time_dimension,
                    value_column=VALUE_COLUMN,
                    mapping_manager=mapping_manager,
                    wrap_time_allowed=query.wrap_time_allowed,
                    time_based_data_adjustment=query.time_based_data_adjustment,
                    to_geo_dim=geography_dimension,
                )
        return ld_df
