import logging
from pathlib import Path
from typing import List

from dsgrid.config.dataset_config import (
    DatasetConfig,
)
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.utils.dataset import check_null_value_in_unique_dimension_rows
from dsgrid.utils.spark import (
    read_dataframe,
    get_unique_values,
    overwrite_dataframe_file,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType, get_project_dimension_types
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.models import TableFormatType
from dsgrid.dataset.pivoted_table import PivotedTableHandler
from dsgrid.query.query_context import QueryContext

logger = logging.getLogger(__name__)


class OneTableDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """define interface/required behaviors for ONE_TABLE dataset schema"""

    def __init__(self, load_data_df, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df

    @classmethod
    def load(cls, config: DatasetConfig, *args, **kwargs):
        load_data_df = config.add_trivial_dimensions(read_dataframe(config.load_data_path)).drop(
            DimensionType.DATA_SOURCE.value
        )
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
        time_columns = time_dim.get_timestamp_load_data_columns()
        pivoted_dim = self._config.model.data_schema.load_data_column_dimension
        expected_pivoted_columns = self.get_pivoted_dimension_columns()
        pivoted_dim_found = False
        for col in self._load_data.columns:
            if col in time_columns:
                continue
            elif col in expected_pivoted_columns:
                pivoted_dim_found = True
                pivoted_cols.add(col)
            else:
                dimension_types.add(DimensionType.from_column(col))

        if pivoted_dim_found:
            dimension_types.add(pivoted_dim)

        expected_dimensions = get_project_dimension_types() - {DimensionType.TIME}
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

    def get_unique_dimension_rows(self):
        """Get distinct combinations of remapped dimensions.
        Check each col in combination for null value."""
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_cols = set(time_dim.get_timestamp_load_data_columns())
        pivoted_cols = set(self.get_pivoted_dimension_columns())
        exclude = time_cols.union(pivoted_cols)
        dim_cols = [x for x in self._load_data.columns if x not in exclude]
        df = self._load_data.select(*dim_cols).distinct()

        dim_table = self._remap_dimension_columns(df).distinct()
        check_null_value_in_unique_dimension_rows(dim_table)

        return dim_table

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: List[DimensionSimpleModel]):
        load_df = self._load_data
        pivoted_dimension_type = self.get_pivoted_dimension_type()
        pivoted_columns = set(self.get_pivoted_dimension_columns())
        pivoted_columns_to_remove = set()
        df_columns = set(load_df.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in df_columns:
                load_df = load_df.filter(load_df[column].isin(dim.record_ids))
            elif dim.dimension_type == pivoted_dimension_type:
                pivoted_columns_to_remove = pivoted_columns.difference(dim.record_ids)

        drop_columns = []
        for dim in get_project_dimension_types().intersection(
            self._config.model.trivial_dimensions
        ):
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
        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            ld_df = self._convert_time_dimension(self._load_data, project_config)
        else:
            ld_df = self._load_data

        ld_df = self._remap_dimension_columns(ld_df)
        ld_df = self._apply_fraction(ld_df)

        if not convert_time_before_project_mapping:
            ld_df = self._convert_time_dimension(ld_df, project_config)

        return ld_df

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        ld_df = self._load_data

        self._check_aggregations(context)
        ld_df = self._prefilter_stacked_dimensions(context, ld_df)
        ld_df = self._prefilter_pivoted_dimensions(context, ld_df)
        ld_df = self._prefilter_time_dimension(context, ld_df)

        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            ld_df = self._convert_time_dimension(ld_df, project_config)

        ld_df = self._remap_dimension_columns(ld_df, filtered_records=context.get_record_ids())
        ld_df = self._apply_fraction(ld_df)

        if not convert_time_before_project_mapping:
            ld_df = self._convert_time_dimension(ld_df, project_config)

        pivoted_columns = set(ld_df.columns).intersection(
            self.get_pivoted_dimension_columns_mapped_to_project()
        )
        context.set_dataset_metadata(
            self.dataset_id,
            pivoted_columns,
            self.get_pivoted_dimension_type(),
            TableFormatType.PIVOTED,
            project_config,
        )
        table_handler = PivotedTableHandler(project_config, dataset_id=self.dataset_id)
        ld_df = table_handler.convert_columns_to_query_names(ld_df)

        return ld_df
