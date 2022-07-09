import logging
from typing import List

from dsgrid.config.dataset_config import (
    DatasetConfig,
)
from dsgrid.config.simple_models import DatasetSimpleModel
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset

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
        pivot_cols = set()

        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = time_dim.get_timestamp_load_data_columns()
        pivot_dim = self._config.model.data_schema.load_data_column_dimension
        expected_pivot_columns = self.get_pivot_dimension_columns()
        pivot_dim_found = False
        for col in self._load_data.columns:
            if col in time_columns:
                continue
            elif col in expected_pivot_columns:
                pivot_dim_found = True
                pivot_cols.add(col)
            else:
                dimension_types.add(DimensionType.from_column(col))

        if pivot_dim_found:
            dimension_types.add(pivot_dim)

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
            if dimension_type == pivot_dim:
                data_records = set(pivot_cols)
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
        pivoted_cols = set(self.get_pivot_dimension_columns())
        exclude = time_cols.union(pivoted_cols)
        dim_cols = [x for x in self._load_data.columns if x not in exclude]
        df = self._load_data.select(*dim_cols).distinct()

        dim_table = self._remap_dimension_columns(df).distinct()
        self._check_null_value_in_unique_dimension_rows(dim_table)

        return dim_table

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: List[DatasetSimpleModel]):
        assert False, "not supported yet"
