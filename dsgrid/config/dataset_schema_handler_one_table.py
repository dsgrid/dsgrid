from pathlib import Path
import logging
import collections

from dsgrid.config.dataset_config import (
    DatasetConfig,
    check_load_data_filename,
    check_load_data_lookup_filename,
)
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, Timer
from dsgrid.config.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension

logger = logging.getLogger(__name__)


class OneTableDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """ define interface/required behaviors for ONE_TABLE dataset schema """

    def __init__(self, config):
        self._config = config

    def _get_pivot_dimension_columns(self):
        """ get cols for the dimension that is pivoted in load_data. """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return self._config.get_dimension(dim_type).get_unique_ids()

    def check_consistency(self):
        path = Path(self._config.model.path)
        load_data_df = read_dataframe(check_load_data_filename(path), cache=True)
        load_data_df = self._config.add_trivial_dimensions(load_data_df)
        with Timer(timer_stats_collector, "check_one_table_data_consistency"):
            self._check_one_table_data_consistency(self._config, load_data_df)
        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(self._config, load_data_df)

    def _check_one_table_data_consistency(self, config: DatasetConfig, load_data):
        dimension_types = set()
        pivot_cols = []

        time_dim = config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())
        pivot_dim = config.model.data_schema.load_data_column_dimension
        expected_pivot_columns = self._get_pivot_dimension_columns()

        for col in load_data.columns:
            if col in time_columns:
                dimension_types.add(DimensionType.TIME)
            elif col in expected_pivot_columns:
                dimension_types.add(pivot_dim)
                pivot_cols.append(col)
            else:
                try:
                    dimension_types.add(DimensionType(col))
                except ValueError:
                    raise DSGInvalidDimension(
                        f"load_data column={col} is not expected or of a known dimension type."
                    )

        # check for duplicated values in pivot_cols
        pivot_cols_dup = [x for x, n in collections.Counter(pivot_cols).items() if n > 1]
        if len(pivot_cols_dup) > 0:
            raise DSGInvalidDataset(
                f"load_data contains duplicated column name(s)={pivot_cols_dup}."
            )

        expected_dimensions = {d for d in DimensionType}
        if len(dimension_types) != len(expected_dimensions):
            raise DSGInvalidDataset(
                f"load_data does not have the correct number of dimensions specified between trivial and non-trivial dimensions."
            )

        missing_dimensions = expected_dimensions.difference(dimension_types)
        if missing_dimensions:
            raise DSGInvalidDataset(
                f"load_data is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
            if dimension_type == DimensionType.TIME:
                continue
            name = dimension_type.value
            dimension = config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            if dimension_type == pivot_dim:
                data_records = set(pivot_cols)
            else:
                data_records = get_unique_values(load_data, name)
            if dim_records != data_records:
                logger.error(
                    "Mismatch in load_data records. dimension=%s mismatched=%s",
                    name,
                    data_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data records do not match dimension records for {name}"
                )

    def _check_dataset_time_consistency(self, config: DatasetConfig, load_data_df):
        time_dim = config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
