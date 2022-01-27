from pathlib import Path
import logging

from dsgrid.config.dataset_config import (
    DatasetConfig,
    check_load_data_filename,
    check_load_data_lookup_filename,
)
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, Timer
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimension

logger = logging.getLogger(__name__)


class GrowthRateDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """ define interface/required behaviors for GROWTH_RATE dataset schema """

    def __init__(self, config):
        self._config = config

    def check_consistency(self):
        path = Path(self._config.model.path)
        load_data_df = read_dataframe(check_load_data_filename(path), cache=True)
        load_data_df = self._config.add_trivial_dimensions(load_data_df)
        with Timer(timer_stats_collector, "check_growth_rate_data_consistency"):
            self._check_growth_rate_data_consistency(self._config, load_data_df)
        with Timer(timer_stats_collector, "check_metric_units"):
            self._check_metric_units(self._config)

    def _check_growth_rate_data_consistency(self, config: DatasetConfig, load_data):
        """ does not check for time. """
        dimension_types = set()
        pivot_cols = set()

        pivot_dim = config.model.data_schema.load_data_column_dimension
        expected_pivot_columns = self.get_pivot_dimension_columns()
        pivot_dim_found = False
        for col in load_data.columns:
            if col in expected_pivot_columns:
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
                f"load_data is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
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

    def _check_metric_units(self, config: DatasetConfig):
        metric_records = config.get_dimension(DimensionType.METRIC).model.records
        metric_units = {x.unit for x in metric_records}

        if len(metric_units) > 1:
            error = True
        elif list(metric_units)[0] != "":
            error = True
        else:
            error = False

        if error:
            raise DSGInvalidDimension(
                f"Dimension={DimensionType.METRIC} is expected to be unitless, but unit(s)={metric_units} are found."
            )
