from pathlib import Path
import logging
import itertools

import pyspark.sql.functions as F

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


class StandardDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """ define interface/required behaviors for STANDARD dataset schema """

    def __init__(self, load_data_df, load_data_lookup, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df
        self._load_data_lookup = load_data_lookup

    @classmethod
    def load(cls, config: DatasetConfig, *args, **kwargs):
        path = Path(config.model.path)
        load_data_df = read_dataframe(config.load_data_path)
        load_data_lookup = read_dataframe(config.load_data_lookup_path, cache=True)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        return cls(load_data_df, load_data_lookup, config, *args, **kwargs)

    def check_consistency(self):
        with Timer(timer_stats_collector, "check_lookup_data_consistency"):
            self._check_lookup_data_consistency()
        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(self._load_data)
        with Timer(timer_stats_collector, "check_dataset_internal_consistency"):
            self._check_dataset_internal_consistency()

    def get_unique_dimension_rows(self):
        path = Path(self._config.model.path)
        return self._remap_dimension_columns(self._load_data_lookup).distinct()

    def _check_lookup_data_consistency(self):
        """Dimension check in load_data_lookup, excludes time:
        * check that data matches record for each dimension.
        * check that all data dimension combinations exist. Time is handled separately.
        """
        found_id = False
        dimension_types = set()
        for col in self._load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            dimension_types.add(DimensionType.from_column(col))

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        load_data_dimensions = (
            DimensionType.TIME,
            self._config.model.data_schema.load_data_column_dimension,
        )
        expected_dimensions = {d for d in DimensionType if d not in load_data_dimensions}
        missing_dimensions = expected_dimensions.difference(dimension_types)
        if missing_dimensions:
            raise DSGInvalidDataset(
                f"load_data_lookup is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = self._config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            lookup_records = get_unique_values(self._load_data_lookup, name)
            if dim_records != lookup_records:
                logger.error(
                    "Mismatch in load_data_lookup records. dimension=%s mismatched=%s",
                    name,
                    lookup_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data_lookup records do not match dimension records for {name}"
                )

    def _check_dataset_internal_consistency(self):
        """ Check load_data dimensions and id series. """
        self._check_load_data_columns()
        data_ids = []
        for row in self._load_data.select("id").distinct().sort("id").collect():
            if row.id is None:
                raise DSGInvalidDataset(
                    f"load_data for dataset {self._config.config_id} has a null ID"
                )
            data_ids.append(row.id)
        lookup_data_ids = (
            self._load_data_lookup.select("id")
            .distinct()
            .filter("id is not null")
            .sort("id")
            .agg(F.collect_list("id"))
            .collect()[0][0]
        )

        if data_ids != lookup_data_ids:
            logger.error(
                f"Data IDs for %s data/lookup are inconsistent: data=%s lookup=%s",
                self._config.config_id,
                data_ids,
                lookup_data_ids,
            )
            raise DSGInvalidDataset(
                f"Data IDs for {self._config.config_id} data/lookup are inconsistent"
            )

    def _check_load_data_columns(self):
        dim_type = self._config.model.data_schema.load_data_column_dimension
        dimension_records = set(self.get_pivot_dimension_columns())
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())

        found_id = False
        pivot_cols = set()
        for col in self._load_data.columns:
            if col == "id":
                found_id = True
                continue
            if col in time_columns:
                continue
            if col in dimension_records:
                pivot_cols.add(col)
            else:
                raise DSGInvalidDataset(f"column={col} is not expected in load_data.")

        if not found_id:
            raise DSGInvalidDataset("load_data does not include an 'id' column")

        if dimension_records != pivot_cols:
            missing = dimension_records.difference(pivot_cols)
            raise DSGInvalidDataset(
                f"load_data is missing {missing} columns for dimension={dim_type.value} based on records."
            )
