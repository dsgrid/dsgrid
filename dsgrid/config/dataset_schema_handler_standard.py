from pathlib import Path
import logging

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import (
    DatasetConfig,
    check_load_data_filename,
    check_load_data_lookup_filename,
)
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, Timer
from dsgrid.config.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimension,
)

logger = logging.getLogger(__name__)


class StandardDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """ define interface/required behaviors for STANDARD dataset schema """

    def __init__(self, config):
        self._config = config

    def get_pivot_dimension_columns(self):
        """ get cols for the dimension that is pivoted in load_data. """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return self._config.get_dimension(dim_type).get_unique_ids()

    def check_consistency(self):
        path = Path(self._config.model.path)
        load_data_df = read_dataframe(check_load_data_filename(path))
        load_data_lookup = read_dataframe(check_load_data_lookup_filename(path), cache=True)
        load_data_lookup = self._config.add_trivial_dimensions(load_data_lookup)
        with Timer(timer_stats_collector, "check_lookup_data_consistency"):
            self._check_lookup_data_consistency(self._config, load_data_lookup)
        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(self._config, load_data_df)
        with Timer(timer_stats_collector, "check_dataset_internal_consistency"):
            self._check_dataset_internal_consistency(self._config, load_data_df, load_data_lookup)

    def _check_lookup_data_consistency(self, config: DatasetConfig, load_data_lookup):
        found_id = False
        dimension_types = []
        for col in load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            try:
                dimension_types.append(DimensionType(col))
            except ValueError:
                raise DSGInvalidDimension(f"load_data_lookup column={col} is not a dimension type")

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        load_data_dimensions = (
            DimensionType.TIME,
            config.model.data_schema.load_data_column_dimension,
        )
        expected_dimensions = [d for d in DimensionType if d not in load_data_dimensions]
        if len(dimension_types) != len(expected_dimensions):
            raise DSGInvalidDataset(
                f"load_data_lookup does not have the correct number of dimensions specified between trivial and non-trivial dimensions."
            )

        missing_dimensions = set(expected_dimensions).difference(dimension_types)
        if len(missing_dimensions) != 0:
            raise DSGInvalidDataset(
                f"load_data_lookup is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            lookup_records = get_unique_values(load_data_lookup, name)
            if dim_records != lookup_records:
                logger.error(
                    "Mismatch in load_data_lookup records. dimension=%s mismatched=%s",
                    name,
                    lookup_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data_lookup records do not match dimension records for {name}"
                )

    def _check_dataset_time_consistency(self, config: DatasetConfig, load_data_df):
        time_dim = config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
        self._check_dataset_time_consistency_by_id(time_dim, load_data_df)

    def _check_dataset_time_consistency_by_id(self, time_dim, load_data_df):

        time_ranges = time_dim.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        expected_count = len(expected_timestamps)

        timestamps_by_id = (
            load_data_df.select("timestamp", "id")
            .groupby("id")
            .agg(F.countDistinct("timestamp").alias("distinct_timestamps"))
        )
        distinct_counts = timestamps_by_id.select("distinct_timestamps").distinct()
        if distinct_counts.count() != 1:
            for row in timestamps_by_id.collect():
                if row.distinct_timestamps != len(expected_timestamps):
                    logger.error(
                        "load_data ID=%s does not have %s timestamps: actual=%s",
                        row.id,
                        len(expected_timestamps),
                        row.distinct_timestamps,
                    )

            raise DSGInvalidDataset(
                f"One or more arrays do not have {len(expected_timestamps)} timestamps"
            )

        val = distinct_counts.collect()[0].distinct_timestamps
        if val != expected_count:
            raise DSGInvalidDataset(
                f"load_data arrays do not have {len(expected_timestamps)} "
                "timestamps: actual={row.distinct_timestamps}"
            )

    def _check_dataset_internal_consistency(
        self, config: DatasetConfig, load_data_df, load_data_lookup
    ):
        """ check data columns and id series """
        self._check_load_data_columns(config, load_data_df)
        data_ids = []
        for row in load_data_df.select("id").distinct().sort("id").collect():
            if row.id is None:
                raise DSGInvalidDataset(f"load_data for dataset {config.config_id} has a null ID")
            data_ids.append(row.id)
        lookup_data_ids = (
            load_data_lookup.select("id")
            .distinct()
            .filter("id is not null")
            .sort("id")
            .agg(F.collect_list("id"))
            .collect()[0][0]
        )

        if data_ids != lookup_data_ids:
            logger.error(
                f"Data IDs for %s data/lookup are inconsistent: data=%s lookup=%s",
                config.config_id,
                data_ids,
                lookup_data_ids,
            )
            raise DSGInvalidDataset(
                f"Data IDs for {config.config_id} data/lookup are inconsistent"
            )

    def _check_load_data_columns(self, config: DatasetConfig, load_data_df):
        dim_type = config.model.data_schema.load_data_column_dimension
        dimension_records = self.get_pivot_dimension_columns()
        time_dim = config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())

        found_id = False
        dim_columns = set()
        for col in load_data_df.columns:
            if col == "id":
                found_id = True
                continue
            if col in time_columns:
                continue
            dim_columns.add(col)

        if not found_id:
            raise DSGInvalidDataset("load_data does not include an 'id' column")

        if dimension_records != dim_columns:
            mismatch = dimension_records.symmetric_difference(dim_columns)
            raise DSGInvalidDataset(
                f"Mismatch between load data columns and dimension={dim_type.value} records. Mismatched={mismatch}"
            )
