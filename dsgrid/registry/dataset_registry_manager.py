"""Manages the registry for dimension datasets"""

import datetime
import getpass
import logging
import os
from pathlib import Path
from datetime import timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_config import (
    DatasetConfig,
    check_load_data_filename,
    check_load_data_lookup_filename,
)
from dsgrid.data_models import serialize_model
from dsgrid.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType, check_required_dimensions
from dsgrid.dimension.time import TimeInvervalType
from dsgrid.exceptions import DSGValueNotRegistered, DSGInvalidDimension, DSGInvalidDataset
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, Timer
from .dataset_registry import (
    DatasetRegistry,
    DatasetRegistryModel,
)
from .registry_manager_base import RegistryManagerBase
from dsgrid.dimension.time import TimeZone, find_time_delta


logger = logging.getLogger(__name__)


class DatasetRegistryManager(RegistryManagerBase):
    """Manages registered dimension datasets."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._datasets = {}  # ConfigKey to DatasetModel
        self._dimension_mgr = None

    @classmethod
    def load(cls, path, params, dimension_manager):
        mgr = cls._load(path, params)
        mgr.dimension_manager = dimension_manager
        return mgr

    @staticmethod
    def name():
        return "Datasets"

    @staticmethod
    def registry_class():
        return DatasetRegistry

    def _run_checks(self, config: DatasetConfig):
        self._check_if_already_registered(config.model.dataset_id)
        check_required_dimensions(config.model.dimensions, "dataset dimensions")
        if not os.environ.get("__DSGRID_SKIP_DATASET_CONSISTENCY_CHECKS__"):
            self._check_dataset_consistency(config)

    def _check_dataset_consistency(self, config: DatasetConfig):
        spark = SparkSession.getActiveSession()
        path = Path(config.model.path)
        load_data = read_dataframe(check_load_data_filename(path))
        load_data_lookup = read_dataframe(check_load_data_lookup_filename(path), cache=True)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        with Timer(timer_stats_collector, "check_lookup_data_consistency"):
            self._check_lookup_data_consistency(config, load_data_lookup)
        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(config, load_data)
        with Timer(timer_stats_collector, "check_dataset_internal_consistency"):
            self._check_dataset_internal_consistency(config, load_data, load_data_lookup)

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

        # TODO: some of this logic will change based on the data table schema type
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

    def _check_dataset_time_consistency(self, config: DatasetConfig, load_data):
        """
        Note:
        - datetime obj are stored on disk in UTC time, if tz-unaware, pyspark session timezone is assigned before converting to UTC,
        - datetime obj are displayed in pyspark session timezone, (which can be set by e.g., .config("spark.sql.session.timeZone", "UTC"))
        - datetime obj are converted to pyspark session timezone if .collect() is used.
        """
        time_dim = config.get_dimension(DimensionType.TIME)
        data_tz = time_dim.get_data_tzinfo()
        time_ranges = time_dim.get_time_ranges()
        time_ranges_tz = time_dim.get_tzinfo()

        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_dim.list_time_range(time_range)

        if data_tz != time_ranges_tz:
            # convert data timestamps to time_ranges_tz and compare to expected_timestamps
            if time_ranges_tz == None:  # TimeZone.LOCAL has no tzinfo
                # load map as spark df and raise error if not found
                tz_map = read_dataframe(
                    filename, cache=False
                )  # FIXME #models_to_dataframe #spark.read.option("header",True).csv(filename)
                tz_map = tz_map.select(
                    F.col("from_id").alias("geography"),
                    F.udf(lambda x: str(TimeZone(x).tz))(F.col("to_id")).alias(
                        "convert_to_timezone"
                    ),
                ).cache()

            else:
                # create a map based on time_ranges_tz
                tz_map = load_data_lookup.select(
                    "geography", F.lit(str(time_ranges_tz)).alias("convert_to_timezone")
                )

            # Show time
            timestamps = load_data.select(
                "id",
                F.col("timestamp").alias(
                    "timestamp_utc"
                ),  # all time is stored on disk in UTC, but when loaded, displayed in pyspark session tz
                F.from_utc_timestamp("timestamp", str(data_tz)).alias("timestamp_data_tz"),
            )  # show in data_tz
            # Join tz map to data
            timestamps = timestamps.join(
                load_data_lookup.join(
                    tz_map,
                    "geography",
                    "left",  # load_data_lookup.geography == tz_map.geography, "left"
                ),
                "id",
                "left",
            ).select(
                "id",
                "geography",  # load_data_lookup.geography,
                "timestamp_data_tz",
                "timestamp_utc",
                "convert_to_timezone",
            )

            # convert time based on map
            timestamps = timestamps.select(
                "*",
                F.from_utc_timestamp("timestamp_utc", F.col("convert_to_timezone")).alias(
                    "timestamp_converted"
                ),
            )  # tz=UTC

            # timestamps_by_geography = load_data.select("id", F.col("timestamp").alias("timestamp_data")).join(
            #     load_data_lookup.join(
            #         tz_map, load_data_lookup.geography == tz_map.from_id, "left"
            #     ),
            #     "id",
            #     "left",
            # ).select(
            #     "id",
            #     "geography",
            #     "timestamp_data",
            #     F.lit(str(data_tz)).alias("from_timezone"),
            #     F.col("to_id").alias("to_timezone"),
            # )
            # udf_find_time_delta = F.udf(lambda ts, fmtz, totz: find_time_delta(ts, fmtz, totz), T.DecimalType())
            # timestamps_by_geography = timestamps_by_geography.withColumn(
            #     "time_delta_sec",
            #     udf_find_time_delta(
            #         F.col("timestamp_data"), F.col("from_timezone"), F.col("to_timezone")
            #     ),
            # )
            # timestamps_by_geography = timestamps_by_geography.select(
            #     "*", (F.unix_timestamp("timestamp_data") + F.col("time_delta_sec")).cast('timestamp').alias("timestamp")
            # )  # generate converted_timestamp

            # Note: x.timestamp.astimezone().astimezone(data_tz): converts to pyspark session time!!!, which is another setting from spark.sql.session.timeZone

            actual_timestamps = [
                x.timestamp.astimezone(TimeZone.UTC.tz).replace(tzinfo=None)
                for x in timestamps.select(F.col("timestamp_converted").alias("timestamp"))
                .distinct()
                .sort("timestamp_converted")
                .collect()
            ]  # check this against EFS comstock test data.

            timestamps_by_id = (
                timestamps.select("timestamp_converted", "id")
                .groupby("id")
                .agg(F.countDistinct("timestamp_converted").alias("distinct_timestamps"))
            )

        else:
            actual_timestamps = [
                x.timestamp.astimezone().astimezone(data_tz)
                for x in load_data.select("timestamp").distinct().sort("timestamp").collect()
            ]

            timestamps_by_id = (
                load_data.select("timestamp", "id")
                .groupby("id")
                .agg(F.countDistinct("timestamp").alias("distinct_timestamps"))
            )

        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )

        for row in timestamps_by_id.collect():
            if row.distinct_timestamps != len(expected_timestamps):
                raise DSGInvalidDataset(
                    f"load_data ID={row.id} does not have {len(expected_timestamps)} timestamps: actual={row.distinct_timestamps}"
                )

    def _check_dataset_internal_consistency(
        self, config: DatasetConfig, load_data, load_data_lookup
    ):
        self._check_load_data_columns(config, load_data)
        data_ids = []
        for row in load_data.select("id").distinct().sort("id").collect():
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

    def _check_load_data_columns(self, config: DatasetConfig, load_data):
        dim_type = config.model.data_schema.load_data_column_dimension
        dimension = config.get_dimension(dim_type)
        dimension_records = dimension.get_unique_ids()

        found_id = False
        dim_columns = set()
        for col in load_data.columns:
            if col == "id":
                found_id = True
                continue
            if col == "timestamp":
                continue
            dim_columns.add(col)

        if not found_id:
            raise DSGInvalidDataset("load_data does not include an 'id' column")

        if dimension_records != dim_columns:
            mismatch = dimension_records.symmetric_difference(dim_columns)
            raise DSGInvalidDataset(
                f"Mismatch between load data columns and dimension={dim_type.value} records. Mismatched={mismatch}"
            )

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val):
        self._dimension_mgr = val

    def get_by_id(self, config_id, version=None):
        self._check_if_not_registered(config_id)
        if version is None:
            version = self._registry_configs[config_id].model.version
        key = ConfigKey(config_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id, version=key.version):
            raise DSGValueNotRegistered(f"dataset={key}")

        dataset = self._datasets.get(key)
        if dataset is not None:
            return dataset

        dataset = DatasetConfig.load(
            self.get_config_file(key.id, key.version), self._dimension_mgr
        )
        self._datasets[key] = dataset
        return dataset

    def get_registry_lock_file(self, config_id):
        return f"configs/.locks/{config_id}.lock"

    def register(self, config_file, submitter, log_message, force=False):
        lock_file_path = self.get_registry_lock_file(load_data(config_file)["dataset_id"])
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._register(config_file, submitter, log_message, force=force)

    def _register(self, config_file, submitter, log_message, force=False):
        config = DatasetConfig.load(Path(config_file), self._dimension_mgr)
        with Timer(timer_stats_collector, "run_dataset_checks"):
            self._run_checks(config)
        registration = make_initial_config_registration(submitter, log_message)

        if self.dry_run_mode:
            logger.info(
                "%s Dataset registration validated for dataset_id=%s",
                self._log_dry_run_mode_prefix(),
                config.model.dataset_id,
            )
            return

        registry_model = DatasetRegistryModel(
            dataset_id=config.model.dataset_id,
            version=registration.version,
            description=config.model.description,
            registration_history=[registration],
        )
        registry_config = DatasetRegistry(registry_model)
        # The dataset_version starts the same as the config but can change later.
        config.model.dataset_version = registration.version
        registry_dir = self.get_registry_directory(config.model.dataset_id)
        registry_config_path = registry_dir / str(registration.version)

        dataset_registry_dir = self.get_registry_data_directory(config.config_id)
        dataset_registry_filename = dataset_registry_dir / REGISTRY_FILENAME
        dataset_path = dataset_registry_dir / str(registry_config.version)
        self.fs_interface.mkdir(dataset_registry_dir)
        registry_config.serialize(dataset_registry_filename)
        self.fs_interface.mkdir(registry_config_path)
        self.fs_interface.copy_tree(config.model.path, dataset_path)

        # The following logic is unfortunately a bit ugly.
        # We have to record the path to the data tables as relative to the location of the
        # dataset config file so that Pydantic validation will work.
        data_parts = list(dataset_path.parts)
        config_parts = registry_config_path.parts
        if data_parts[0] != config_parts[0]:
            raise Exception(f"paths have different roots: {dataset_path} {registry_config_path}")
        rel_path = Path(*[".."] * (len(config_parts) - 1) + data_parts[1:])
        cwd = os.getcwd()
        os.chdir(registry_config_path)
        try:
            config.model.path = str(rel_path)
        finally:
            os.chdir(cwd)

        # Serialize the registry file as well as the updated DatasetConfig to the registry.
        registry_filename = registry_dir / REGISTRY_FILENAME
        registry_config.serialize(registry_filename, force=True)
        config.serialize(self.get_config_directory(config.config_id, registry_config.version))

        self._update_registry_cache(config.model.dataset_id, registry_config)

        if not self.offline_mode:
            self.sync_push(registry_dir)
            self.sync_push(dataset_registry_dir)

        logger.info(
            "%s Registered dataset %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.dataset_id,
            registration.version,
        )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = DatasetConfig.load(config_file, self.dimension_manager)
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter)

    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            # Note that projects will not pick up these changes until submit-dataset
            # is called again.
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        registry = self.get_registry_config(config.config_id)
        old_key = ConfigKey(config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.config_id, version)
        self._datasets.pop(old_key, None)
        self._datasets[new_key] = config

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._datasets if x.id == config_id]:
            self._datasets.pop(key)
