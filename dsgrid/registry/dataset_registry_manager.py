"""Manages the registry for dimension datasets"""

import datetime
import getpass
import logging
import os
from pathlib import Path
from datetime import timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.data_models import serialize_model
from dsgrid.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType, check_required_dimensions
from dsgrid.dimension.time import Period
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
        self._check_dataset_consistency(config)

    def _check_dataset_consistency(self, config):
        spark = SparkSession.getActiveSession()
        load_data = read_dataframe(Path(config.model.path) / Dataset.DATA_FILENAME)
        load_data_lookup = read_dataframe(
            Path(config.model.path) / Dataset.LOOKUP_FILENAME, cache=True
        )
        with Timer(timer_stats_collector, "check_lookup_data_consistency"):
            self._check_lookup_data_consistency(config, load_data_lookup)

        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(config, load_data)
        with Timer(timer_stats_collector, "check_dataset_internal_consistency"):
            self._check_dataset_internal_consistency(config, load_data, load_data_lookup)

    def _check_lookup_data_consistency(self, config, load_data_lookup):
        found_id = False
        dimension_types = []
        for col in load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            try:
                dimension_types.append(DimensionType(col))
            except ValueError:
                raise DSGInvalidDimension("load_data_lookup column={col} is not a dimension type")

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = config.get_dimension(dimension_type)
            dim_records = {x.id for x in dimension.model.records.select("id").collect()}
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
        time_dim = config.get_dimension(DimensionType.TIME)
        time_ranges = time_dim.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        # TODO: need to support validation of multiple time ranges
        time_range = time_ranges[0]

        weather_dim = config.get_dimension(DimensionType.WEATHER_YEAR)
        weather_years = {int(x.id) for x in weather_dim.model.records.collect()}
        assert len(weather_years) == 1, len(weather_years)
        # TODO: need to support handling of multiple weather years

        if time_range.start.year not in weather_years:
            raise DSGInvalidDataset(
                f"weather year mismatch: time_dimension start={time_range.start} weather_years={weather_years}"
            )
        if time_range.end.year not in weather_years:
            valid = True
            if time_dim.period == Period.PERIOD_BEGINNING:
                valid = False
            elif (
                time_dim.period == Period.PERIOD_ENDING
                and time_dim.end_time != max(weather_years) + 1
            ):
                valid = False
            if not valid:
                raise DSGInvalidDataset(
                    f"weather year mismatch: time_dimension end={time_range.end} weather_years={weather_years}"
                )

        unique = (
            load_data.select("timestamp", "id")
            .sort("timestamp")
            .groupby("id")
            .agg(
                F.first("timestamp").alias("first_timestamp"),
                F.last("timestamp").alias("last_timestamp"),
            )
            .select("first_timestamp", "last_timestamp")
            .distinct()
            .collect()
        )
        if len(unique) != 1:
            raise DSGInvalidDimension(
                f"Dataset {config.config_id} does not have common first and last timestamps: {unique}"
            )

        tz = time_dim.get_tzinfo()
        dataset_start = unique[0].first_timestamp.astimezone().astimezone(tz)
        dataset_end = unique[0].last_timestamp.astimezone().astimezone(tz)
        if dataset_start != time_range.start:
            raise DSGInvalidDimension(
                f"Mismatch between dataset start time ({dataset_start} and dimension start time ({time_range.start}"
            )
        if dataset_end != time_range.end:
            raise DSGInvalidDimension(
                f"Mismatch between dataset end time ({dataset_end} and dimension start time: {time_range.end}"
            )

    def _check_dataset_internal_consistency(
        self, config: DatasetConfig, load_data, load_data_lookup
    ):
        self._check_dataset_columns(config, load_data)
        data_ids = []
        for row in load_data.select("id").distinct().sort("id").collect():
            if row.id is None:
                raise DSGInvalidDataset(f"load_data for dataset {config.config_id} has a null ID")
            data_ids.append(row.id)
        lookup_data_ids = []
        for row in load_data_lookup.select("id").distinct().sort("id").collect():
            if row.id is None:
                raise DSGInvalidDataset(
                    f"load_data_lookup for dataset {config.config_id} has a null data ID"
                )
            lookup_data_ids.append(row.id)
        if data_ids != lookup_data_ids:
            raise DSGInvalidDataset(
                f"Data IDs for {config.config_id} data/lookup are inconsistent"
            )

    def _check_dataset_columns(self, config: DatasetConfig, load_data):
        dim_type = config.model.load_data_dimension
        dimension = config.get_dimension(dim_type)
        dimension_records = get_unique_values(dimension.model.records, "id")

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
        config = DatasetConfig.load(config_file, self._dimension_mgr)
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
        registry_dir = self.get_registry_directory(config.model.dataset_id)
        data_dir = registry_dir / str(registration.version)

        # Serialize the registry file as well as the updated DatasetConfig to the registry.
        self.fs_interface.mkdir(data_dir)
        registry_filename = registry_dir / REGISTRY_FILENAME
        registry_config.serialize(registry_filename, force=True)
        config.serialize(self.get_config_directory(config.config_id, registry_config.version))
        dataset_path = self.get_registry_data_directory(config.config_id)
        if self.fs_interface.exists(dataset_path):
            raise DSGInvalidDataset(f"path already exists: {dataset_path}")
        self.fs_interface.copy_tree(config.model.path, dataset_path)
        self._update_registry_cache(config.model.dataset_id, registry_config)

        if not self.offline_mode:
            self.sync_push(registry_dir)

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
