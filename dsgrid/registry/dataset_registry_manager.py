"""Manages the registry for dimension datasets"""

import datetime
import getpass
import logging
import os
from pathlib import Path
from datetime import timedelta
import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_config import (
    DatasetConfig,
    DataSchemaType,
)
from dsgrid.data_models import serialize_model
from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType, check_required_dimensions
from dsgrid.dimension.time import TimeInvervalType, TimeDimensionType
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
from dsgrid.dataset.dataset_schema_handler_standard import StandardDatasetSchemaHandler
from dsgrid.dataset.dataset_schema_handler_one_table import OneTableDatasetSchemaHandler

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

    @staticmethod
    def make_dataset_schema_handler(config):
        if config.model.data_schema_type == DataSchemaType.STANDARD:
            return StandardDatasetSchemaHandler(config)
        elif config.model.data_schema_type == DataSchemaType.ONE_TABLE:
            return OneTableDatasetSchemaHandler(config)
        else:
            assert False, config.model.data_schema_type

    def _run_checks(self, config: DatasetConfig):
        self._check_if_already_registered(config.model.dataset_id)
        check_required_dimensions(config.model.dimensions, "dataset dimensions")
        if not os.environ.get("__DSGRID_SKIP_DATASET_CONSISTENCY_CHECKS__"):
            self._check_dataset_consistency(config)

    def _check_dataset_consistency(self, config: DatasetConfig):
        spark = SparkSession.getActiveSession()
        path = Path(config.model.path)
        schema_handler = self.make_dataset_schema_handler(config)
        schema_handler.check_consistency()

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
