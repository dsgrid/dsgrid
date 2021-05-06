"""Manages the registry for dimension datasets"""

import logging
import os

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.data_models import serialize_model
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
)
from dsgrid.utils.files import dump_data, load_data
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

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val):
        self._dimension_mgr = val

    def get_by_id(self, config_id, version=None):
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

    def register(self, config_file, submitter, log_message, force=False):
        if self.offline_mode or self.dry_run_mode:
            self._register(config_file, submitter, log_message, force=force)
        else:
            lock_file_path = self.get_registry_lockfile(load_data(config_file)["dataset_id"])
            with self.cloud_interface.make_lock(load_data(config_file)["dataset_id"]):
                self._register(config_file, submitter, log_message, force=force)

    def _register(self, config_file, submitter, log_message, force=False):
        config = DatasetConfig.load(config_file, self._dimension_mgr)
        self._check_if_already_registered(config.model.dataset_id)

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
        registry_dir = self.get_registry_directory(config.model.dataset_id)
        data_dir = registry_dir / str(registration.version)

        # Serialize the registry file as well as the updated DatasetConfig to the registry.
        self.fs_interface.mkdir(data_dir)
        registry_filename = registry_dir / REGISTRY_FILENAME
        dump_data(serialize_model(registry_model), registry_filename)

        config_filename = data_dir / ("dataset" + os.path.splitext(config_file)[1])
        dump_data(serialize_model(config.model), config_filename)

        self._update_registry_cache(config.model.dataset_id, registry_model)

        if not self.offline_mode:
            self.sync_push(registry_dir)

        logger.info(
            "%s Registered dataset %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.dataset_id,
            registration.version,
        )

    def remove(self, config_id):
        self._raise_if_dry_run("remove")
        self._check_if_not_registered(config_id)

        self.fs_interface.rmtree(self._get_dataset_directory(config_id))

        assert False, "broken code"  # FIXME
        for project_registry in self._project_registries.values():
            if project_registry.has_dataset(config_id, DatasetRegistryStatus.REGISTERED):
                project_registry.set_dataset_status(config_id, DatasetRegistryStatus.UNREGISTERED)
                project_registry.serialize(
                    self._get_registry_filename(ProjectRegistry, project_registry.project_id)
                )

        logger.info("Removed %s from the registry.", config_id)

    def update(self, config_file, submitter, update_type, log_message):
        assert False, "Updating a dataset is not currently supported"
        # Commented-out code from registry_manager.py needs to be ported here and tested.
        self._check_if_not_registered(config_id)

        registry_file = self._get_registry_filename(dataset_id)
        registry_config = DatasetRegistryModel(**load_data(registry_file))
        self._update_config(
            dataset_id, registry_config, config_file, submitter, update_type, log_message
        )
