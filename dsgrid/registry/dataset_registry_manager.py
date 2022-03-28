"""Manages the registry for dimension datasets"""

import getpass
import logging
import os
from pathlib import Path
from typing import Union, List, Dict

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.dimension.base_models import check_required_dimensions
from dsgrid.exceptions import DSGValueNotRegistered, DSGInvalidDataset
from dsgrid.utils.files import load_data
from dsgrid.utils.timing import timer_stats_collector, Timer, track_timing
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import display_table
from .common import (
    make_initial_config_registration,
    ConfigKey,
    auto_register_dimensions,
    RegistryType,
)
from .registration_context import RegistrationContext
from .dataset_registry import DatasetRegistry, DatasetRegistryModel
from .registry_manager_base import RegistryManagerBase

logger = logging.getLogger(__name__)


class DatasetRegistryManager(RegistryManagerBase):
    """Manages registered dimension datasets."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._datasets = {}  # ConfigKey to DatasetModel
        self._dimension_mgr = None
        self._dimension_mapping_mgr = None

    @classmethod
    def load(cls, path, params, dimension_manager, dimension_mapping_manager):
        mgr = cls._load(path, params)
        mgr.dimension_manager = dimension_manager
        mgr.dimension_mapping_manager = dimension_mapping_manager
        return mgr

    @staticmethod
    def name():
        return "Datasets"

    @staticmethod
    def registry_class():
        return DatasetRegistry

    @track_timing(timer_stats_collector)
    def _run_checks(self, config: DatasetConfig):
        logger.info("Run dataset registration checks.")
        check_required_dimensions(config.model.dimensions, "dataset dimensions")
        if not os.environ.get("__DSGRID_SKIP_DATASET_CONSISTENCY_CHECKS__"):
            self._check_dataset_consistency(config)

    def _check_dataset_consistency(self, config: DatasetConfig):
        schema_handler = make_dataset_schema_handler(
            config, self._dimension_mgr, self._dimension_mapping_mgr
        )
        schema_handler.check_consistency()

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val):
        self._dimension_mgr = val

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_mgr

    @dimension_mapping_manager.setter
    def dimension_mapping_manager(self, val):
        self._dimension_mapping_mgr = val

    def finalize_registration(self, config_ids, error_occurred):
        assert len(config_ids) == 1, config_ids
        dataset_id = config_ids[0]
        if error_occurred:
            logger.info("Remove intermediate dataset after error")
            self.remove(dataset_id)
            return

        # TODO DT: add mechanism to verify that nothing has changed.
        lock_file_path = self.get_registry_lock_file(dataset_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            if not self.offline_mode and not self.dry_run_mode:
                self.sync_push(self.get_registry_directory(dataset_id))
                self.sync_push(self.get_registry_data_directory(dataset_id))

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

        dataset = DatasetConfig.load_from_registry(
            self.get_config_file(key.id, key.version),
            self._dimension_mgr,
            self._params.base_path / "data",
        )
        self._datasets[key] = dataset
        return dataset

    def get_registry_lock_file(self, config_id):
        return f"configs/.locks/{config_id}.lock"

    @track_timing(timer_stats_collector)
    def register(
        self,
        config_file,
        dataset_path,
        submitter,
        log_message,
        dimension_file=None,
        force=False,
        context=None,
    ):
        error_occurred = False
        need_to_finalize = context is None
        if context is None:
            context = RegistrationContext()

        config_data = load_data(config_file)
        try:
            self._register_dataset_and_dimensions(
                config_file,
                config_data,
                dataset_path,
                submitter,
                log_message,
                context,
                dimension_file=dimension_file,
                force=force,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    def _register_dataset_and_dimensions(
        self,
        config_file: Path,
        config_data: Dict,
        dataset_path: Path,
        submitter: str,
        log_message: str,
        context: RegistrationContext,
        dimension_file=None,
        force=False,
    ):
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if str(dataset_path).startswith("s3://"):
            raise DSGInvalidDataset(
                f"Loading a dataset from S3 is not currently supported: {dataset_path}"
            )

        self._check_if_already_registered(config_data["dataset_id"])

        if dimension_file is not None:
            auto_register_dimensions(
                [config_data["dimensions"]],
                self._dimension_mgr,
                dimension_file,
                submitter,
                log_message,
                force,
                context,
            )

        config = DatasetConfig.load_from_user_path(
            config_file, self._dimension_mgr, dataset_path, data=config_data
        )
        self._register(config, dataset_path, submitter, log_message, force=force)
        context.add_id(RegistryType.DATASET, config.model.dataset_id, self)

    def _register(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        submitter: str,
        log_message: str,
        force=False,
    ):
        dataset_id = config.model.dataset_id
        self._run_checks(config)
        registration = make_initial_config_registration(submitter, log_message)

        if self.dry_run_mode:
            logger.info(
                "%s Dataset registration validated for dataset_id=%s",
                self._log_dry_run_mode_prefix(),
                dataset_id,
            )
            return

        registry_model = DatasetRegistryModel(
            dataset_id=dataset_id,
            version=registration.version,
            description=config.model.description,
            registration_history=[registration],
        )
        registry_config = DatasetRegistry(registry_model)
        # The dataset_version starts the same as the config but can change later.
        config.model.dataset_version = registration.version
        registry_dir = self.get_registry_directory(dataset_id)
        registry_config_path = registry_dir / str(registration.version)

        dataset_registry_dir = self.get_registry_data_directory(dataset_id)
        dataset_registry_filename = dataset_registry_dir / REGISTRY_FILENAME
        dataset_path = dataset_registry_dir / str(registry_config.version)
        self.fs_interface.mkdir(dataset_registry_dir)
        registry_config.serialize(dataset_registry_filename)
        self.fs_interface.mkdir(registry_config_path)
        self.fs_interface.copy_tree(config.dataset_path, dataset_path)

        # Serialize the registry file as well as the updated DatasetConfig to the registry.
        registry_filename = registry_dir / REGISTRY_FILENAME
        registry_config.serialize(registry_filename, force=force)
        config.serialize(self.get_config_directory(dataset_id, registry_config.version))

        self._update_registry_cache(dataset_id, registry_config)

        logger.info(
            "%s Registered dataset %s with version=%s",
            self._log_offline_mode_prefix(),
            dataset_id,
            registration.version,
        )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = DatasetConfig.load_from_registry(
            config_file, self.dimension_manager, self._params.base_path / "data"
        )
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter)

    @track_timing(timer_stats_collector)
    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(config.model.dataset_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            # Note that projects will not pick up these changes until submit-dataset
            # is called again.
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        self._run_checks(config)
        dataset_id = config.model.dataset_id
        registry = self.get_registry_config(dataset_id)
        old_key = ConfigKey(dataset_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(dataset_id, version)
        self._datasets.pop(old_key, None)
        self._datasets[new_key] = config

        if not self.offline_mode:
            self.sync_push(self.get_registry_directory(dataset_id))
            self.sync_push(self.get_registry_data_directory(dataset_id))

        return version

    def remove(self, config_id):
        # TODO: Do we want to handle specific versions?
        config = self.get_by_id(config_id)
        self.fs_interface.rm_tree(config.dataset_path.parent)
        self._remove(config_id)
        for key in [x for x in self._datasets if x.id == config_id]:
            self._datasets.pop(key)

    def show(
        self,
        filters: List[str] = None,
        max_width: Union[int, Dict] = None,
        drop_fields: List[str] = None,
        **kwargs,
    ):
        """Show registry in PrettyTable

        Parameters
        ----------
        filters : list or tuple
            List of filter expressions for reigstry content (e.g., filters=["Submitter==USER", "Description contains comstock"])
        max_width
            Max column width in PrettyTable, specify as a single value or as a dict of values by field name
        drop_fields
            List of field names not to show

        """

        if filters:
            logger.info("List registry for: %s", filters)

        table = PrettyTable(title=self.name())
        all_field_names = (
            "ID",
            "Version",
            "Date",
            "Submitter",
            "Description",
        )
        if drop_fields is None:
            table.field_names = all_field_names
        else:
            table.field_names = tuple(x for x in all_field_names if x not in drop_fields)

        if max_width is None:
            table._max_width = {
                "ID": 50,
                "Date": 10,
                "Description": 50,
            }
        if isinstance(max_width, int):
            table.max_width = max_width
        elif isinstance(max_width, dict):
            table._max_width = max_width

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for config_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[0]

            all_fields = (
                config_id,
                last_reg.version,
                last_reg.date.strftime("%Y-%m-%d %H:%M:%S"),
                last_reg.submitter,
                registry_config.model.description,
            )
            if drop_fields is None:
                row = all_fields
            else:
                row = tuple(
                    y for (x, y) in zip(all_field_names, all_fields) if x not in drop_fields
                )

            if not filters or matches_filters(row, field_to_index, transformed_filters):
                rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        table.align = "l"
        display_table(table)
