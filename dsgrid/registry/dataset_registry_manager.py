"""Manages the registry for dimension datasets"""

import getpass
import logging
import os
from pathlib import Path
from typing import Union

from prettytable import PrettyTable

from dsgrid.config.dataset_config import DatasetConfig, ALLOWED_DATA_FILES
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dimensions_config import DimensionsConfig, DimensionsConfigModel
from dsgrid.dimension.base_models import check_required_dimensions
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.utils.spark import read_dataframe, write_dataframe
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import check_uniqueness, display_table
from .common import (
    make_initial_config_registration,
    ConfigKey,
    RegistryType,
)
from .registration_context import RegistrationContext
from .registry_interface import DatasetRegistryInterface
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
    def load(cls, path, params, dimension_manager, dimension_mapping_manager, db):
        mgr = cls._load(path, params)
        mgr.dimension_manager = dimension_manager
        mgr.dimension_mapping_manager = dimension_mapping_manager
        mgr.db = db
        return mgr

    @staticmethod
    def config_class():
        return DatasetConfig

    @property
    def db(self) -> DatasetRegistryInterface:
        return self._db

    @db.setter
    def db(self, db: DatasetRegistryInterface):
        self._db = db

    @staticmethod
    def name():
        return "Datasets"

    def _get_registry_data_path(self):
        if self._params.use_remote_data:
            dataset_path = self._params.remote_path
        else:
            dataset_path = str(self._params.base_path)
        return dataset_path

    @track_timing(timer_stats_collector)
    def _run_checks(self, config: DatasetConfig):
        logger.info("Run dataset registration checks.")
        check_required_dimensions(config.model.dimension_references, "dataset dimensions")
        check_uniqueness((x.model.name for x in config.model.dimensions), "dimension name")
        if not os.environ.get("__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__"):
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

        if not self.offline_mode:
            lock_file = self.get_registry_lock_file(dataset_id)
            self.cloud_interface.check_lock_file(lock_file)
            if not error_occurred:
                self.sync_push(self.get_registry_data_directory(dataset_id))
            self.cloud_interface.remove_lock_file(lock_file)

    def get_by_id(self, dataset_id, version=None):
        if version is None:
            version = self._db.get_latest_version(dataset_id)

        key = ConfigKey(dataset_id, version)
        dataset = self._datasets.get(key)
        if dataset is not None:
            return dataset

        if version is None:
            model = self.db.get_latest(dataset_id)
        else:
            model = self.db.get_by_version(dataset_id, version)

        dataset_path = self._get_registry_data_path()
        config = DatasetConfig.load_from_registry(model, dataset_path)
        self._update_dimensions(config)
        self._datasets[key] = config
        return config

    def acquire_registry_locks(self, config_ids: list[str]):
        for dataset_id in config_ids:
            lock_file = self.get_registry_lock_file(dataset_id)
            self.cloud_interface.make_lock_file(lock_file)

    def get_registry_lock_file(self, config_id):
        return f"configs/.locks/{config_id}.lock"

    def _update_dimensions(self, config: DatasetConfig):
        dimensions = self._dimension_mgr.load_dimensions(config.model.dimension_references)
        config.update_dimensions(dimensions)

    @track_timing(timer_stats_collector)
    def register(
        self,
        config_file,
        dataset_path,
        submitter,
        log_message,
        context=None,
    ):
        config = DatasetConfig.load_from_user_path(config_file, dataset_path)
        self._update_dimensions(config)
        return self.register_from_config(
            config, dataset_path, submitter, log_message, context=context
        )

    @track_timing(timer_stats_collector)
    def register_from_config(
        self,
        config,
        dataset_path,
        submitter,
        log_message,
        context=None,
    ):
        error_occurred = False
        need_to_finalize = context is None
        if context is None:
            context = RegistrationContext()

        try:
            self._register_dataset_and_dimensions(
                config,
                dataset_path,
                submitter,
                log_message,
                context,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    def _register_dataset_and_dimensions(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        submitter: str,
        log_message: str,
        context: RegistrationContext,
    ):
        logger.info("Start registration of dataset %s", config.model.dataset_id)
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if str(dataset_path).startswith("s3://"):
            raise DSGInvalidDataset(
                f"Loading a dataset from S3 is not currently supported: {dataset_path}"
            )

        self._check_if_already_registered(config.model.dataset_id)

        if config.model.dimensions:
            dim_model = DimensionsConfigModel(dimensions=config.model.dimensions)
            dims_config = DimensionsConfig.load_from_model(dim_model)
            dimension_ids = self._dimension_mgr.register_from_config(
                dims_config, submitter, log_message, context=context
            )
            config.model.dimension_references += self._dimension_mgr.make_dimension_references(
                dimension_ids
            )
            config.model.dimensions.clear()

        self._update_dimensions(config)
        self._register(config, dataset_path, submitter, log_message)
        context.add_id(RegistryType.DATASET, config.model.dataset_id, self)

    def _register(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        submitter: str,
        log_message: str,
    ):
        dataset_id = config.model.dataset_id
        self._run_checks(config)
        registration = make_initial_config_registration(submitter, log_message)

        # The dataset_version starts the same as the config but can change later.
        config.model.dataset_version = registration.version
        dataset_registry_dir = self.get_registry_data_directory(dataset_id)
        dataset_path = dataset_registry_dir / registration.version
        dataset_path.mkdir(exist_ok=True, parents=True)
        self.fs_interface.mkdir(dataset_registry_dir)
        for filename in ALLOWED_DATA_FILES:
            path = Path(config.dataset_path) / filename
            if path.exists():
                dst = dataset_path / filename
                # Writing with Spark is much faster than copying or rsync if there are
                # multiple nodes in the cluster - much more parallelism.
                write_dataframe(read_dataframe(path), dst)

        self._db.insert(config.model, registration)
        logger.info(
            "%s Registered dataset %s with version=%s",
            self._log_offline_mode_prefix(),
            dataset_id,
            registration.version,
        )

    def update_from_file(
        self, config_file, dataset_id, submitter, update_type, log_message, version
    ):
        dataset_path = self._params.base_path / "data" / dataset_id / version
        config = DatasetConfig.load_from_user_path(config_file, dataset_path)
        self._update_dimensions(config)
        self._check_update(config, dataset_id, version)
        self.update(config, update_type, log_message, submitter)

    @track_timing(timer_stats_collector)
    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(config.model.dataset_id)
        with self.cloud_interface.make_lock_file_managed(lock_file_path):
            # Note that projects will not pick up these changes until submit-dataset
            # is called again.
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        self._run_checks(config)
        dataset_id = config.model.dataset_id
        cur_model = self.db.get_latest(dataset_id)
        old_key = ConfigKey(dataset_id, cur_model.version)
        model = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(dataset_id, model.version)
        self._datasets.pop(old_key, None)

        dataset_path = self._get_registry_data_path()
        config = DatasetConfig.load_from_registry(model, dataset_path)
        self._update_dimensions(config)
        self._datasets[new_key] = config

        if not self.offline_mode:
            self.sync_push(self.get_registry_data_directory(dataset_id))

        return model

    def remove(self, dataset_id):
        config = self.get_by_id(dataset_id)
        if self.fs_interface.exists(config.dataset_path):
            self.fs_interface.rm_tree(Path(config.dataset_path).parent)
        self.db.delete_all(dataset_id)
        for key in [x for x in self._datasets if x.id == dataset_id]:
            self._datasets.pop(key)

        logger.info("Removed %s from the registry.", dataset_id)

    def show(
        self,
        filters: list[str] = None,
        max_width: Union[int, dict] = None,
        drop_fields: list[str] = None,
        return_table: bool = False,
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
        for model in self.db.iter_models(all_versions=True):
            registration = self.db.get_registration(model)

            all_fields = (
                model.dataset_id,
                registration.version,
                registration.date.strftime("%Y-%m-%d %H:%M:%S"),
                registration.submitter,
                registration.log_message,
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
        if return_table:
            return table
        display_table(table)
