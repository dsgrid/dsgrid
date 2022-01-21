"""Manages the registry for dimension projects"""
import getpass
import itertools
import logging

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceListModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.spark import models_to_dataframe
from .common import VersionUpdateType
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry import (
    ProjectRegistry,
    ProjectRegistryModel,
)
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class ProjectRegistryManager(RegistryManagerBase):
    """Manages registered dimension projects."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._projects = {}  # ConfigKey to ProjectModel
        self._dataset_mgr = None
        self._dimension_mgr = None
        self._dimension_mapping_mgr = None

    @classmethod
    def load(
        cls, path, fs_interface, dataset_manager, dimension_manager, dimension_mapping_manager
    ):
        mgr = cls._load(path, fs_interface)
        mgr.dataset_manager = dataset_manager
        mgr.dimension_manager = dimension_manager
        mgr.dimension_mapping_manager = dimension_mapping_manager
        return mgr

    @staticmethod
    def name():
        return "Projects"

    @property
    def dataset_manager(self):
        return self._dataset_mgr

    @dataset_manager.setter
    def dataset_manager(self, val: DatasetRegistryManager):
        self._dataset_mgr = val

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val: DimensionRegistryManager):
        self._dimension_mgr = val

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_mgr

    @dimension_mapping_manager.setter
    def dimension_mapping_manager(self, val: DimensionRegistryManager):
        self._dimension_mapping_mgr = val

    @staticmethod
    def registry_class():
        return ProjectRegistry

    def get_by_id(self, config_id, version=None):
        self._check_if_not_registered(config_id)
        if version is None:
            version = self._registry_configs[config_id].model.version
        key = ConfigKey(config_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id, version=key.version):
            raise DSGValueNotRegistered(f"project={key}")

        project = self._projects.get(key)
        if project is not None:
            return project

        project = ProjectConfig.load(
            self.get_config_file(key.id, key.version),
            self._dimension_mgr,
            self._dimension_mapping_mgr,
        )
        self._projects[key] = project
        return project

    def get_registry_lock_file(self, config_id):
        return f"configs/.locks/{config_id}.lock"

    def register(self, config_file, submitter, log_message, force=False):
        config = ProjectConfig.load(config_file, self._dimension_mgr, self._dimension_mapping_mgr)
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._register(config, submitter, log_message, force=force)

    def _register(self, config, submitter, log_message, force=False):
        self._check_if_already_registered(config.model.project_id)

        registration = make_initial_config_registration(submitter, log_message)

        if self.dry_run_mode:
            logger.info(
                "%s Project validated for registration project_id=%s",
                self._log_dry_run_mode_prefix(),
                config.model.project_id,
            )
            return

        registry_model = ProjectRegistryModel(
            project_id=config.model.project_id,
            version=registration.version,
            description=config.model.description,
            registration_history=[registration],
        )
        registry_config = ProjectRegistry(registry_model)
        registry_dir = self.get_registry_directory(config.model.project_id)
        data_dir = registry_dir / str(registration.version)

        # Serialize the registry file as well as the updated ProjectConfig to the registry.
        self.fs_interface.mkdir(data_dir)
        registry_filename = registry_dir / REGISTRY_FILENAME
        registry_config.serialize(registry_filename, force=True)
        config.serialize(self.get_config_directory(config.config_id, registry_config.version))

        self._update_registry_cache(config.model.project_id, registry_config)

        if not self.offline_mode:
            registry_dir = self.get_registry_directory(config.config_id)
            self.sync_push(registry_dir)

        logger.info(
            "%s Registered project %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.project_id,
            registration.version,
        )

    def submit_dataset(
        self, project_id, dataset_id, dimension_mapping_files, submitter, log_message
    ):
        """Registers a dataset with a project. This can only be performed on the
        latest version of the project.

        Parameters
        ----------
        project_id : str
        dataset_id : str
        dimension_mapping_files : tuple
            dimension mapping filenames
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id or dataset_id is not registered.
        DSGDuplicateValueRegistered
            Raised if the dataset is already registered with the project.
        ValueError
            Raised if the project does not contain this dataset.

        """
        config = self.get_by_id(project_id)
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._submit_dataset(
                config, dataset_id, dimension_mapping_files, submitter, log_message
            )

    def _submit_dataset(
        self, project_config, dataset_id, dimension_mapping_files, submitter, log_message
    ):
        self._check_if_not_registered(project_config.config_id)
        dataset_config = self._dataset_mgr.get_by_id(dataset_id)
        dataset_model = project_config.get_dataset(dataset_id)
        if dataset_model.status == DatasetRegistryStatus.REGISTERED:
            raise DSGDuplicateValueRegistered(
                f"dataset={dataset_id} has already been submitted to project={project_config.config_id}"
            )

        mapping_references = []
        for filename in dimension_mapping_files:
            for ref in DimensionMappingReferenceListModel.load(filename).references:
                if not self.dimension_mapping_manager.has_id(ref.mapping_id, version=ref.version):
                    raise DSGValueNotRegistered(f"mapping_id={ref.mapping_id}")
                mapping_references.append(ref)

        project_config.add_dataset_dimension_mappings(dataset_config, mapping_references)
        dataset = Dataset.load(dataset_config)
        # TODO: does this function need mapping_references?
        #self.check_dataset_base_to_project_base_mappings(project_config, dataset_config, dataset)

        if self.dry_run_mode:
            logger.info(
                "%s Dataset submission to project validated dataset_id=%s project_id=%s",
                self._log_dry_run_mode_prefix(),
                dataset_id,
                project_config.config_id,
            )
            return

        dataset_model.status = DatasetRegistryStatus.REGISTERED
        if project_config.are_all_datasets_submitted():
            new_status = ProjectRegistryStatus.COMPLETE
        else:
            new_status = ProjectRegistryStatus.IN_PROGRESS
        project_config.set_status(new_status)
        version = self._update(project_config, submitter, VersionUpdateType.MINOR, log_message)

        if not self.offline_mode:
            self.sync_push(self.get_registry_directory(project_config.config_id))

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_id,
            version,
            project_config.config_id,
        )

    #def check_dataset_base_to_project_base_mappings(self, project_config, dataset_config, dataset):
    #    """Check that the dataset contains all mappings of base dimensions.

    #    Parameters
    #    ----------
    #    project_config : ProjectConfig
    #    dataset_config : DatasetConfig
    #    dataset : Dataset

    #    Raises
    #    ------
    #    DSGInvalidDimensionMapping
    #        Raised if a requirement is violated.

    #    """
    #    needs_full_join = set()
    #    types = [x for x in DimensionType if x != DimensionType.TIME]
    #    for type1, type2 in itertools.product(types, types):
    #        if type1 != type2:
    #            needs_full_join.add((type1, type2))

    #    for mapping_ref in project_config.model.dimension_mappings.base_to_base:
    #        from_dimension = mapping_ref.from_dimension_type
    #        to_dimension = mapping_ref.to_dimension_type
    #        needs_full_join.remove((from_dimension, to_dimension))
    #        mapping_config = self._dimension_mapping_mgr.get_by_id(mapping_ref.mapping_id)
    #        mapping_records = models_to_dataframe(mapping_config.model.records)
    #        # TODO perform checks across mapping_records, dataset.load_data, dataset.load_data_lookup

    #    # TODO Perform checks on all from-to dimensions in needs_full_join.

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = ProjectConfig.load(
            config_file, self.dimension_manager, self.dimension_mapping_manager
        )
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            # TODO DSGRID-81: Adding new dimensions or dimension mappings requires
            # re-validation of registered datasets.
            # TODO: When/how does the project status get updated?
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        registry = self.get_registry_config(config.config_id)
        old_key = ConfigKey(config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.config_id, version)
        self._projects.pop(old_key, None)
        self._projects[new_key] = config
        return version

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._projects if x.id == config_id]:
            self._projects.pop(key)
