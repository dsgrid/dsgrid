"""Manages the registry for dimension projects"""

import logging
import os
from pathlib import Path

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceListModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
from dsgrid.data_models import serialize_model
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.files import dump_data, load_data
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry import (
    ProjectRegistry,
    ProjectRegistryModel,
    ProjectDatasetRegistryModel,
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
            self.get_config_file(key.id, key.version), self._dimension_mgr
        )
        self._projects[key] = project
        return project

    def register(self, config_file, submitter, log_message, force=False):
        config = ProjectConfig.load(config_file, self._dimension_mgr)
        self._check_if_already_registered(config.model.project_id)

        registration = make_initial_config_registration(submitter, log_message)

        if self.dry_run_mode:
            logger.info(
                "Project validated for registration project_id=%s",
                config.model.project_id,
            )
            return

        dataset_registries = []
        for dataset in config.iter_datasets():
            status = DatasetRegistryStatus.UNREGISTERED
            dataset.status = status
            dataset_registries.append(
                ProjectDatasetRegistryModel(
                    dataset_id=dataset.dataset_id,
                    status=status,
                )
            )
        registry_model = ProjectRegistryModel(
            project_id=config.model.project_id,
            version=registration.version,
            status=ProjectRegistryStatus.INITIAL_REGISTRATION,
            description=config.model.description,
            dataset_registries=dataset_registries,
            registration_history=[registration],
        )
        registry_dir = self.get_registry_directory(config.model.project_id)
        data_dir = registry_dir / str(registration.version)

        # Serialize the registry file as well as the updated ProjectConfig to the registry.
        # TODO: Both the registry.toml and project.toml contain dataset status, which is
        # redundant. It needs to be in project.toml so that we can load older versions of a
        # project. It may be convenient to be in the registry.toml for quick searches but
        # should not be required.
        self.fs_interface.mkdir(data_dir)
        registry_filename = registry_dir / REGISTRY_FILENAME
        dump_data(serialize_model(registry_model), registry_filename)

        config_filename = data_dir / ("project" + os.path.splitext(config_file)[1])
        dump_data(serialize_model(config.model), config_filename)

        self._update_registry_cache(config.model.project_id, registry_model)

        if not self.offline_mode:
            self.cloud_interface.sync_push(registry_dir)

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
        self._check_if_not_registered(project_id)
        registry = self.get_registry_config(project_id)
        if registry.has_dataset(dataset_id, DatasetRegistryStatus.REGISTERED):
            raise DSGDuplicateValueRegistered(
                f"dataset={dataset_id} has already been submitted to project={project_id}"
            )

        dataset_config = self._dataset_mgr.get_by_id(dataset_id)
        project_config = self.get_by_id(project_id)
        if not project_config.has_dataset(dataset_config.model.dataset_id):
            raise ValueError(f"dataset={dataset_id} is not defined in project={project_id}")

        mapping_references = []
        for filename in dimension_mapping_files:
            for ref in DimensionMappingReferenceListModel.load(filename).references:
                if not self.dimension_mapping_manager.has_id(ref.mapping_id, version=ref.version):
                    raise DSGValueNotRegistered(f"mapping_id={ref.mapping_id}")
                mapping_references.append(ref)

        project_config.add_dataset_dimension_mappings(dataset_config, mapping_references)

        if self.dry_run_mode:
            logger.info(
                "Dataset submission to project validated dataset_id=%s project_id=%s",
                dataset_id,
                project_id,
            )
            return

        # The dataset status is recorded in both project registry and config files.
        status = DatasetRegistryStatus.REGISTERED
        registry.set_dataset_status(dataset_id, status)
        filename = self.get_registry_file(project_id)
        registry.serialize(filename)

        project_config.get_dataset(dataset_id).status = status
        project_file = self.get_config_file(project_id, registry.version)
        dump_data(serialize_model(project_config.model), project_file)

        if not self.offline_mode:
            self.cloud_interface.sync_push(self.get_registry_directory(project_id))

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_id,
            registry.version,
            project_id,
        )

        # TODO: update project with new version

    def update(self, config_file, submitter, update_type, log_message):
        assert False, "Updating a project is not currently supported"
        # Commented-out code from registry_manager.py needs to be ported here and tested.
        config = ProjectConfig.load(config_file)
        self._check_if_not_registered(config.model.project_id)

        registry_file = self.get_registry_file(config.model.project_id)
        registry_config = ProjectRegistryModel(**load_data(registry_file))

        self._update_config(
            config.model.project_id,
            registry_config,
            config_file,
            submitter,
            update_type,
            log_message,
        )
