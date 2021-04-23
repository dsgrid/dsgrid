"""Manages registration of all projects and datasets."""

import logging
import os
from datetime import datetime
from pathlib import Path

from semver import VersionInfo

from dsgrid.common import (
    PROJECT_FILENAME,
    REGISTRY_FILENAME,
    DATASET_FILENAME,
    DIMENSIONS_FILENAME,
    LOCAL_REGISTRY,
    REMOTE_REGISTRY,
)
from dsgrid.data_models import serialize_model
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceListModel
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
from dsgrid.filesytem.cloud_filesystem import CloudFilesystemInterface
from dsgrid.filesytem.factory import make_filesystem_interface
from dsgrid.filesytem.local_filesystem import LocalFilesystem
from .common import (
    RegistryType,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
    VersionUpdateType,
    ConfigRegistrationModel,
    ConfigKey,
)
from .dimension_mapping_registry import DimensionMappingRegistry
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dataset_registry import DatasetRegistry, DatasetRegistryModel
from .dimension_registry import DimensionRegistry
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry import (
    ProjectRegistry,
    ProjectRegistryModel,
    ProjectDatasetRegistryModel,
)
from dsgrid.utils.files import dump_data, load_data
from dsgrid.config.dimensions import DimensionType

logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages registration of all projects and datasets.

    Whichever module loads this class will sync the official registry to the local system and run
    from there. This uses a FilesystemInterface object to allow remote operations as well.

    """

    # TODO: how are we supporting offline mode?

    def __init__(self, path, fs_interface, cloud_interface, offline_mode, dryrun_mode):
        # if isinstance(fs_interface, S3Filesystem):
        #     self._path = fs_interface.path
        # else:
        assert isinstance(fs_interface, LocalFilesystem)
        if cloud_interface is not None:
            assert isinstance(cloud_interface, CloudFilesystemInterface)
            self._remote_path = cloud_interface.path
        self._path = path

        self._fs_intf = fs_interface
        self._cld_intf = cloud_interface
        self._offline_mode = offline_mode
        self._dryrun_mode = dryrun_mode
        self._projects = {}  # (project_id, version) to ProjectConfig. Loaded on demand.
        self._project_registries = {}  # project_id to ProjectRegistry. Loaded on demand.
        self._datasets = {}  # dataset_id to DatasetConfig. Loaded on demand.
        self._dataset_registries = {}  # dataset_id to DatasetRegistry. Loaded on demand.
        self._dimension_mgr = DimensionRegistryManager.load(
            Path(path) / DimensionRegistry.registry_path(),
            fs_interface,
            cloud_interface,
            offline_mode,
            dryrun_mode,
        )
        self._dimension_mapping_dimension_mgr = DimensionMappingRegistryManager.load(
            Path(path) / DimensionMappingRegistry.registry_path(),
            fs_interface,
            cloud_interface,
            offline_mode,
            dryrun_mode,
        )

        project_ids = self._fs_intf.listdir(
            self._path / ProjectRegistry.registry_path(),
            directories_only=True,
            exclude_hidden=True,
            recursive=False,
        )
        dataset_ids = self._fs_intf.listdir(
            self._path / DatasetRegistry.registry_path(),
            directories_only=True,
            exclude_hidden=True,
            recursive=False,
        )
        self._project_ids = set(project_ids)
        self._dataset_ids = set(dataset_ids)

    @classmethod
    # TODO: Is this method needed?
    def create(cls, path):
        """Creates a new RegistryManager at the given path.

        Parameters
        ----------
        path : str

        Returns
        -------
        RegistryManager

        """
        fs_interface = make_filesystem_interface(path)
        path = Path(path)
        fs_interface.mkdir(path)
        fs_interface.mkdir(path / "data")  # TODO: replace with DataRegistry.registry_path()
        fs_interface.mkdir(path / DatasetRegistry.registry_path())
        fs_interface.mkdir(path / ProjectRegistry.registry_path())
        fs_interface.mkdir(path / DimensionRegistry.registry_path())
        fs_interface.mkdir(path / DimensionMappingRegistry.registry_path())
        for dim_type in DimensionType:
            for dir_name in (path / DimensionRegistry.registry_path() / dim_type.value,):
                if not fs_interface.exists(str(dir_name)):
                    fs_interface.mkdir(dir_name)
        logger.info("Created registry at %s", path)
        return cls(path, fs_interface, None, None, None)  # TODO: this needs a cloud interface

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_dimension_mgr

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @classmethod
    def load(cls, path, offline_mode=False, dryrun_mode=False):
        """Loads a local registry from a given path and loads the remote registry if offline_mode
         is False. If offline_mode is False, load() also syncs the local registry with the remote
         registry.

        Parameters
        ----------
        path : str
            Local registry path
        offline_mode : bool
            Load registry in offline mode; default is False
        dryrun_mode : bool
            Test registry operations in dry-run "test" mode (i.e., do not commit changes to remote)

        Returns
        -------
        RegistryManager

        """

        if offline_mode:
            cloud_interface = None
        else:
            cloud_interface = make_filesystem_interface(REMOTE_REGISTRY)
            cloud_interface.sync_pull(local_registry=path)
        fs_interface = make_filesystem_interface(path)
        path = Path(path)
        for dir_name in (
            path,
            path / "data",  # TODO: replace with DataRegistry.registry_path() ?
            path / DatasetRegistry.registry_path(),
            path / ProjectRegistry.registry_path(),
            path / DimensionRegistry.registry_path(),
            path / DimensionMappingRegistry.registry_path(),
        ):
            if not fs_interface.exists(str(dir_name)):
                fs_interface.mkdir(dir_name)
        # NOTE: @dtom we actually want to make these dirs if they do not exist, esp. for now
        # since syncing doesn't sync empty folders. Does this mean we should call create()?
        # NOTE: if dimension type dirs do not exist, then we need to make them or else it fails.
        # maybe we can find a better way around this.
        for dim_type in DimensionType:
            for dir_name in (path / DimensionRegistry.registry_path() / dim_type.value,):
                if not fs_interface.exists(str(dir_name)):
                    fs_interface.mkdir(dir_name)
        logger.info(f"Loaded local registry at {path}")
        return cls(path, fs_interface, cloud_interface, offline_mode, dryrun_mode)

    @property
    def log_offline_message(self):
        if self._offline_mode:
            msg = "* OFFLINE MODE * | "
        else:
            msg = ""
        return msg

    def list_datasets(self):
        """Return the datasets in the registry.

        Returns
        -------
        list

        """
        return sorted(list(self._dataset_ids))

    def list_projects(self):
        """Return the projects in the registry.

        Returns
        -------
        list

        """
        return sorted(list(self._project_ids))

    def remove_project(self, project_id):
        """Remove a project from the registry

        Parameters
        ----------
        project_id : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id is not registered.

        """
        if project_id not in self._project_ids:
            raise DSGValueNotRegistered(f"project_id={project_id}")

        self._fs_intf.rmtree(self._get_project_directory(project_id))
        logger.info("Removed %s from the registry.", project_id)

    def load_project_config(self, project_id, version=None, registry=None):
        """Return the ProjectConfig for a project_id. Returns from cache if already loaded.

        Parameters
        ----------
        project_id : str
        version : VersionInfo | None
            Use the latest if not specified.

        Returns
        -------
        ProjectConfig

        """
        if project_id not in self._project_ids:
            raise DSGValueNotRegistered(f"project_id={project_id}")

        if version is None:
            if registry is None:
                registry = self.load_project_registry(project_id)
            version = registry.version

        key = ConfigKey(project_id, version)
        project_config = self._projects.get(key)
        if project_config is not None:
            logger.debug("Loaded ProjectConfig for project_id=%s from cache", key)
            return project_config

        config_file = self._get_project_config_file(project_id, version)
        if not self._fs_intf.exists(config_file):
            raise DSGValueNotRegistered(
                f"config file for project_id={project_id} {version} does not exist"
            )

        project_config = ProjectConfig.load(config_file, self._dimension_mgr)
        self._projects[key] = project_config
        logger.info("Loaded ProjectConfig for project_id=%s", key)
        return project_config

    def load_dataset_config(self, dataset_id):
        """Return the DatasetConfig for a dataset_id. Returns from cache if already loaded.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        DatasetConfig

        """
        if dataset_id not in self._dataset_ids:
            raise DSGValueNotRegistered(f"dataset_id={dataset_id}")

        dataset_config = self._datasets.get(dataset_id)
        if dataset_config is not None:
            logger.debug("Loaded DatasetConfig for dataset_id=%s from cache", dataset_id)
            return dataset_config

        registry = self.load_dataset_registry(dataset_id)
        config_file = self._get_dataset_config_file(dataset_id, registry.version)
        dataset_config = DatasetConfig.load(config_file, self._dimension_mgr)
        self._datasets[dataset_id] = dataset_config
        logger.info("Loaded DatasetConfig for dataset_id=%s", dataset_id)
        return dataset_config

    def load_dataset_registry(self, dataset_id):
        """Return the DatasetRegistry for a dataset_id. Returns from cache if already loaded.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        DatasetRegistry

        """
        if dataset_id not in self._dataset_ids:
            raise DSGValueNotRegistered(f"dataset_id={dataset_id}")

        registry = self._dataset_registries.get(dataset_id)
        if registry is not None:
            logger.debug("Loaded DatasetRegistry for dataset_id=%s from cache", dataset_id)
            return registry

        filename = self._get_registry_filename(DatasetRegistry, dataset_id)
        logger.info("Loaded DatasetRegistry for dataset_id=%s", dataset_id)
        return DatasetRegistry.load(filename)

    def load_project_registry(self, project_id):
        """Return the ProjectRegistry for a project_id. Returns from cache if already loaded.

        Parameters
        ----------
        project_id : str

        Returns
        -------
        ProjectRegistry

        """
        if project_id not in self._project_ids:
            raise DSGValueNotRegistered(f"project_id={project_id}")

        registry = self._project_registries.get(project_id)
        if registry is not None:
            logger.debug("Loaded ProjectRegistry for project_id=%s from cache", project_id)
            return registry

        filename = self._get_registry_filename(ProjectRegistry, project_id)
        logger.info("Loaded ProjectRegistry for project_id=%s", project_id)
        return ProjectRegistry.load(filename)

    def register_project(self, config_file, submitter, log_message):
        """Registers a new project with DSGRID.

        Parameters
        ----------
        project_id : str
            Unique identifier for project
        config_file : str
            Path to project config file
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the project_id is already registered.

        """
        config = ProjectConfig.load(config_file, self._dimension_mgr)
        if config.model.project_id in self._project_ids:
            raise DSGDuplicateValueRegistered(f"project_id={config.model.project_id}")

        version = VersionInfo(major=1)
        registration = ConfigRegistrationModel(
            version=version,
            submitter=submitter,
            date=datetime.now(),
            log_message=log_message,
        )

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
            version=version,
            status=ProjectRegistryStatus.INITIAL_REGISTRATION,
            description=config.model.description,
            dataset_registries=dataset_registries,
            registration_history=[registration],
        )
        config_dir = self._get_project_directory(config.model.project_id)
        data_dir = config_dir / str(version)

        # Serialize the registry file as well as the updated ProjectConfig to the registry.
        # TODO: Both the registry.toml and project.toml contain dataset status, which is
        # redundant. It needs to be in project.toml so that we can load older versions of a
        # project. It may be convenient to be in the registry.toml for quick searches but
        # should not be required.
        if not self._dryrun_mode:
            self._fs_intf.mkdir(data_dir)
            registry_filename = config_dir / REGISTRY_FILENAME
            dump_data(serialize_model(registry_model), registry_filename)

            config_filename = data_dir / ("project" + os.path.splitext(config_file)[1])
            dump_data(serialize_model(config.model), config_filename)

            # sync with remote registry
            if not self._offline_mode:
                ProjectRegistry.sync_push(self._path)
            logger.info(
                "%sRegistered project %s with version=%s",
                self.log_offline_message,
                config.model.project_id,
                version,
            )
        else:
            logger.info(
                "* DRY-RUN MODE * | Project with version=%s validated for registration",
                config.model.project_id,
                version,
            )

        return version

    def update_project(self, config_file, submitter, update_type, log_message):
        """Updates an existing project with new parameters or data.

        Parameters
        ----------
        project_id : str
            Unique identifier for project
        config_file : str
            Path to project config file
        submitter : str
            Submitter name
        update_type : VersionUpdateType
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.

        """
        data = load_data(config_file)
        project_id = data["project_id"]

        if project_id not in self._project_ids:
            raise DSGValueNotRegistered(f"project_id={project_id}")

        registry_file = self._get_registry_filename(ProjectRegistry, project_id)
        registry_config = ProjectRegistryModel(**load_data(registry_file))
        self._update_config(
            project_id, registry_config, config_file, submitter, update_type, log_message
        )

    def submit_dataset(
        self, config_file, project_id, dimension_mapping_files, submitter, log_message
    ):
        """Registers a new dataset with a dsgrid project. This can only be performed on the
        latest version of the project.

        Parameters
        ----------
        config_file : str
            Path to dataset config file
        project_id : str
        dimension_mapping_files : tuple
            dimension mapping filenames
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
            Raised if the project does not contain this dataset.
        DSGDuplicateValueRegistered
            Raised if the dataset is already registered with the project.

        """
        config = DatasetConfig.load(config_file, self._dimension_mgr)
        project_registry = self.load_project_registry(project_id)

        if project_registry.has_dataset(config.model.dataset_id, DatasetRegistryStatus.REGISTERED):
            raise DSGDuplicateValueRegistered(
                f"dataset={config.model.dataset_id} has already been submitted to "
                "project={project_id}"
            )

        project_config = self.load_project_config(project_id, registry=project_registry)
        if not project_config.has_dataset(config.model.dataset_id):
            raise ValueError(
                f"dataset={config.model.dataset_id} is not defined in project={project_id}"
            )

        assert config.model.dataset_id not in self._dataset_ids, config.model.dataset_id

        mapping_references = []
        for filename in dimension_mapping_files:
            for ref in DimensionMappingReferenceListModel.load(filename).references:
                if not self.dimension_mapping_manager.has_id(ref.mapping_id, version=ref.version):
                    raise DSGValueNotRegistered(f"mapping_id={ref.mapping_id}")
                mapping_references.append(ref)

        project_config.add_dataset_dimension_mappings(config, mapping_references)

        version = VersionInfo(major=1)
        registration = ConfigRegistrationModel(
            version=version,
            submitter=submitter,
            date=datetime.now(),
            log_message=log_message,
        )
        registry_config = DatasetRegistryModel(
            dataset_id=config.model.dataset_id,
            version=version,
            description=config.model.description,
            registration_history=[registration],
        )
        config_dir = self._get_dataset_directory(config.model.dataset_id)
        data_dir = config_dir / str(version)

        if not self._dryrun_mode:
            self._fs_intf.mkdir(data_dir)
            filename = config_dir / REGISTRY_FILENAME
            data = serialize_model(registry_config)
            config_filename = "dataset" + os.path.splitext(config_file)[1]
            dump_data(data, filename)
            self._fs_intf.copy_file(config_file, data_dir / config_filename)
            if not self._offline_mode:
                ProjectRegistry.sync_push(self._path)
            logger.info(
                "%sRegistered dataset %s with version=%s in project %s",
                self.log_offline_message,
                config.model.dataset_id,
                version,
                project_id,
            )

        status = DatasetRegistryStatus.REGISTERED
        project_registry.set_dataset_status(config.model.dataset_id, status)
        filename = self._get_registry_filename(ProjectRegistry, project_id)
        if not self._dryrun_mode:
            project_registry.serialize(filename)

        project_config.get_dataset(config.model.dataset_id).status = status
        project_file = self._get_project_config_file(
            project_config.model.project_id, project_registry.version
        )
        if not self._dryrun_mode:
            dump_data(serialize_model(project_config.model), project_file)
            if not self._offline_mode:
                ProjectRegistry.sync_push(self._path)  # TODO: two syncs in one function?

        self._dataset_ids.add(config.model.dataset_id)

    def update_dataset(self, dataset_id, config_file, submitter, update_type, log_message):
        """Updates an existing dataset with new parameters or data.

        Parameters
        ----------
        dataset_id : str
            Unique identifier for dataset
        config_file : str
            Path to dataset config file
        update_type : VersionUpdateType
            Path to dataset config file
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGValueNotRegistered
            Raised if the dataset_id is not registered.

        """
        assert False, "not tested and probably not correct"
        if dataset_id not in self._dataset_ids:
            raise DSGValueNotRegistered(f"dataset_id={dataset_id}")

        registry_file = self._get_registry_filename(DatasetRegistry, dataset_id)
        registry_config = DatasetRegistryModel(**load_data(registry_file))
        self._update_config(
            dataset_id, registry_config, config_file, submitter, update_type, log_message
        )

    def remove_dataset(self, dataset_id):
        """Remove a dataset from the registry

        Parameters
        ----------
        dataset_id : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the dataset_id is not registered.

        """
        if dataset_id not in self._dataset_ids:
            raise DSGValueNotRegistered(f"dataset_id={dataset_id}")

        self._fs_intf.rmtree(self._get_dataset_directory(dataset_id))

        for project_registry in self._project_registries.values():
            if project_registry.has_dataset(dataset_id, DatasetRegistryStatus.REGISTERED):
                project_registry.set_dataset_status(dataset_id, DatasetRegistryStatus.UNREGISTERED)
                if not self._dryrun_mode:
                    project_registry.serialize(
                        self._get_registry_filename(ProjectRegistry, project_registry.project_id)
                    )
        if not self._dryrun_mode:
            if not self._offline_mode:
                ProjectRegistry.sync_push(self._path)
            logger.info("%sRemoved %s from the registry.", self.log_offline_message, dataset_id)
        else:
            logger.info(
                "* DRY-RUN MODE * | Removal of dataset_id=%s from registry validated",
                self.log_offline_message,
                dataset_id,
            )

    def _get_registry_filename(self, registry_class, config_id):
        return self._path / registry_class.registry_path() / config_id / REGISTRY_FILENAME

    def _get_dataset_config_file(self, dataset_id, version):
        return (
            self._path
            / DatasetRegistry.registry_path()
            / dataset_id
            / str(version)
            / DATASET_FILENAME
        )

    def _get_dataset_directory(self, dataset_id):
        return self._path / DatasetRegistry.registry_path() / dataset_id

    def _get_project_config_file(self, project_id, version):
        return (
            self._path
            / ProjectRegistry.registry_path()
            / project_id
            / str(version)
            / PROJECT_FILENAME
        )

    def _get_project_directory(self, project_id):
        return self._path / ProjectRegistry.registry_path() / project_id

    def _get_dimension_config_file(self, version):
        # need to change
        return self._path / DimensionRegistry.registry_path() / str(version) / DIMENSIONS_FILENAME

    def _update_config(
        self, config_id, registry_config, config_file, submitter, update_type, log_message
    ):
        # TODO: need to check that there are indeed changes to the config
        # TODO: if a new version is created but is deleted in .dsgrid-registry,
        #       version number should be reset
        # desired feature: undo a revision

        if isinstance(registry_config, DatasetRegistryModel):
            registry_type = RegistryType.DATASET
        else:
            registry_type = RegistryType.PROJECT

        # This validates that all data.
        registry_class = get_registry_class(registry_type)
        registry_class.load(config_file)

        registry_config.description = load_data(config_file)[
            "description"
        ]  # always copy the latest from config

        if update_type == VersionUpdateType.MAJOR:
            registry_config.version = registry_config.version.bump_major()
        elif update_type == VersionUpdateType.MINOR:
            registry_config.version = registry_config.version.bump_minor()
        elif update_type == VersionUpdateType.PATCH:
            registry_config.version = registry_config.version.bump_patch()
        else:
            assert False

        registration = ConfigRegistrationModel(
            version=registry_config.version,
            submitter=submitter,
            date=datetime.now(),
            log_message=log_message,
        )
        registry_config.registration_history.append(registration)
        filename = self._get_registry_filename(registry_class, config_id)
        config_dir = self._get_project_directory(config_id)
        data_dir = config_dir / str(registry_config.version)

        if not self._dryrun_mode:
            self._fs_intf.mkdir(data_dir)

            if registry_type == RegistryType.DATASET:
                config_file_name = "dataset"
            elif registry_type == RegistryType.PROJECT:
                config_file_name = "project"
            config_file_name = config_file_name + os.path.splitext(config_file)[1]

            dump_data(serialize_model(registry_config), filename)
            self._fs_intf.copy_file(config_file, data_dir / config_file_name)
            dimensions_dir = Path(os.path.dirname(config_file)) / "dimensions"
            # copy new dimensions, to be removed with dimension id mapping
            self._fs_intf.copy_tree(dimensions_dir, data_dir / "dimensions")

            # **************************
            # TODO: Add SYNC HERE
            # **************************
            if not self._offline_mode:
                if registry_type == RegistryType.DATASET:
                    DatasetRegistry.sync_push(self._path)
                elif registry_type == RegistryType.PROJECT:
                    ProjectRegistry.sync_push(self._path)
            logger.info(
                "%sUpdated %s %s with version=%s",
                self.log_offline_message,
                registry_type.value,
                config_id,
                registry_config.version,
            )
        else:
            logger.info(
                "* DRY-RUN MODE * | Update validated for %s %s with version=%s",
                registry_type.value,
                config_id,
                registry_config.version,
            )


def get_registry_path(registry_path=None):
    """
    Returns the registry_path, defaulting to the DSGRID_REGISTRY_PATH environment
    variable or dsgrid.common.LOCAL_REGISTRY = Path.home() / ".dsgrid-registry"
    if registry_path is None.
    """
    if registry_path is None:
        registry_path = os.environ.get("DSGRID_REGISTRY_PATH", None)
    if registry_path is None:
        registry_path = LOCAL_REGISTRY
    if not os.path.exists(registry_path):
        if "$HOME" in registry_path:
            registry_path = Path.home() / ".dsgrid-registry"
    if not os.path.exists(registry_path):
        raise ValueError(
            f"Registry path {registry_path} does not exist. To create the registry, "
            "run the following commands:\n"
            "  dsgrid registry create $DSGRID_REGISTRY_PATH\n"
            "  dsgrid registry register-project $US_DATA_REPO/dsgrid_project/project.toml\n"
            "  dsgrid registry submit-dataset "
            "$US_DATA_REPO/dsgrid_project/datasets/sector_models/comstock/dataset.toml "
            "-p test -l initial_submission\n"
            "where $US_DATA_REPO points to the location of the dsgrid-data-UnitedStates "
            "repository on your system. If you would prefer a different location, "
            "set the DSGRID_REGISTRY_PATH environment variable before running the commands."
        )
    return registry_path


_REGISTRY_TYPE_TO_CLASS = {
    RegistryType.DATASET: DatasetRegistry,
    RegistryType.DIMENSION: DimensionRegistry,
    RegistryType.DIMENSION_MAPPING: DimensionMappingRegistry,
    RegistryType.PROJECT: ProjectRegistry,
}


def get_registry_class(registry_type):
    """Return the subtype of RegistryBase correlated with registry_type."""
    return _REGISTRY_TYPE_TO_CLASS[registry_type]
