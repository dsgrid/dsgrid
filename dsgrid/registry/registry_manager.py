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
    DIMENSION_FILENAME,
    LOCAL_REGISTRY,
    S3_REGISTRY,
)
from dsgrid.data_models import serialize_model
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceListModel
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.exceptions import DSGValueNotStored
from dsgrid.filesytem.factory import make_filesystem_interface
from .common import (
    RegistryType,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
    VersionUpdateType,
    ConfigRegistrationModel,
    ConfigKey,
    make_initial_config_registration,
    make_registry_id,
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
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase
from dsgrid.utils.files import dump_data, load_data


logger = logging.getLogger(__name__)


class RegistryManager(RegistryManagerBase):
    """Manages registration of all projects and datasets.

    Whichever module loads this class will sync the official registry to the local
    system and run from there. This uses a FilesystemInterface object to allow
    remote operations as well.

    """

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._projects = {}  # (project_id, version) to ProjectConfig. Loaded on demand.
        self._project_registries = {}  # project_id to ProjectRegistry. Loaded on demand.
        self._datasets = {}  # dataset_id to DatasetConfig. Loaded on demand.
        self._dataset_registries = {}  # dataset_id to DatasetRegistry. Loaded on demand.
        self._dimension_mgr = DimensionRegistryManager(
            Path(path) / DimensionRegistry.registry_path(), fs_interface
        )
        self._dimension_mapping_dimension_mgr = DimensionMappingRegistryManager(
            Path(path) / DimensionMappingRegistry.registry_path(), fs_interface
        )

        project_ids = self._fs_intf.listdir(
            self._path / ProjectRegistry.registry_path(),
            directories_only=True,
            exclude_hidden=True,
        )
        dataset_ids = self._fs_intf.listdir(
            self._path / DatasetRegistry.registry_path(),
            directories_only=True,
            exclude_hidden=True,
        )
        self._project_ids = set(project_ids)
        self._dataset_ids = set(dataset_ids)

    @classmethod
    def create(cls, path):
        """Creates a new RegistryManager at the given path.

        Parameters
        ----------
        path : str

        Returns
        -------
        RegistryManager

        """
        # TODO S3
        if str(path).startswith("s3"):
            raise Exception(f"s3 is not currently supported: {path}")

        fs_interface = make_filesystem_interface(path)
        fs_interface.mkdir(path)
        fs_interface.mkdir(path / DatasetRegistry.registry_path())
        fs_interface.mkdir(path / ProjectRegistry.registry_path())
        fs_interface.mkdir(path / DimensionRegistry.registry_path())
        fs_interface.mkdir(path / DimensionMappingRegistry.registry_path())
        logger.info("Created registry at %s", path)
        return cls(path, fs_interface)

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_dimension_mgr

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @classmethod
    def load(cls, path):
        """Loads a registry from the given path.

        Parameters
        ----------
        path : str

        Returns
        -------
        RegistryManager

        """
        # TODO S3
        if str(path).startswith("s3"):
            raise Exception(f"S3 is not yet supported: {path}")
        fs_interface = make_filesystem_interface(path)
        path = Path(path)
        for dir_name in (
            path,
            path / DatasetRegistry.registry_path(),
            path / ProjectRegistry.registry_path(),
            path / DimensionRegistry.registry_path(),
            path / DimensionMappingRegistry.registry_path(),
        ):
            if not fs_interface.exists(str(dir_name)):
                raise FileNotFoundError(f"{dir_name} does not exist")

        return cls(path, fs_interface)

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
        ValueError
            Raised if the project_id is not stored.

        """
        if project_id not in self._project_ids:
            raise ValueError(f"project={project_id} is not registered")

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
            raise ValueError(f"project={project_id} is not registered")

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
            raise DSGValueNotStored(
                f"config file for project={project_id} {version} does not exist"
            )

        project_config = ProjectConfig.load(config_file)
        project_config.load_dimensions(self._dimension_mgr)
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
            raise ValueError(f"dataset={dataset_id} is not registered")

        dataset_config = self._datasets.get(dataset_id)
        if dataset_config is not None:
            logger.debug("Loaded DatasetConfig for dataset_id=%s from cache", dataset_id)
            return dataset_config

        registry = self.load_dataset_registry(dataset_id)
        config_file = self._get_dataset_config_file(dataset_id, registry.version)
        dataset_config = DatasetConfig.load(config_file)
        dataset_config.load_dimensions(self._dimension_mgr)
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
            raise ValueError(f"dataset={dataset_id} is not registered")

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
            raise ValueError(f"project={project_id} is not registered")

        registry = self._project_registries.get(project_id)
        if registry is not None:
            logger.debug("Loaded ProjectRegistry for project_id=%s from cache", project_id)
            return registry

        filename = self._get_registry_filename(ProjectRegistry, project_id)
        logger.info("Loaded ProjectRegistry for project_id=%s", project_id)
        return ProjectRegistry.load(filename)

    @staticmethod
    def assign_dimension_id(data: dict):
        """Assign dimension_id to each dimension. Print the assigned IDs."""

        # TODO: check that id does not already exist in .dsgrid-registry
        # TODO: need regular expression check on name and/or limit number of chars in dim id
        # TODO: currently a dimension record can be registered indefinitely,
        # each time with a different UUID. Need to enforce that it is registered only once.
        #   Potential solution: use hash() to check records on file and records to be submitted;
        #   Can use this function to check whether a record exists and suggest the data_submitter
        #   to use that record id if available
        # NOTE: this has been implemented for dimension mappings and we could use the
        # same logic.

        dim_data = data["dimensions"]
        logger.info("Dimension record ID assignment:")
        for item in dim_data:
            logger.info(" - type: %s, name: %s", item["type"], item["name"])

            # assign id, made from dimension.name and a UUID4
            item["id"] = make_registry_id([item["name"].lower().replace(" ", "_")])
            logger.info("   id: %s", item["id"])

    def _register_dimension_config(
        self, registry_type, config_file, submitter, log_message, config_data
    ):
        """
        - It validates that the configuration meets all requirements.
        - It files dimension records by dimension type in output registry folder.
        """

        # TODO: search also from dataset/../dimensions folder for records and find a union record set

        # Just verify that it loads.
        DimensionConfig.load(config_file)
        version = VersionInfo(major=1)

        registration = ConfigRegistrationModel(
            version=version,
            submitter=submitter,
            date=datetime.now(),
            log_message=log_message,
        )

        config_file_name = "dimension" + os.path.splitext(config_file)[1]

        dim_data = config_data["dimensions"]
        n_registered_dims = 0
        for item in dim_data:
            # Leading directories from the original are not relevant in the registry.
            orig_file = item.get("file")
            if orig_file is not None:
                # Time dimensions do not have a record file.
                item["file"] = os.path.basename(orig_file)

            registry_config = RegistryBaseModel(
                version=version,
                description=item["description"].strip(),
                registration_history=[registration],
            )
            config_dir = self._get_dimension_directory()

            data_type_dir = config_dir / item["type"]
            data_dir = data_type_dir / item["id"] / str(version)
            self._fs_intf.mkdir(data_type_dir)
            self._fs_intf.mkdir(data_dir)

            filename = Path(os.path.dirname(data_dir)) / REGISTRY_FILENAME
            data = serialize_model(registry_config)

            # TODO: if we want to update AWS directly, this needs to change.

            # export registry.toml
            dump_data(data, filename)

            # export individual dimension_config.toml
            dump_data(item, data_dir / config_file_name)

            # export dimension record file
            if orig_file is not None:
                dimension_record = Path(os.path.dirname(config_file)) / orig_file
                self._fs_intf.copy_file(
                    dimension_record, data_dir / os.path.basename(item["file"])
                )

            n_registered_dims += 1

        logger.info(
            "Registered %s %s(s) with version=%s", n_registered_dims, registry_type.value, version
        )
        return version

    def _config_file_extend_name(self, config_file, name_extension):
        """Add name extension to existing config_file"""
        name_extension = str(name_extension).lower().replace(" ", "_")
        return (
            os.path.splitext(config_file)[0]
            + "_"
            + name_extension
            + os.path.splitext(config_file)[1]
        )

    def register_dimensions(self, config_file, submitter, log_message):
        """Registers dimensions for projects and datasets.

        Parameters
        ----------
        config_file : str
            Path to dimension config file
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.

        """
        data = load_data(config_file)
        self.assign_dimension_id(data)

        self._register_dimension_config(
            RegistryType.DIMENSION, config_file, submitter, log_message, data
        )

        # save a copy to
        config_file_updated = self._config_file_extend_name(config_file, "with assigned id")
        dump_data(data, config_file_updated)

        logger.info(
            "--> New config file containing the dimension ID assignment exported: %s",
            config_file_updated,
        )

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

        """
        config = ProjectConfig.load(config_file)
        config.load_dimensions(self._dimension_mgr)
        if config.model.project_id in self._project_ids:
            raise ValueError(f"{config.model.project_id} is already registered")

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
        self._fs_intf.mkdir(data_dir)
        registry_filename = config_dir / REGISTRY_FILENAME
        dump_data(serialize_model(registry_model), registry_filename)

        config_filename = data_dir / ("project" + os.path.splitext(config_file)[1])
        dump_data(serialize_model(config.model), config_filename)

        logger.info("Registered project %s with version=%s", config.model.project_id, version)
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
            raise ValueError(f"{project_id} is not already stored")

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
            Raised if the config_file or project_id is invalid.
            Raised if the project does not contain this dataset.

        """
        config = DatasetConfig.load(config_file)
        config.load_dimensions(self._dimension_mgr)
        project_registry = self.load_project_registry(project_id)

        if project_registry.has_dataset(config.model.dataset_id, DatasetRegistryStatus.REGISTERED):
            raise ValueError(
                f"dataset={config.model.dataset_id} has already been submitted to project={project_id}"
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
                if not self.dimension_mapping_manager.has_mapping_id(
                    ref.mapping_id, version=ref.version
                ):
                    raise ValueError(f"mapping_id={ref.mapping_id} is not registered")
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

        self._fs_intf.mkdir(data_dir)
        filename = config_dir / REGISTRY_FILENAME
        data = serialize_model(registry_config)
        config_filename = "dataset" + os.path.splitext(config_file)[1]
        dump_data(data, filename)
        self._fs_intf.copy_file(config_file, data_dir / config_filename)

        logger.info(
            "Registered dataset %s with version=%s in project %s",
            config.model.dataset_id,
            version,
            project_id,
        )

        status = DatasetRegistryStatus.REGISTERED
        project_registry.set_dataset_status(config.model.dataset_id, status)
        filename = self._get_registry_filename(ProjectRegistry, project_id)
        project_registry.serialize(filename)

        project_config.get_dataset(config.model.dataset_id).status = status
        project_file = self._get_project_config_file(
            project_config.model.project_id, project_registry.version
        )
        dump_data(serialize_model(project_config.model), project_file)

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

        """
        assert False, "not tested and probably not correct"
        if dataset_id not in self._dataset_ids:
            raise ValueError(f"{dataset_id} is not already stored")

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
        ValueError
            Raised if the dataset_id is not registered.

        """
        if dataset_id not in self._dataset_ids:
            raise ValueError(f"dataset={dataset_id} is not registered")

        self._fs_intf.rmtree(self._get_dataset_directory(dataset_id))

        for project_registry in self._project_registries.values():
            if project_registry.has_dataset(dataset_id, DatasetRegistryStatus.REGISTERED):
                project_registry.set_dataset_status(dataset_id, DatasetRegistryStatus.UNREGISTERED)
                project_registry.serialize(
                    self._get_registry_filename(ProjectRegistry, project_registry.project_id)
                )

        logger.info("Removed %s from the registry.", dataset_id)

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
        return self._path / DimensionRegistry.registry_path() / str(version) / DIMENSION_FILENAME

    def _get_dimension_directory(self):
        return self._path / DimensionRegistry.registry_path()

    def _update_config(
        self, config_id, registry_config, config_file, submitter, update_type, log_message
    ):
        # TODO: need to check that there are indeed changes to the config
        # TODO: if a new version is created but is deleted in .dsgrid-registry, version number should be reset
        #   accordingly, currently it does not.
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
        logger.info(
            "Updated %s %s with version=%s",
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
        registry_path = (
            LOCAL_REGISTRY  # TEMPORARY: Replace with S3_REGISTRY when that is supported
        )
    if not os.path.exists(registry_path):
        raise ValueError(
            f"Registry path {registry_path} does not exist. To create the registry, "
            "run the following commands:\n"
            "  dsgrid registry create $DSGRID_REGISTRY_PATH\n"
            "  dsgrid registry register-project $US_DATA_REPO/dsgrid_project/project.toml\n"
            "  dsgrid registry submit-dataset "
            "$US_DATA_REPO/dsgrid_project/datasets/input/sector_models/comstock/dataset.toml "
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
