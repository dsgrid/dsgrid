"""Manages registration of all projects and datasets."""

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import uuid
import toml

from semver import VersionInfo

import dsgrid.utils.aws as aws
from dsgrid.common import (
    PROJECT_FILENAME,
    REGISTRY_FILENAME,
    DATASET_FILENAME,
    DIMENSION_FILENAME,
    LOCAL_REGISTRY,
    S3_REGISTRY,
)
from dsgrid.dimension.base import serialize_model
from dsgrid.config._config import VersionUpdateType, ConfigRegistrationModel
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.registry.common import RegistryType, DatasetRegistryStatus, ProjectRegistryStatus
from dsgrid.registry.dataset_registry import DatasetRegistry, DatasetRegistryModel
from dsgrid.registry.project_registry import (
    ProjectRegistry,
    ProjectRegistryModel,
    ProjectDatasetRegistryModel,
)
from dsgrid.registry.dimension_registry import DimensionRegistry, DimensionRegistryModel
from dsgrid.utils.files import dump_data, load_data, make_dirs, exists


logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages registration of all projects and datasets."""

    DATASET_REGISTRY_PATH = Path("datasets")
    PROJECT_REGISTRY_PATH = Path("projects")
    DIMENSION_REGISTRY_PATH = Path("dimensions")
    REGEX_S3_PATH = re.compile(r"s3:\/\/(?P<bucket>[\w-]+)\/(?P<path>.*)")

    def __init__(self, path):
        self._on_aws = path.name.lower().startswith("s3")
        self._projects = {}  # project_id to ProjectConfig. Loaded on demand.
        self._project_registries = {}  # project_id to ProjectRegistry. Loaded on demand.
        self._datasets = {}  # dataset_id to DatasetConfig. Loaded on demand.
        self._dataset_registries = {}  # dataset_id to DatasetRegistry. Loaded on demand.
        self._dimensions = {}  # dimension_id to DimensionConfig. Loaded on demand.
        self._dimension_registries = {}  # dimension_id to DimensionRegistry. Loaded on demand.

        if self._on_aws:
            match = self.REGEX_S3_PATH.search(path.name)
            assert match, str(match)
            self._bucket = match.groupdict("bucket")
            self._path = match.groupdict("path")
            project_ids = aws.list_dir_in_bucket(self._bucket, self.PROJECT_REGISTRY_PATH)
            dataset_ids = aws.list_dir_in_bucket(self._bucket, self.DATASET_REGISTRY_PATH)
            dimension_ids = aws.list_dir_in_bucket(self._bucket, self.DIMENSION_REGISTRY_PATH)
        else:
            self._path = path
            project_ids = os.listdir(self._path / self.PROJECT_REGISTRY_PATH)
            dataset_ids = os.listdir(self._path / self.DATASET_REGISTRY_PATH)
            dimension_ids = os.listdir(self._path / self.DIMENSION_REGISTRY_PATH)
        self._project_ids = set(project_ids)
        self._dataset_ids = set(dataset_ids)
        self._dimension_ids = set(dimension_ids)

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

        path = Path(path)
        make_dirs(path, exist_ok=True)
        make_dirs(path / RegistryManager.DATASET_REGISTRY_PATH, exist_ok=True)
        make_dirs(path / RegistryManager.PROJECT_REGISTRY_PATH, exist_ok=True)
        make_dirs(path / RegistryManager.DIMENSION_REGISTRY_PATH, exist_ok=True)
        logger.info("Created registry at %s", path)
        return cls(path)

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
        for dir_name in (
            path,
            os.path.join(path, RegistryManager.DATASET_REGISTRY_PATH),
            os.path.join(path, RegistryManager.PROJECT_REGISTRY_PATH),
            os.path.join(path, RegistryManager.DIMENSION_REGISTRY_PATH),
        ):
            if not exists(dir_name):
                raise FileNotFoundError(f"{dir_name} does not exist")

        return cls(Path(path))

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

    def list_dimensions(self):
        """Return the projects in the registry.

        Returns
        -------
        list

        """
        return sorted(list(self._dimension_ids))

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
            raise ValueError(f"{project_id} is not stored")

        if self._on_aws:
            assert False  # TODO S3
        else:
            shutil.rmtree(self._get_project_directory(project_id))

        logger.info("Removed %s from the registry.", project_id)

    def load_project_config(self, project_id):
        """Return the ProjectConfig for a project_id. Returns from cache if already loaded.

        Parameters
        ----------
        project_id : str

        Returns
        -------
        ProjectConfig

        """
        if project_id not in self._project_ids:
            raise ValueError(f"project={project_id} is not stored")

        project_config = self._projects.get(project_id)
        if project_config is not None:
            logger.debug("Loaded ProjectConfig for project_id=%s from cache", project_id)
            return project_config

        registry = self.load_project_registry(project_id)
        config_file = self._get_project_config_file(project_id, registry.version)
        project_config = ProjectConfig.load(config_file)
        self._projects[project_id] = project_config
        logger.info("Loaded ProjectConfig for project_id=%s", project_id)
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
            raise ValueError(f"dataset={dataset_id} is not stored")

        dataset_config = self._datasets.get(dataset_id)
        if dataset_config is not None:
            logger.debug("Loaded DatasetConfig for dataset_id=%s from cache", dataset_id)
            return dataset_config

        registry = self.load_dataset_registry(dataset_id)
        config_file = self._get_dataset_config_file(dataset_id, registry.version)
        dataset_config = DatasetConfig.load(config_file)
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
            raise ValueError(f"dataset={dataset_id} is not stored")

        registry = self._dataset_registries.get(dataset_id)
        if registry is not None:
            logger.debug("Loaded DatasetRegistry for dataset_id=%s from cache", dataset_id)
            return registry

        filename = self._get_registry_filename(RegistryType.DATASET, dataset_id)
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
            raise ValueError(f"project={project_id} is not stored")

        registry = self._project_registries.get(project_id)
        if registry is not None:
            logger.debug("Loaded ProjectRegistry for project_id=%s from cache", project_id)
            return registry

        filename = self._get_registry_filename(RegistryType.PROJECT, project_id)
        logger.info("Loaded ProjectRegistry for project_id=%s", project_id)
        return ProjectRegistry.load(filename)

    def assign_dimension_id(self, data: dict):
        """Assign dimension_id only to those with a dimension record (e.g. csv)"""

        # TODO: check that id does not already exist in .dsgrid-registry
        # TODO: need regular expression check on name and/or limit number of chars in dim id
        # TODO: currently a dimension record can be registered indefinitely,
        # each time with a different UUID. Need to enforce that it is registered only once.
        #   Potential solution: use hash() to check records on file and records to be submitted;
        #   Can use this function to check whether a record exists and suggest the data_submitter
        #   to use that record id that is already on file

        dim_data = data["dimensions"]
        logger.info("Dimension record ID assignment:")
        for i in range(len(dim_data)):
            logger.info(" - type: %s, name: %s", dim_data[i]["type"], dim_data[i]["name"])
            if "file" in dim_data[i].keys():
                dim_data[i]["id"] = (
                    dim_data[i]["name"].lower().replace(" ", "_") + "_" + str(uuid.uuid1())
                )
                logger.info("   id: %s", dim_data[i]["id"])
            else:
                logger.info(f"   no record found.")

        data["dimensions"] = dim_data
        return data

    def _register_dimension_config(
        self, registry_type, config_file, submitter, log_message, config_data
    ):
        """
        This is a variation of `_register_config`
        - It validates that the configuration meets all requirements.
        - It files dimension records by dimension type in output registry folder.
        """

        config = self._load_config(config_file, registry_type)
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
        for i in range(len(dim_data)):
            if "file" in dim_data[i].keys():

                registry_config = DimensionRegistryModel(
                    version=version,
                    description=dim_data[i]["description"],
                    registration_history=[registration],
                )
                config_dir = self._get_dimension_directory()

                data_type_dir = config_dir / dim_data[i]["type"]
                os.makedirs(data_type_dir, exist_ok=True)

                data_dir = data_type_dir / dim_data[i]["id"] / str(version)
                if self._on_aws:
                    pass  # TODO S3
                else:
                    if os.path.exists(data_dir):  # <--- temp, to be deleted
                        shutil.rmtree(data_dir)  # <--- temp, to be deleted
                    os.makedirs(data_dir)

                filename = Path(os.path.dirname(data_dir)) / REGISTRY_FILENAME
                data = serialize_model(registry_config)
                if self._on_aws:
                    # TODO S3: not handled
                    assert False
                else:
                    dump_data(data, filename)  # export registry.toml

                    # export individual dimension_record.toml
                    config_file_i = data_dir / config_file_name
                    with open(config_file_i, "w") as toml_file:
                        toml.dump(dim_data[i], toml_file)

                    # export/copy dimension record
                    dimension_record = Path(os.path.dirname(config_file)) / dim_data[i]["file"]
                    shutil.copyfile(
                        dimension_record, data_dir / os.path.basename(dim_data[i]["file"])
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

    def register_dimension(self, config_file, submitter, log_message):
        """Registers dimensions from project and its dataset.

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
        data = load_data(config_file)
        data = self.assign_dimension_id(data)

        # save a copy to
        config_file_updated = self._config_file_extend_name(config_file, "with assigned id")
        with open(config_file_updated, "w") as toml_file:
            toml.dump(data, toml_file)

        logger.info(
            "--> New config file containing the dimension ID assignment exported: \n%s",
            config_file_updated,
        )

        self._register_dimension_config(
            RegistryType.DIMENSION, config_file, submitter, log_message, data
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
        data = load_data(config_file)
        project_id = data["project_id"]
        if project_id in self._project_ids:
            raise ValueError(f"{project_id} is already stored")

        self._register_config(
            RegistryType.PROJECT,
            project_id,
            config_file,
            submitter,
            log_message,
        )

    def update_project(self, project_id, config_file, submitter, update_type, log_message):
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
            Path to project config file
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.

        """
        # assert False, "not tested and probably not correct"
        if project_id not in self._project_ids:
            raise ValueError(f"{project_id} is not already stored")

        registry_file = self._get_registry_filename(RegistryType.PROJECT, project_id)
        registry_config = ProjectRegistryModel(**load_data(registry_file))
        if update_type in ["major", "MAJOR"]:
            update_type = VersionUpdateType.MAJOR
        elif update_type in ["minor", "MINOR"]:
            update_type = VersionUpdateType.MINOR
        elif update_type in ["patch", "PATCH"]:
            update_type = VersionUpdateType.PATCH
        else:
            raise ValueError(" invalid 'update_type', options: major | minor | patch")

        self._update_config(
            project_id, registry_config, config_file, submitter, update_type, log_message
        )

    def submit_dataset(self, config_file, project_id, submitter, log_message):
        """Registers a new dataset with a dsgrid project.

        Parameters
        ----------
        config_file : str
            Path to dataset config file
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file or project_id is invalid.
            Raised if the project does not contain this dataset.

        """
        data = load_data(config_file)
        dataset_id = data["dataset_id"]
        if dataset_id in self._dataset_ids:
            raise ValueError(f"{dataset_id} is already stored")

        project_registry = self.load_project_registry(project_id)
        if project_registry.has_dataset(dataset_id, DatasetRegistryStatus.REGISTERED):
            raise ValueError(f"{dataset_id} is already stored")

        project_config = self.load_project_config(project_id)
        if not project_config.has_dataset(dataset_id):
            raise ValueError(f"{dataset_id} is not defined in project={project_id}")

        version = self._register_config(
            RegistryType.DATASET,
            dataset_id,
            config_file,
            submitter,
            log_message,
        )

        project_registry.set_dataset_status(dataset_id, DatasetRegistryStatus.REGISTERED)
        filename = self._get_registry_filename(RegistryType.PROJECT, project_id)
        project_registry.serialize(filename)

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

        registry_file = self._get_registry_filename(RegistryType.DATASET, dataset_id)
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
            Raised if the dataset_id is not stored.

        """
        if dataset_id not in self._dataset_ids:
            raise ValueError(f"{dataset_id} is not stored")

        if self._on_aws:
            assert False  # TODO S3
        else:
            shutil.rmtree(self._get_dataset_directory(dataset_id))

        for project_registry in self._project_registries.values():
            if project_registry.has_dataset(dataset_id, DatasetRegistryStatus.REGISTERED):
                project_registry.set_dataset_status(dataset_id, DatasetRegistryStatus.UNREGISTERED)
                project_registry.serialize(
                    self._get_registry_filename(RegistryType.PROJECT, project_registry.project_id)
                )

        logger.info("Removed %s from the registry.", dataset_id)

    def _get_registry_filename(self, registry_type, config_id):
        if registry_type == RegistryType.DATASET:
            sub_path = self.DATASET_REGISTRY_PATH
        elif registry_type == RegistryType.PROJECT:
            sub_path = self.PROJECT_REGISTRY_PATH
        else:
            assert False

        return self._path / sub_path / config_id / REGISTRY_FILENAME

    def _get_dataset_config_file(self, dataset_id, version):
        return (
            self._path / self.DATASET_REGISTRY_PATH / dataset_id / str(version) / DATASET_FILENAME
        )

    def _get_dataset_directory(self, dataset_id):
        return self._path / self.DATASET_REGISTRY_PATH / dataset_id

    def _get_project_config_file(self, project_id, version):
        return (
            self._path / self.PROJECT_REGISTRY_PATH / project_id / str(version) / PROJECT_FILENAME
        )

    def _get_project_directory(self, project_id):
        return self._path / self.PROJECT_REGISTRY_PATH / project_id

    def _get_dimension_config_file(self, version):
        # need to change
        return self._path / self.DIMENSION_REGISTRY_PATH / str(version) / DIMENSION_FILENAME

    def _get_dimension_directory(self):
        return self._path / self.DIMENSION_REGISTRY_PATH

    def _load_config(self, config_file, registry_type):
        if not os.path.exists(config_file):
            raise ValueError(f"config file {config_file} does not exist")

        if registry_type == RegistryType.DATASET:
            config = DatasetConfig.load(config_file)
        elif registry_type == RegistryType.PROJECT:
            config = ProjectConfig.load(config_file)
        elif registry_type == RegistryType.DIMENSION:
            config = DimensionConfig.load(config_file)

        return config

    def _register_config(self, registry_type, config_id, config_file, submitter, log_message):
        """This validates that the configuration meets all requirements."""

        config = self._load_config(config_file, registry_type)
        description = load_data(config_file)["description"]
        version = VersionInfo(major=1)

        registration = ConfigRegistrationModel(
            version=version,
            submitter=submitter,
            date=datetime.now(),
            log_message=log_message,
        )

        if registry_type == RegistryType.DATASET:
            registry_config = DatasetRegistryModel(
                dataset_id=config_id,
                version=version,
                description=description,
                registration_history=[registration],
            )
            config_dir = self._get_dataset_directory(config_id)

        elif registry_type == RegistryType.PROJECT:
            registry_config = ProjectRegistryModel(
                project_id=config_id,
                version=version,
                status=ProjectRegistryStatus.INITIAL_REGISTRATION,
                description=description,
                dataset_registries=[
                    ProjectDatasetRegistryModel(
                        dataset_id=dataset_id,
                        status=DatasetRegistryStatus.UNREGISTERED,
                    )
                    for dataset_id in config.iter_dataset_ids()
                ],
                registration_history=[registration],
            )
            config_dir = self._get_project_directory(config_id)

        data_dir = config_dir / str(version)

        if self._on_aws:
            pass  # TODO S3
        else:
            if os.path.exists(data_dir):  # <--- temp, to be deleted
                shutil.rmtree(data_dir)  # <--- temp, to be deleted
            os.makedirs(data_dir)
        filename = config_dir / REGISTRY_FILENAME
        data = serialize_model(registry_config)

        if registry_type == RegistryType.DATASET:
            config_file_name = "dataset"
        elif registry_type == RegistryType.PROJECT:
            config_file_name = "project"
        config_file_name = config_file_name + os.path.splitext(config_file)[1]
        if self._on_aws:
            # TODO S3: not handled
            assert False
        else:
            dump_data(data, filename)
            shutil.copyfile(config_file, data_dir / config_file_name)
            dimensions_dir = Path(os.path.dirname(config_file)) / "dimensions"
            shutil.copytree(dimensions_dir, data_dir / "dimensions")

        logger.info("Registered %s %s with version=%s", registry_type.value, config_id, version)
        return version

    def _update_config(
        self, config_id, registry_config, config_file, submitter, update_type, log_message
    ):
        # TODO: need to check that there are indeed changes to the config
        # TODO: if a new version is created but is deleted in .dsgrid-registry, version number should be reset
        #   accordingly, currently it does not.
        if isinstance(registry_config, DatasetRegistryModel):
            registry_type = RegistryType.DATASET
        else:
            registry_type = RegistryType.PROJECT

        # This validates that all data.
        config = self._load_config(config_file, registry_type)  # is this needed?
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
        filename = self._get_registry_filename(registry_type, config_id)
        config_dir = self._get_project_directory(config_id)
        data_dir = config_dir / str(registry_config.version)
        if self._on_aws:
            pass  # TODO S3
        else:
            if os.path.exists(data_dir):  # <--- temp, to be deleted
                shutil.rmtree(data_dir)  # <--- temp, to be deleted
            os.makedirs(data_dir)

        if registry_type == RegistryType.DATASET:
            config_file_name = "dataset"
        elif registry_type == RegistryType.PROJECT:
            config_file_name = "project"
        config_file_name = config_file_name + os.path.splitext(config_file)[1]

        # TODO: account for S3
        dump_data(serialize_model(registry_config), filename)
        shutil.copyfile(config_file, data_dir / config_file_name)  # copy new config file
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
