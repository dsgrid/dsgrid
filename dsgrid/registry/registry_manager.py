"""Manages registration of all projects and datasets."""

import logging
import os
from pathlib import Path

from dsgrid.common import (
    LOCAL_REGISTRY,
    REMOTE_REGISTRY,
)
from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.dimension.base_models import DimensionType
from dsgrid.filesystem.factory import make_filesystem_interface
from .common import (
    RegistryType,
    RegistryManagerParams,
)
from .dimension_mapping_registry import DimensionMappingRegistry
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dataset_registry import DatasetRegistry, DatasetRegistryModel
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry import DimensionRegistry
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry import ProjectRegistry
from .project_registry_manager import ProjectRegistryManager


logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages registration of all projects and datasets.

    Whichever module loads this class will sync the official registry to the local
    system and run from there. This uses a FilesystemInterface object to allow
    remote operations as well.

    """

    def __init__(self, params: RegistryManagerParams):
        self._params = params
        self._dimension_mgr = DimensionRegistryManager.load(
            params.base_path / DimensionRegistry.registry_path(), params
        )
        self._dimension_mapping_dimension_mgr = DimensionMappingRegistryManager.load(
            params.base_path / DimensionMappingRegistry.registry_path(), params
        )
        self._dataset_mgr = DatasetRegistryManager.load(
            params.base_path / DatasetRegistry.registry_path(), params, self._dimension_mgr
        )
        self._project_mgr = ProjectRegistryManager.load(
            params.base_path / ProjectRegistry.registry_path(),
            params,
            self._dataset_mgr,
            self._dimension_mgr,
            self._dimension_mapping_dimension_mgr,
        )

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
        if str(path).startswith("s3"):
            raise Exception(f"s3 is not currently supported: {path}")

        fs_interface = make_filesystem_interface(path)
        fs_interface.mkdir(path)
        fs_interface.mkdir(path / DatasetRegistry.registry_path())
        fs_interface.mkdir(path / ProjectRegistry.registry_path())
        fs_interface.mkdir(path / DimensionRegistry.registry_path())
        fs_interface.mkdir(path / DimensionMappingRegistry.registry_path())
        logger.info("Created registry at %s", path)
        cloud_interface = make_cloud_storage_interface(path, "", offline=True)
        params = RegistryManagerParams(
            Path(path), REMOTE_REGISTRY, fs_interface, cloud_interface, offline=True, dry_run=False
        )
        return cls(params)

    @property
    def dataset_manager(self):
        return self._dataset_mgr

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_dimension_mgr

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @property
    def project_manager(self):
        return self._project_mgr

    @classmethod
    def load(cls, path, offline_mode=False, dry_run_mode=False):
        """Loads a registry from the given path.

        Parameters
        ----------
        path : str
        offline_mode : bool
            Load registry in offline mode; default is False
        dry_run_mode : bool
            Test registry operations in dry-run "test" mode (i.e., do not commit changes to remote)

        Returns
        -------
        RegistryManager

        """
        # TODO S3
        if str(path).startswith("s3"):
            raise Exception(f"S3 is not yet supported as the base path: {path}")
        fs_interface = make_filesystem_interface(path)
        cloud_interface = make_cloud_storage_interface(path, REMOTE_REGISTRY, offline=offline_mode)

        if not offline_mode:
            logger.info("Sync from remote registry.")
            cloud_interface.sync_pull(REMOTE_REGISTRY, path)

        params = RegistryManagerParams(
            path, REMOTE_REGISTRY, fs_interface, cloud_interface, offline_mode, dry_run_mode
        )
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

        for dim_type in DimensionType:
            dir_name = path / DimensionRegistry.registry_path() / dim_type.value
            if not fs_interface.exists(str(dir_name)):
                fs_interface.mkdir(dir_name)

        logger.info(
            f"Loaded local registry at %s offline_mode=%s dry_run_mode=%s",
            path,
            offline_mode,
            dry_run_mode,
        )
        return cls(params)

    @property
    def path(self):
        return self._params.base_path

    # TODO: currently unused but may be needed for project/dataset updates
    # def _update_config(
    #    self, config_id, registry_config, config_file, submitter, update_type, log_message
    # ):
    #    # TODO: need to check that there are indeed changes to the config
    #    # TODO: if a new version is created but is deleted in .dsgrid-registry, version number should be reset
    #    #   accordingly, currently it does not.
    #    # desired feature: undo a revision

    #    if isinstance(registry_config, DatasetRegistryModel):
    #        registry_type = RegistryType.DATASET
    #    else:
    #        registry_type = RegistryType.PROJECT

    #    # This validates that all data.
    #    registry_class = get_registry_class(registry_type)
    #    registry_class.load(config_file)

    #    registry_config.description = load_data(config_file)[
    #        "description"
    #    ]  # always copy the latest from config

    #    if update_type == VersionUpdateType.MAJOR:
    #        registry_config.version = registry_config.version.bump_major()
    #    elif update_type == VersionUpdateType.MINOR:
    #        registry_config.version = registry_config.version.bump_minor()
    #    elif update_type == VersionUpdateType.PATCH:
    #        registry_config.version = registry_config.version.bump_patch()
    #    else:
    #        assert False

    #    registration = ConfigRegistrationModel(
    #        version=registry_config.version,
    #        submitter=submitter,
    #        date=datetime.now(),
    #        log_message=log_message,
    #    )
    #    registry_config.registration_history.append(registration)
    #    filename = self._get_registry_filename(registry_class, config_id)
    #    config_dir = self._get_project_directory(config_id)
    #    data_dir = config_dir / str(registry_config.version)
    #    self._fs_intf.mkdir(data_dir)

    #    if registry_type == RegistryType.DATASET:
    #        config_file_name = "dataset"
    #    elif registry_type == RegistryType.PROJECT:
    #        config_file_name = "project"
    #    config_file_name = config_file_name + os.path.splitext(config_file)[1]

    #    dump_data(serialize_model(registry_config), filename)
    #    self._fs_intf.copy_file(config_file, data_dir / config_file_name)
    #    dimensions_dir = Path(os.path.dirname(config_file)) / "dimensions"
    #    # copy new dimensions, to be removed with dimension id mapping
    #    self._fs_intf.copy_tree(dimensions_dir, data_dir / "dimensions")
    #    logger.info(
    #        "Updated %s %s with version=%s",
    #        registry_type.value,
    #        config_id,
    #        registry_config.version,
    #    )


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
