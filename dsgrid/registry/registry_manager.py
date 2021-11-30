"""Manages registration of all projects and datasets."""

import getpass
import logging
import os
from pathlib import Path
import uuid

from pyspark.sql import SparkSession

from dsgrid.common import (
    LOCAL_REGISTRY,
    REMOTE_REGISTRY,
    SYNC_EXCLUDE_LIST,
)
from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.config.association_tables import AssociationTableConfig
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.filesystem.factory import make_filesystem_interface
from dsgrid.utils.spark import init_spark
from .common import (
    RegistryType,
    RegistryManagerParams,
)
from .dimension_mapping_registry import DimensionMappingRegistry
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dataset_registry import DatasetRegistry
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
        if SparkSession.getActiveSession() is None:
            init_spark("registry")
        self._params = params
        self._dimension_mgr = DimensionRegistryManager.load(
            params.base_path / DimensionRegistry.registry_path(), params
        )
        self._dimension_mapping_dimension_mgr = DimensionMappingRegistryManager.load(
            params.base_path / DimensionMappingRegistry.registry_path(),
            params,
            self._dimension_mgr,
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
    def create(cls, path, remote_path=REMOTE_REGISTRY, user=None):
        """Creates a new RegistryManager at the given path.

        Parameters
        ----------
        path : str

        Returns
        -------
        RegistryManager

        """
        if not user:
            user = getpass.getuser()
        uid = str(uuid.uuid4())

        if str(path).startswith("s3"):
            raise Exception(f"s3 is not currently supported: {path}")

        fs_interface = make_filesystem_interface(path)
        fs_interface.mkdir(path)
        fs_interface.mkdir(path / "configs")
        fs_interface.mkdir(path / "data")
        fs_interface.mkdir(path / DatasetRegistry.registry_path())
        fs_interface.mkdir(path / ProjectRegistry.registry_path())
        fs_interface.mkdir(path / DimensionRegistry.registry_path())
        fs_interface.mkdir(path / DimensionMappingRegistry.registry_path())
        logger.info("Created registry at %s", path)
        cloud_interface = make_cloud_storage_interface(path, "", offline=True, uuid=uid, user=user)
        params = RegistryManagerParams(
            Path(path), remote_path, fs_interface, cloud_interface, offline=True, dry_run=False
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
    def load(
        cls,
        path,
        remote_path=REMOTE_REGISTRY,
        offline_mode=False,
        dry_run_mode=False,
        user=None,
        no_prompts=False,
    ):
        """Loads a registry from the given path.

        Parameters
        ----------
        path : str
            base path of the local or base registry
        remote_path: str, optional
            path of the remote registry; default is REMOTE_REGISTRY
        offline_mode : bool
            Load registry in offline mode; default is False
        dry_run_mode : bool
            Test registry operations in dry-run "test" mode (i.e., do not commit changes to remote)
        user : str
            username
        no_prompts : bool
            If no_prompts is False, the user will be prompted to continue sync pulling the registry if lock files exist.

        Returns
        -------
        RegistryManager

        """
        if not user:
            user = getpass.getuser()
        uid = str(uuid.uuid4())
        # TODO S3
        if str(path).startswith("s3"):
            raise Exception(f"S3 is not yet supported as the base path: {path}")
        fs_interface = make_filesystem_interface(path)
        cloud_interface = make_cloud_storage_interface(
            path, remote_path, offline=offline_mode, uuid=uid, user=user
        )

        if not offline_mode:
            lock_files = list(cloud_interface.get_lock_files())
            if lock_files:
                msg = f"There are {len(lock_files)} lock files in the registry:"
                for lock_file in lock_files:
                    msg = msg + "\n\t" + f"- {lock_file}"
                logger.log(msg)
                if not no_prompts:
                    msg = (
                        msg
                        + "\n... Do you want to continue syncing the registry contents? [Y] >>> "
                    )
                    val = input(msg)
                    if val == "" or val.lower() == "y":
                        sync = True
                    else:
                        logger.info("Skipping remote registry sync.")
            else:
                sync = True

            if sync:
                logger.info("Sync configs from remote registry.")
                # NOTE: When creating a registry, only the /configs are pulled. To sync_pull /data, use the dsgrid registry data-sync CLI command.
                cloud_interface.sync_pull(
                    remote_path + "/configs",
                    str(path) + "/configs",
                    exclude=SYNC_EXCLUDE_LIST,
                    delete_local=True,
                )

        params = RegistryManagerParams(
            path, remote_path, fs_interface, cloud_interface, offline_mode, dry_run_mode
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
                fs_interface.mkdir(dir_name)

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

    def data_sync(self, project_id, dataset_id, no_prompts=True):
        """Sync data from the remote dsgrid registry.

        Parameters
        ----------
        project_id : str
            Sync by project_id filter
        dataset_id : str
            Sync by dataset_id filter
        no_prompts :  bool
            If no_prompts is False, the user will be prompted to continue sync pulling the registry if lock files exist. By default, True.
        """
        fs_interface = self._params.fs_interface
        cloud_interface = self._params.cloud_interface
        offline_mode = self._params.offline
        dry_run_mode = self._params.dry_run

        if not offline_mode:
            if project_id:
                if self.project_manager.has_id(project_id):
                    project_version = self.project_manager.get_current_version(project_id)
                    config_file = self.project_manager.get_config_file(project_id, project_version)
                    config = ProjectConfig.load(
                        config_file, self.dimension_manager, self.dimension_mapping_manager
                    )
                    if dataset_id:
                        if dataset_id in config.list_registered_dataset_ids():
                            datasets = [(dataset_id, str(config.get_dataset(dataset_id).version))]
                        else:
                            raise ValueError(
                                f"No registered dataset ID = '{dataset_id}' registered to project ID = '{project_id}'"
                            )
                    else:
                        datasets = []
                        for dataset in config.list_registered_dataset_ids():
                            datasets.append((dataset, str(config.get_dataset(dataset).version)))
                else:
                    raise ValueError(f"No registered project ID = '{project_id}'")

            if dataset_id and not project_id:
                if self.dataset_manager.has_id(dataset_id):
                    version = self.dataset_manager.get_current_version(dataset_id)
                    datasets = [(dataset_id, version)]
                else:
                    raise ValueError(f"No registered dataset ID = '{dataset_id}'")

            lock_files = list(cloud_interface.get_lock_files())
            if lock_files:
                msg = f"There are {len(lock_files)} lock files in the registry:"
                for lock_file in lock_files:
                    msg = msg + "\n\t" + f"- {lock_file}"
                logger.log(msg)
                if not no_prompts:
                    msg = (
                        msg
                        + "\n... Do you want to continue syncing the registry contents? [Y] >>> "
                    )
                    val = input(msg)
                    if val == "" or val.lower() == "y":
                        sync = True
                    else:
                        logger.info("Skipping remote registry sync.")

            for dataset, version in datasets:
                if dry_run_mode == True:
                    sync = False
                else:
                    sync = True
                if sync:
                    logger.info(
                        f"Sync data from remote registry for {dataset}, version={version}."
                    )
                    cloud_interface.sync_pull(
                        remote_path=self._params.remote_path + f"/data/{dataset}/{version}",
                        local_path=str(self._params.base_path) + f"/data/{dataset}/{version}",
                        exclude=SYNC_EXCLUDE_LIST,
                        delete_local=True,
                    )
                    cloud_interface.sync_pull(
                        remote_path=self._params.remote_path + f"/data/{dataset}/registry.toml",
                        local_path=str(self._params.base_path) + f"/data/{dataset}/registry.toml",
                        exclude=SYNC_EXCLUDE_LIST,
                        delete_local=True,
                        is_file=True,
                    )
                else:
                    logger.info(
                        f"Skipping remote registry data sync for {dataset}, version={version}."
                    )

    @property
    def path(self):
        return self._params.base_path

    def show(self):
        """Show tables of all registry configs."""
        self.project_manager.show()
        self.dataset_manager.show()
        self.dimension_manager.show()
        self.dimension_mapping_manager.show()

    def update_dependent_configs(self, config, update_type, log_message):
        """Update all configs that consume this config. Recursive.
        This is an experimental feature and is subject to change.
        Should only be called an admin that understands the consequences.
        Passing a dimension may trigger an update to a project and a dimension mapping.
        The change to that dimension mapping may trigger another update to the project.
        This guarantees that each config version will only be bumped once.

        It is up to the caller to ensure changes are synced to the remote registry.

        Parameters
        ----------
        config : ConfigBase
        update_type : VersionUpdateType
        log_message : str

        """
        if isinstance(config, DimensionConfig):
            version = self.dimension_manager.get_current_version(config.config_id)
            self._update_dimension_users(config, version, update_type, log_message)
        elif isinstance(config, AssociationTableConfig):
            version = self.dimension_mapping_manager.get_current_version(config.config_id)
            self._update_dimension_mapping_users(config, version, update_type, log_message)
        elif isinstance(config, DatasetConfig):
            version = self.dataset_manager.get_current_version(config.config_id)
            self._update_dataset_users(config, version, update_type, log_message)
        else:
            assert False, type(config)

    def _update_dimension_users(self, config: DimensionConfig, version, update_type, log_message):
        # Order is important because
        # - dimension mappings may have this dimension.
        # - datasets may have this dimension.
        # - projects may have this dimension as well as updated mappings and datasets.
        updated_dimension_versions = {config.config_id: version}
        updated_mappings = {}
        updated_mapping_versions = {}
        updated_datasets = {}
        updated_dataset_versions = {}
        updated_projects = {}

        self._update_dimension_mappings_with_dimensions(
            updated_dimension_versions, updated_mappings
        )
        for mapping in updated_mappings.values():
            self.dimension_mapping_manager.update(mapping, update_type, log_message)
            updated_mapping_versions[
                mapping.config_id
            ] = self.dimension_mapping_manager.get_current_version(mapping.config_id)
            logger.info(
                "Updated dimension mapping %s as a result of dimension update", mapping.config_id
            )

        self._update_datasets_with_dimensions(updated_dimension_versions, updated_datasets)
        for dataset in updated_datasets.values():
            self.dataset_manager.update(dataset, update_type, log_message)
            updated_dataset_versions[dataset.config_id] = self.dataset_manager.get_current_version(
                dataset.config_id
            )
            logger.info("Updated dataset %s as a result of dimension update", dataset.config_id)

        self._update_projects_with_dimensions(updated_dimension_versions, updated_projects)
        self._update_projects_with_dimension_mappings(updated_mapping_versions, updated_projects)
        self._update_projects_with_datasets(updated_dataset_versions, updated_projects)
        for project in updated_projects.values():
            self.project_manager.update(project, update_type, log_message)
            logger.info("Updated project %s as a result of dimension update", project.config_id)

    def _update_dimension_mapping_users(
        self, config: DimensionMappingBaseModel, version, update_type, log_message
    ):
        updated_mapping_versions = {config.config_id: version}
        updated_projects = {}
        self._update_projects_with_dimension_mappings(updated_mapping_versions, updated_projects)
        for project in updated_projects.values():
            self.project_manager.update(project, update_type, log_message)
            logger.info(
                "Updated project %s as a result of dimension mapping update", project.config_id
            )

    def _update_dataset_users(self, config: DatasetConfig, version, update_type, log_message):
        updated_dataset_versions = {config.config_id: version}
        updated_projects = {}
        self._update_projects_with_datasets(updated_dataset_versions, updated_projects)
        for project in updated_projects.values():
            self.project_manager.update(project, update_type, log_message)
            logger.info("Updated project %s as a result of dataset update", project.config_id)

    def _update_dimension_mappings_with_dimensions(self, updated_dimensions, updated_mappings):
        if not updated_dimensions:
            return
        for mapping in self.dimension_mapping_manager.iter_configs():
            updated = False
            if mapping.model.from_dimension.dimension_id in updated_dimensions:
                mapping.model.from_dimension.version = updated_dimensions[
                    mapping.model.from_dimension.dimension_id
                ]
                updated = True
            elif mapping.model.to_dimension.dimension_id in updated_dimensions:
                mapping.model.to_dimension.version = updated_dimensions[
                    mapping.model.to_dimension.dimension_id
                ]
                updated = True
            if updated and mapping.config_id not in updated_mappings:
                updated_mappings[mapping.config_id] = mapping

    def _update_datasets_with_dimensions(self, updated_dimensions, updated_datasets):
        if not updated_dimensions:
            return
        for dataset in self.dataset_manager.iter_configs():
            updated = False
            for dimension_ref in dataset.model.dimensions:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = updated_dimensions[dimension_ref.dimension_id]
                    updated = True
            if updated and dataset.config_id not in updated_datasets:
                updated_datasets[dataset.config_id] = dataset

    def _update_projects_with_dimensions(self, updated_dimensions, updated_projects):
        if not updated_dimensions:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for dimension_ref in project.model.dimensions.base_dimensions:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = updated_dimensions[dimension_ref.dimension_id]
                    updated = True
            for dimension_ref in project.model.dimensions.supplemental_dimensions:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = updated_dimensions[dimension_ref.dimension_id]
                    updated = True
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project

    def _update_projects_with_dimension_mappings(self, updated_mappings, updated_projects):
        if not updated_mappings:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for mapping_ref in project.model.dimension_mappings.base_to_base:
                if mapping_ref.mapping_id in updated_mappings:
                    mapping_ref.version = updated_mappings[mapping_ref.mapping_id]
                    updated = True
            for mapping_ref in project.model.dimension_mappings.base_to_supplemental:
                if mapping_ref.mapping_id in updated_mappings:
                    mapping_ref.version = updated_mappings[mapping_ref.mapping_id]
                    updated = True
            for mapping_list in project.model.dimension_mappings.dataset_to_project.values():
                for mapping_ref in mapping_list:
                    if mapping_ref.mapping_id in updated_mappings:
                        mapping_ref.version = updated_mappings[mapping_ref.mapping_id]
                        updated_project = True
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project

    def _update_projects_with_datasets(self, updated_datasets, updated_projects):
        if not updated_datasets:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for dataset in project.model.datasets:
                # TODO: does dataset status matter? update unregistered?
                if dataset.dataset_id in updated_datasets:
                    dataset.version = updated_datasets[dataset.dataset_id]
                    updated = True
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project


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
            "run the following command:\n"
            "  dsgrid registry create $DSGRID_REGISTRY_PATH\n"
            "Then register dimensions, dimension mappings, projects, and datasets."
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
