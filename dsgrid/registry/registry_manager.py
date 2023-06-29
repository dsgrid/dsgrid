"""Manages registration of all projects and datasets."""

import getpass
import logging
import os
import requests
import shutil
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

from dsgrid.common import (
    LOCAL_REGISTRY,
    REMOTE_REGISTRY,
    SYNC_EXCLUDE_LIST,
    on_hpc,
)
from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.config.mapping_tables import MappingTableConfig
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.exceptions import DSGValueNotRegistered, DSGInvalidParameter
from dsgrid.utils.run_command import check_run_command
from dsgrid.filesystem.factory import make_filesystem_interface
from dsgrid.utils.spark import init_spark
from .common import (
    RegistryManagerParams,
)
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry_manager import ProjectRegistryManager
from .registry_database import DatabaseConnection, RegistryDatabase
from .registry_interface import (
    DatasetRegistryInterface,
    DimensionMappingRegistryInterface,
    DimensionRegistryInterface,
    ProjectRegistryInterface,
)


logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages registration of all projects and datasets."""

    def __init__(self, params: RegistryManagerParams, db: RegistryDatabase):
        self._check_environment_variables(params)
        if SparkSession.getActiveSession() is None:
            init_spark("dsgrid")
        self._params = params
        self._dimension_mgr = DimensionRegistryManager.load(
            params.base_path,
            params,
            DimensionRegistryInterface(db),
        )
        self._dimension_mapping_mgr = DimensionMappingRegistryManager.load(
            params.base_path,
            params,
            self._dimension_mgr,
            DimensionMappingRegistryInterface(db),
        )
        self._dataset_mgr = DatasetRegistryManager.load(
            params.base_path,
            params,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            DatasetRegistryInterface(db),
        )
        self._project_mgr = ProjectRegistryManager.load(
            params.base_path,
            params,
            self._dataset_mgr,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            ProjectRegistryInterface(db),
        )

    @classmethod
    def create(
        cls, conn: DatabaseConnection, data_path: Path, remote_path=REMOTE_REGISTRY, user=None
    ):
        """Creates a new RegistryManager at the given path.

        Parameters
        ----------
        db_url : str
        data_path : Path
        remote_path : str
            Path to remote registry.
        use_remote_data_path : None, str
            Path to remote registry.

        Returns
        -------
        RegistryManager

        """
        if not user:
            user = getpass.getuser()
        uid = str(uuid.uuid4())

        if str(data_path).startswith("s3"):
            raise Exception(f"s3 is not currently supported: {data_path}")

        fs_interface = make_filesystem_interface(data_path)
        logger.info("Created registry at %s", data_path)
        cloud_interface = make_cloud_storage_interface(
            data_path, "", offline=True, uuid=uid, user=user
        )
        params = RegistryManagerParams(
            Path(data_path), remote_path, False, fs_interface, cloud_interface, offline=True
        )
        RegistryDatabase.delete(conn)
        db = RegistryDatabase.create(conn, data_path)
        return cls(params, db)

    @property
    def dataset_manager(self) -> DatasetRegistryManager:
        """Return the dataset manager."""
        return self._dataset_mgr

    @property
    def dimension_mapping_manager(self) -> DimensionMappingRegistryManager:
        """Return the dimension mapping manager."""
        return self._dimension_mapping_mgr

    @property
    def dimension_manager(self) -> DimensionRegistryManager:
        """Return the dimension manager."""
        return self._dimension_mgr

    @property
    def project_manager(self) -> ProjectRegistryManager:
        """Return the project manager."""
        return self._project_mgr

    @classmethod
    def load(
        cls,
        conn: DatabaseConnection,
        remote_path=REMOTE_REGISTRY,
        use_remote_data=None,
        offline_mode=False,
        user=None,
        no_prompts=False,
    ):
        """Loads a registry from the given path.

        Parameters
        ----------
        conn : DatabaseConnection
        remote_path: str, optional
            path of the remote registry; default is REMOTE_REGISTRY
        use_remote_data: bool, None
            If set, use load data tables from remote_path. If not set, auto-determine what to do
            based on HPC or AWS EMR environment variables.
        offline_mode : bool
            Load registry in offline mode; default is False
        user : str
            username
        no_prompts : bool
            If no_prompts is False, the user will be prompted to continue sync pulling the registry if lock files exist.

        Returns
        -------
        RegistryManager

        Examples
        --------
        >>> from dsgrid.registry.registry_manager import RegistryManager
        >>> from dsgrid.registry.registry_database import DatabaseConnection
        >>> manager = RegistryManager.load(
            DatabaseConnection(
                hostname="dsgrid-registry.hpc.nrel.gov",
                database="standard-scenarios",
            )
        )
        """
        db = RegistryDatabase.connect(conn)
        data_path = db.get_data_path()
        if not user:
            user = getpass.getuser()
        uid = str(uuid.uuid4())
        fs_interface = make_filesystem_interface(data_path)

        if use_remote_data is None:
            use_remote_data = _should_use_remote_data(remote_path)

        cloud_interface = make_cloud_storage_interface(
            data_path, remote_path, offline=offline_mode, uuid=uid, user=user
        )

        if not offline_mode:
            lock_files = list(cloud_interface.get_lock_files())
            if lock_files:
                msg = f"There are {len(lock_files)} lock files in the registry:"
                for lock_file in lock_files:
                    msg = msg + "\n\t" + f"- {lock_file}"
                logger.info(msg)
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
                    str(data_path) + "/configs",
                    exclude=SYNC_EXCLUDE_LIST,
                    delete_local=True,
                )

        params = RegistryManagerParams(
            data_path, remote_path, use_remote_data, fs_interface, cloud_interface, offline_mode
        )

        logger.info(
            "Loaded local registry at %s:%s offline_mode=%s",
            conn.hostname,
            conn.port,
            offline_mode,
        )
        return cls(params, db)

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
        if not project_id and not dataset_id:
            raise ValueError("Must provide a dataset_id or project_id for dsgrid data-sync.")

        if project_id:
            config = self.project_manager.get_by_id(project_id)
            if dataset_id:
                if dataset_id not in config.list_registered_dataset_ids():
                    raise DSGValueNotRegistered(
                        f"No registered dataset ID = '{dataset_id}' registered to project ID = '{project_id}'"
                    )
                datasets = [(dataset_id, str(config.get_dataset(dataset_id).version))]
            else:
                datasets = []
                for dataset in config.list_registered_dataset_ids():
                    datasets.append((dataset, str(config.get_dataset(dataset).version)))

        if dataset_id and not project_id:
            if not self.dataset_manager.has_id(dataset_id):
                raise DSGValueNotRegistered(f"No registered dataset ID = '{dataset_id}'")
            version = self.dataset_manager.get_latest_version(dataset_id)
            datasets = [(dataset_id, version)]

        for dataset, version in datasets:
            self._data_sync(dataset, version, no_prompts)

    def _data_sync(self, dataset_id, version, no_prompts=True):
        cloud_interface = self._params.cloud_interface
        offline_mode = self._params.offline

        if offline_mode:
            raise ValueError("dsgrid data-sync only works in online mode.")
        sync = True

        lock_files = list(
            cloud_interface.get_lock_files(
                relative_path=f"{cloud_interface._s3_filesystem._bucket}/configs/datasets/{dataset_id}"
            )
        )
        if lock_files:
            assert len(lock_files) == 1
            msg = f"There are {len(lock_files)} lock files in the registry:"
            for lock_file in lock_files:
                msg = msg + "\n\t" + f"- {lock_file}"
            logger.log(msg)
            if not no_prompts:
                msg = msg + "\n... Do you want to continue syncing the registry contents? [Y] >>> "
                val = input(msg)
                if val == "" or val.lower() == "y":
                    sync = True
                else:
                    logger.info("Skipping remote registry sync.")
                    sync = False

        if sync:
            logger.info("Sync data from remote registry for %s, version=%s.", dataset_id, version)
            cloud_interface.sync_pull(
                remote_path=self._params.remote_path + f"/data/{dataset_id}/{version}",
                local_path=str(self._params.base_path) + f"/data/{dataset_id}/{version}",
                delete_local=True,
            )
            cloud_interface.sync_pull(
                remote_path=self._params.remote_path + f"/data/{dataset_id}/registry.json5",
                local_path=str(self._params.base_path) + f"/data/{dataset_id}/registry.json5",
                delete_local=True,
                is_file=True,
            )
        else:
            logger.info(
                "Skipping remote registry data sync for %s, version=%s.", dataset_id, version
            )

    @property
    def path(self):
        return self._params.base_path

    def show(self, filters=None, max_width=None, drop_fields=None):
        """Show tables of all registry configs."""
        self.project_manager.show(filters=filters, max_width=max_width, drop_fields=drop_fields)
        self.dataset_manager.show(filters=filters, max_width=max_width, drop_fields=drop_fields)
        self.dimension_manager.show(filters=filters, max_width=max_width, drop_fields=drop_fields)
        self.dimension_mapping_manager.show(
            filters=filters, max_width=max_width, drop_fields=drop_fields
        )

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
            version = self.dimension_manager.get_latest_version(config.config_id)
            self._update_dimension_users(config, version, update_type, log_message)
        elif isinstance(config, MappingTableConfig):
            version = self.dimension_mapping_manager.get_latest_version(config.config_id)
            self._update_dimension_mapping_users(config, version, update_type, log_message)
        elif isinstance(config, DatasetConfig):
            version = self.dataset_manager.get_latest_version(config.config_id)
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
            ] = self.dimension_mapping_manager.get_latest_version(mapping.config_id)
            logger.info(
                "Updated dimension mapping %s as a result of dimension update", mapping.config_id
            )

        self._update_datasets_with_dimensions(updated_dimension_versions, updated_datasets)
        for dataset in updated_datasets.values():
            self.dataset_manager.update(dataset, update_type, log_message)
            updated_dataset_versions[dataset.config_id] = self.dataset_manager.get_latest_version(
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
                mapping.model.from_dimension.version = str(
                    updated_dimensions[mapping.model.from_dimension.dimension_id]
                )
                updated = True
            elif mapping.model.to_dimension.dimension_id in updated_dimensions:
                mapping.model.to_dimension.version = str(
                    updated_dimensions[mapping.model.to_dimension.dimension_id]
                )
                updated = True
            if updated and mapping.config_id not in updated_mappings:
                updated_mappings[mapping.config_id] = mapping

    def _update_datasets_with_dimensions(self, updated_dimensions, updated_datasets):
        if not updated_dimensions:
            return
        for dataset in self.dataset_manager.iter_configs():
            updated = False
            for dimension_ref in dataset.model.dimension_references:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = str(updated_dimensions[dimension_ref.dimension_id])
                    updated = True
            if updated and dataset.config_id not in updated_datasets:
                updated_datasets[dataset.config_id] = dataset

    def _update_projects_with_dimensions(self, updated_dimensions, updated_projects):
        if not updated_dimensions:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for dimension_ref in project.model.dimensions.base_dimension_references:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = str(updated_dimensions[dimension_ref.dimension_id])
                    updated = True
            for dimension_ref in project.model.dimensions.supplemental_dimension_references:
                if dimension_ref.dimension_id in updated_dimensions:
                    dimension_ref.version = str(updated_dimensions[dimension_ref.dimension_id])
                    updated = True
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project

    def _update_projects_with_dimension_mappings(self, updated_mappings, updated_projects):
        if not updated_mappings:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for mapping_ref in project.model.dimension_mappings.base_to_supplemental_references:
                if mapping_ref.mapping_id in updated_mappings:
                    mapping_ref.version = str(updated_mappings[mapping_ref.mapping_id])
                    updated = True
            for mapping_list in project.model.dimension_mappings.dataset_to_project.values():
                for mapping_ref in mapping_list:
                    if mapping_ref.mapping_id in updated_mappings:
                        mapping_ref.version = str(updated_mappings[mapping_ref.mapping_id])
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project

    def _update_projects_with_datasets(self, updated_datasets, updated_projects):
        if not updated_datasets:
            return
        for project in self.project_manager.iter_configs():
            updated = False
            for dataset in project.model.datasets:
                # TODO #191: does dataset status matter? update unregistered?
                if dataset.dataset_id in updated_datasets:
                    dataset.version = str(updated_datasets[dataset.dataset_id])
                    updated = True
            if updated and project.config_id not in updated_projects:
                updated_projects[project.config_id] = project

    @staticmethod
    def copy(
        src: DatabaseConnection, dst: DatabaseConnection, dst_data_path, mode="copy", force=False
    ):
        """Copy a registry to a new path.

        Parameters
        ----------
        src : DatabaseConnection
        dst : DatabaseConnection
        dst_data_path : Path
        simple_model : RegistrySimpleModel
            Filter all configs and data according to this model.
        mode : str
            Controls whether to copy all data, make symlinks to data files, or sync data with the
            rsync utility (not available on Windows). Options: 'copy', 'data-symlinks', 'rsync'
        force : bool
            Overwrite dst_data_path if it already exists. Does not apply if using rsync.

        Raises
        ------
        DSGInvalidParameter
            Raised if src is not a valid registry.
            Raised if dst_data_path exists, use_rsync is False, and force is False.

        """
        src_db = RegistryDatabase.connect(src)
        src_data_path = src_db.get_data_path()
        if not {x.name for x in src_data_path.iterdir()}.issuperset({"data"}):
            raise DSGInvalidParameter(f"{src_data_path} is not a valid registry")

        if mode in ("copy", "data-symlinks"):
            if dst_data_path.exists():
                if force:
                    shutil.rmtree(dst_data_path)
                else:
                    raise DSGInvalidParameter(f"{dst_data_path} already exists.")
        RegistryDatabase.copy(src, dst, dst_data_path)
        if mode == "rsync":
            cmd = f"rsync -a {src_data_path}/ {dst_data_path}"
            logger.info("rsync data with [%s]", cmd)
            check_run_command(cmd)
        elif mode in ("copy", "data-symlinks"):
            logger.info("Copy data from source registry %s", src_data_path)
            if mode == "data-symlinks":
                _make_data_symlinks(src_data_path, dst_data_path)
            else:
                shutil.copytree(src_data_path / "data", dst_data_path / "data", symlinks=True)
        else:
            raise DSGInvalidParameter(f"mode={mode} is not supported")

    @staticmethod
    def _check_environment_variables(params):
        if not params.offline:
            illegal_vars = [x for x in os.environ if x.startswith("__DSGRID_SKIP_CHECK")]
            if illegal_vars:
                raise Exception(
                    f"Internal environment variables to skip checks are not allowed to be set "
                    f"in online mode: {illegal_vars}"
                )


def _make_data_symlinks(src, dst):
    # registry/data/dataset_id/registry.json5
    # registry/data/dataset_id/version/*.parquet
    for dataset_id in (src / "data").iterdir():
        if dataset_id.is_dir():
            (dst / "data" / dataset_id.name).mkdir(parents=True)
        for path in (src / "data" / dataset_id.name).iterdir():
            if path.is_dir():
                (dst / "data" / dataset_id.name / path.name).mkdir()
                for data_file in path.iterdir():
                    os.symlink(
                        data_file.absolute(),
                        dst / "data" / dataset_id.name / path.name / data_file.name,
                        target_is_directory=data_file.is_dir(),
                    )
            elif path.is_file():
                shutil.copyfile(path, dst / "data" / dataset_id.name / path.name)


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


def _should_use_remote_data(remote_path):
    if not str(remote_path).lower().startswith("s3"):
        # We are on a local filesystem. Use the remote path.
        return True

    use_remote_data = False
    if "DSGRID_USE_LOCAL_DATA" in os.environ:
        pass
    elif sys.platform in ("darwin", "win32"):
        # Local systems need to sync all load data files.
        pass
    elif on_hpc():
        pass
    elif "GITHUB_ACTION" in os.environ:
        logger.info("Do not use remote data on GitHub CI")
    else:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html
        try:
            response = requests.get(
                "http://169.254.169.254/latest/dynamic/instance-identity/document", timeout=2
            )
            ret = 0
        except requests.ConnectTimeout:
            logger.warning(
                "Connection timed out while trying to read AWS identity. "
                "If you are not running on AWS and would prefer to not experience this delay, set "
                "the environment varible DSGRID_USE_LOCAL_DATA."
            )
            ret = 1
        except Exception:
            logger.exception("Failed to read identity document")
            ret = 1

        if ret == 0 and response.status_code == 200:
            identity_data = response.json()
            logger.info("Identity data: %s", identity_data)
            if "instanceId" in identity_data:
                logger.info("Use remote data on AWS")
                use_remote_data = True
            else:
                logger.warning("Unknown payload from identity request.")

    return use_remote_data
