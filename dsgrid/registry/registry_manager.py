"""Manages registration of all projects and datasets."""

import getpass
import logging
import os
import requests
import shutil
import sys
import uuid
from pathlib import Path

from sqlalchemy import Connection

from dsgrid.common import (
    LOCAL_REGISTRY,
    REMOTE_REGISTRY,
    SYNC_EXCLUDE_LIST,
    on_hpc,
)
from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.exceptions import DSGInvalidOperation, DSGValueNotRegistered, DSGInvalidParameter
from dsgrid.utils.run_command import check_run_command
from dsgrid.filesystem.factory import make_filesystem_interface
from dsgrid.utils.spark import init_spark, get_active_session
from .common import (
    DataStoreType,
    RegistryManagerParams,
)
from dsgrid.registry.registry_database import RegistryDatabase
from dsgrid.registry.registry_interface import (
    DatasetRegistryInterface,
    DimensionMappingRegistryInterface,
    DimensionRegistryInterface,
    ProjectRegistryInterface,
)
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry_manager import ProjectRegistryManager
from .registry_database import DatabaseConnection


logger = logging.getLogger(__name__)


class RegistryManager:
    """Manages registration of all projects and datasets."""

    def __init__(self, params: RegistryManagerParams, db: RegistryDatabase):
        self._db = db
        self._data_store = db.data_store
        self._check_environment_variables(params)
        if get_active_session() is None:
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
            self._data_store,
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
        cls,
        conn: DatabaseConnection,
        data_path: Path,
        data_store_type: DataStoreType = DataStoreType.FILESYSTEM,
        remote_path=REMOTE_REGISTRY,
        user=None,
        scratch_dir=None,
        overwrite=False,
    ):
        """Creates a new RegistryManager at the given path.

        Parameters
        ----------
        db_url : str
        data_path : Path
        data_store_type : DataStoreType
        remote_path : str
            Path to remote registry.
        use_remote_data_path : None, str
            Path to remote registry.
        scratch_dir : None | Path
            Base directory for dsgrid temporary directories. Must be accessible on all compute
            nodes. Defaults to the current directory.
        overwrite: bool
            Overwrite the database if it exists.

        Returns
        -------
        RegistryManager

        """
        if RegistryDatabase.has_database(conn) and not overwrite:
            msg = f"database={conn.url} already exists. Choose a different name or set overwrite=True."
            raise DSGInvalidOperation(msg)

        db_filename = conn.try_get_filename()
        if db_filename is not None and db_filename.is_relative_to(data_path):
            msg = (
                f"The database path {db_filename} cannot be relative to the data_path {data_path}."
            )
            raise DSGInvalidOperation(msg)

        if not user:
            user = getpass.getuser()
        uid = str(uuid.uuid4())

        if str(data_path).startswith("s3"):
            msg = f"s3 is not currently supported: {data_path}"
            raise Exception(msg)

        fs_interface = make_filesystem_interface(data_path)
        logger.info("Created registry with database=%s data_path=%s", conn.url, data_path)
        cloud_interface = make_cloud_storage_interface(
            data_path, "", offline=True, uuid=uid, user=user
        )
        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        params = RegistryManagerParams(
            base_path=Path(data_path),
            remote_path=remote_path,
            use_remote_data=False,
            fs_interface=fs_interface,
            cloud_interface=cloud_interface,
            offline=True,
            scratch_dir=scratch_dir,
        )
        RegistryDatabase.delete(conn)
        db = RegistryDatabase.create(
            conn, data_path, data_store_type=data_store_type, overwrite=overwrite
        )
        return cls(params, db)

    def dispose(self) -> None:
        """Dispose the database engine and release all connections."""
        self._db.dispose()

    def __enter__(self) -> "RegistryManager":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and dispose resources."""
        self.dispose()

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
        offline_mode=True,
        user=None,
        no_prompts=False,
        scratch_dir=None,
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
            Load registry in offline mode; default is True
        user : str
            username
        no_prompts : bool
            If no_prompts is False, the user will be prompted to continue sync pulling the registry if lock files exist.
        scratch_dir : None | Path
            Base directory for dsgrid temporary directories. Must be accessible on all compute
            nodes. Defaults to the current directory.

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

        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        params = RegistryManagerParams(
            base_path=data_path,
            remote_path=remote_path,
            use_remote_data=use_remote_data,
            fs_interface=fs_interface,
            cloud_interface=cloud_interface,
            offline=offline_mode,
            scratch_dir=scratch_dir,
        )

        logger.info(
            "Loaded local registry at %s offline_mode=%s",
            conn.url,
            # conn.port,
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
            msg = "Must provide a dataset_id or project_id for dsgrid data-sync."
            raise ValueError(msg)

        if project_id:
            config = self.project_manager.get_by_id(project_id)
            if dataset_id:
                if dataset_id not in config.list_registered_dataset_ids():
                    msg = f"No registered dataset ID = '{dataset_id}' registered to project ID = '{project_id}'"
                    raise DSGValueNotRegistered(msg)
                datasets = [(dataset_id, str(config.get_dataset(dataset_id).version))]
            else:
                datasets = []
                for dataset in config.list_registered_dataset_ids():
                    datasets.append((dataset, str(config.get_dataset(dataset).version)))

        if dataset_id and not project_id:
            if not self.dataset_manager.has_id(dataset_id):
                msg = f"No registered dataset ID = '{dataset_id}'"
                raise DSGValueNotRegistered(msg)
            version = self.dataset_manager.get_latest_version(dataset_id)
            datasets = [(dataset_id, version)]

        for dataset, version in datasets:
            self._data_sync(dataset, version, no_prompts)

    def _data_sync(self, dataset_id, version, no_prompts=True):
        cloud_interface = self._params.cloud_interface
        offline_mode = self._params.offline

        if offline_mode:
            msg = "dsgrid data-sync only works in online mode."
            raise ValueError(msg)
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
            logger.info(msg)
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

    def show(self, conn: Connection | None = None, filters=None, max_width=None, drop_fields=None):
        """Show tables of all registry configs."""
        self.project_manager.show(
            conn=conn, filters=filters, max_width=max_width, drop_fields=drop_fields
        )
        self.dataset_manager.show(
            conn=conn, filters=filters, max_width=max_width, drop_fields=drop_fields
        )
        self.dimension_manager.show(
            conn=conn, filters=filters, max_width=max_width, drop_fields=drop_fields
        )
        self.dimension_mapping_manager.show(
            conn=conn, filters=filters, max_width=max_width, drop_fields=drop_fields
        )

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
        src_db.engine.dispose()
        # TODO: This does not support the duckdb data store. Need to implement this copy operation
        # in the DataStoreInterface.
        if not {x.name for x in src_data_path.iterdir()}.issuperset({"data"}):
            msg = f"{src_data_path} is not a valid registry"
            raise DSGInvalidParameter(msg)

        if mode in ("copy", "data-symlinks"):
            if dst_data_path.exists():
                if force:
                    shutil.rmtree(dst_data_path)
                else:
                    msg = f"{dst_data_path} already exists."
                    raise DSGInvalidParameter(msg)
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
                for path in (src_data_path / "data").iterdir():
                    dst_path = dst_data_path / "data" / path.name
                    shutil.copytree(path, dst_path, symlinks=True)
        else:
            msg = f"mode={mode} is not supported"
            raise DSGInvalidParameter(msg)

    @staticmethod
    def _check_environment_variables(params):
        if not params.offline:
            illegal_vars = [x for x in os.environ if x.startswith("__DSGRID_SKIP_CHECK")]
            if illegal_vars:
                msg = (
                    f"Internal environment variables to skip checks are not allowed to be set "
                    f"in online mode: {illegal_vars}"
                )
                raise Exception(msg)


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
        msg = (
            f"Registry path {registry_path} does not exist. To create the registry, "
            "run the following command:\n"
            "  dsgrid registry create $DSGRID_REGISTRY_PATH\n"
            "Then register dimensions, dimension mappings, projects, and datasets."
        )
        raise ValueError(msg)
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
