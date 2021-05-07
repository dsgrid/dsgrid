"""Base class for all registry managers."""

import abc
from contextlib import contextmanager
import logging
import os.path
from pathlib import Path

from prettytable import PrettyTable

from .common import RegistryManagerParams
from .registry_base import RegistryBaseModel
from dsgrid.common import REGISTRY_FILENAME, SYNC_EXCLUDE_LIST
from dsgrid.exceptions import (
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
    DSGInvalidOperation,
)


logger = logging.getLogger(__name__)


class RegistryManagerBase(abc.ABC):
    """Base class for all registry managers."""

    def __init__(self, path, params: RegistryManagerParams):
        self._path = path
        self._params = params
        self._registry_configs = {}  # ID to registry config

    def inventory(self):
        for config_id in self.fs_interface.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            registry = self.registry_class().load(self.get_registry_file(config_id))
            self._registry_configs[config_id] = registry

    @classmethod
    def load(cls, path, params, *args, **kwargs):
        """Load the registry manager.

        path : str
        params : RegistryManagerParams

        Returns
        -------
        RegistryManagerBase

        """
        mgr = cls(path, params)
        mgr.inventory()
        return mgr

    @classmethod
    def _load(cls, path, fs_interface):
        mgr = cls(path, fs_interface)
        mgr.inventory()
        return mgr

    @abc.abstractmethod
    def get_by_key(self, key):
        """Get the item matching key. Returns from cache if already loaded.

        Parameters
        ----------
        key : ConfigKey Key

        Returns
        -------
        DSGBaseModel

        Raises
        ------
        DSGValueNotRegistered
            Raised if the ID is not stored.

        """

    @abc.abstractmethod
    def get_by_id(self, config_id, version=None):
        """Get the item matching matching ID. Returns from cache if already loaded.

        Parameters
        ----------
        config_id : str
        version : VersionInfo
            If None, return the latest version.

        Returns
        -------
        DSGBaseModel

        Raises
        ------
        DSGValueNotRegistered
            Raised if the ID is not stored.

        """

    @staticmethod
    @abc.abstractmethod
    def name():
        """Return the name of the registry, used for reporting.

        Returns
        -------
        str

        """

    @abc.abstractmethod
    def register(self, config_file, submitter, log_message, force=False):
        """Registers a config file in the registry.

        Parameters
        ----------
        config_file : str
            Path to  config file
        submitter : str
            Submitter name
        log_message : str
        force : bool
            If true, register even if an ID is duplicate.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the config ID is already registered.

        """
        if self.offline_mode or self.dry_run_mode:
            self._register(config_file, submitter, log_message, force=force)
        else:
            lock_file_path = self.get_registry_lockfile(load_data(config_file)["project_id"])
            with self.cloud_interface.make_lock(lock_file_path):
                self._register(config_file, submitter, log_message, force=force)

    @staticmethod
    @abc.abstractmethod
    def registry_class():
        """Return the class used for the registry item."""

    @abc.abstractmethod
    def update(self, config_file, submitter, update_type, log_message):
        """Updates an existing registry with new parameters or data.

        Parameters
        ----------
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

    def _check_if_already_registered(self, config_id):
        if config_id in self._registry_configs:
            raise DSGDuplicateValueRegistered(f"{self.name()}={config_id}")

    def _check_if_not_registered(self, config_id):
        if config_id not in self._registry_configs:
            raise DSGValueNotRegistered(f"{self.name()}={config_id}")

    def _log_dry_run_mode_prefix(self):
        return "* DRY RUN MODE * |" if self.dry_run_mode else ""

    def _log_offline_mode_prefix(self):
        return "* OFFLINE MODE * |" if self.offline_mode else ""

    def _update_registry_cache(self, config_id, registry_model: RegistryBaseModel):
        assert config_id not in self._registry_configs, config_id
        self._registry_configs[config_id] = self.registry_class()(registry_model)

    def _raise_if_dry_run(self, operation):
        if self.dry_run_mode:
            raise DSGInvalidOperation(f"operation={operation} is not allowed in dry-run mode")

    @property
    def cloud_interface(self):
        """Return the CloudStorageInterface to sync remote data."""
        return self._params.cloud_interface

    @cloud_interface.setter
    def cloud_interface(self, cloud_interface):
        """Set the CloudStorageInterface (used in testing)"""
        self._params = self._params._replace(cloud_interface=cloud_interface)

    @property
    def fs_interface(self):
        """Return the FilesystemInterface to list directories and read/write files."""
        return self._params.fs_interface

    @property
    def offline_mode(self):
        """Return True if there is to be no syncing with the remote registry."""
        return self._params.offline

    @property
    def dry_run_mode(self):
        """Return True if no registry changes are to be made. Checking only."""
        return self._params.dry_run

    def get_config_directory(self, config_id, version):
        """Return the path to the config file.

        Parameters
        ----------
        config_id : str
        version : VersionInfo

        Returns
        -------
        str

        """
        return self.get_registry_directory(config_id) / str(version)

    def get_config_file(self, config_id, version):
        """Return the path to the config file.

        Parameters
        ----------
        config_id : str
        version : VersionInfo

        Returns
        -------
        str

        """
        return (
            self.get_config_directory(config_id, version) / self.registry_class().config_filename()
        )

    def get_current_version(self, config_id):
        """Return the current version in the registry.

        Returns
        -------
        VersionInfo

        """
        return self._registry_configs[config_id].model.version

    def get_registry_config(self, config_id):
        """Return the registry config.

        Parameters
        ----------
        config_id : str

        Returns
        -------
        RegistryBase

        """
        if config_id not in self._registry_configs:
            raise DSGValueNotRegistered(f"{self.name()}={config_id}")
        return self._registry_configs[config_id]

    def get_registry_lockfile(self, config_id):
        """Return registry lock file path.

        Parameters
        ----------
        config_id : str
            Config ID

        Returns
        -------
        str
            Lock file path
        """
        if self.__class__.name() == "Projects":
            return f"configs/.locks/{config_id}.lock"
        elif self.__class__.name() == "Datasets":
            return f"configs/.locks/{config_id}.lock"
        elif self.__class__.name() == "Dimensions":
            return "configs/.locks/dimensions.lock"
        elif self.__class__.name() == "Dimension Mappings":
            return "configs/.locks/dimension_mappings.lock"

    def get_registry_directory(self, config_id):
        """Return the directory containing data for config_id (registry.toml and versions).

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return self._path / config_id

    def get_registry_file(self, config_id):
        """Return the path to the registry file.

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return self.get_registry_directory(config_id) / REGISTRY_FILENAME

    def has_id(self, config_id, version=None):
        """Return True if an item matching the parameters is stored.

        Parameters
        ----------
        config_id : str
        version : VersionInfo
            If None, use latest.

        Returns
        -------
        bool

        """
        if version is None:
            return config_id in self._registry_configs
        return self.fs_interface.exists(self.get_config_directory(config_id, version))

    def iter_ids(self):
        """Return an iterator over the registered IDs."""
        return self._registry_configs.keys()

    def list_ids(self, **kwargs):
        """Return the IDs.

        Returns
        -------
        list

        """
        return sorted(self.iter_ids())

    def relative_remote_path(self, path):
        """Return relative remote registry path."""
        relative_path = Path(path).relative_to(self._params.base_path)
        remote_path = f"{self._params.remote_path}/{relative_path}"
        return remote_path

    def remove(self, config_id):
        """Remove an item from the registry

        Parameters
        ----------
        config_id : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id is not registered.

        """
        self._raise_if_dry_run("remove")
        self._check_if_not_registered(config_id)
        self.fs_interface.rmtree(self.get_registry_directory(config_id))
        logger.info("Removed %s from the registry.", config_id)

    def show(self, **kwargs):
        """Show a summary of the registered items in a table."""
        # TODO: filter by submitter
        table = PrettyTable(title=self.name())
        table.field_names = ("ID", "Version", "Registration Date", "Submitter", "Description")
        rows = []
        for config_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[-1]
            row = (
                config_id,
                last_reg.version,
                last_reg.date,
                last_reg.submitter,
                registry_config.model.description,
            )
            rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        print(table)

    def sync_pull(self, path):
        """Synchronizes files from the remote registry to local.
        Deletes any files locally that do not exist on remote.

        path : Path
            Local path

        """
        remote_path = self.relative_remote_path(path)
        self.cloud_interface.sync_pull(
            remote_path, path, exclude=SYNC_EXCLUDE_LIST, delete_local=True
        )

    def sync_push(self, path):
        """Synchronizes files from the local path to the remote registry.

        path : Path
            Local path

        """
        remote_path = self.relative_remote_path(path)
        lock_file_path = self.get_registry_lockfile(path.name)
        self.cloud_interface.check_lock(lock_file_path)
        self.cloud_interface.sync_push(
            remote_path=remote_path, local_path=path, exclude=SYNC_EXCLUDE_LIST
        )
