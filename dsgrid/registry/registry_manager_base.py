"""Base class for all registry managers."""

import abc
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List

from dsgrid import timer_stats_collector
from dsgrid.common import REGISTRY_FILENAME, SYNC_EXCLUDE_LIST
from dsgrid.exceptions import (
    DSGInvalidParameter,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid.utils.timing import track_timing
from .common import RegistryManagerParams, ConfigRegistrationModel, VersionUpdateType


logger = logging.getLogger(__name__)


class RegistryManagerBase(abc.ABC):
    """Base class for all registry managers."""

    def __init__(self, path, params: RegistryManagerParams):
        self._path = path
        self._params = params
        self._registry_configs = {}  # ID to registry config

    @track_timing(timer_stats_collector)
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
    def register(self, config_file, submitter, log_message, force=False, context=None):
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
        context : None or RegistrationContext
            If not None, assign the config IDs that get registered.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the config ID is already registered.

        """

    @abc.abstractmethod
    def register_from_config(self, config, submitter, log_message, force=False, context=None):
        """Registers a config file in the registry.

        Parameters
        ----------
        config : ConfigBase
            Configuration instance
        submitter : str
            Submitter name
        log_message : str
        force : bool
            If true, register even if an ID is duplicate.
        context : None or RegistrationContext
            If not None, assign the config IDs that get registered.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the config ID is already registered.

        """

    @staticmethod
    @abc.abstractmethod
    def registry_class():
        """Return the class used for the registry item."""

    @abc.abstractmethod
    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        """Updates the current registry with new parameters or data from a config file.

        Parameters
        ----------
        config_file : str
            Path to project config file
        config_id : str
        submitter : str
        update_type : VersionUpdateType
        log_message : str
        version : VersionInfo
            Version to update. Must be the current version.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGInvalidParameter
            Raised if config_id does not match config_file.
            Raised if the version is not the current version.

        """

    @abc.abstractmethod
    def update(self, config, update_type, log_message, submitter=None):
        """Updates the current registry with new parameters or data.

        Parameters
        ----------
        config : ConfigBase
        update_type : VersionUpdateType
        log_message : str
        submitter : str | None
            Submitter name. Use current user if None.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGInvalidParameter
            Raised if config_id does not match config_file.
            Raised if the version is not the current version.

        """

    def _check_update(self, config, config_id, version):
        if config.config_id != config_id:
            raise DSGInvalidParameter(
                f"ID={config_id} does not match ID in file: {config.config_id}"
            )

        cur_version = self.get_current_version(config_id)
        if version != cur_version:
            raise DSGInvalidParameter(f"version={version} is not current. Current={cur_version}")

    def _update_config(self, config, submitter, update_type, log_message):
        config_id = config.config_id
        registry_config = self.get_registry_config(config_id)

        if update_type == VersionUpdateType.MAJOR:
            registry_config.version = registry_config.version.bump_major()
        elif update_type == VersionUpdateType.MINOR:
            registry_config.version = registry_config.version.bump_minor()
        elif update_type == VersionUpdateType.PATCH:
            registry_config.version = registry_config.version.bump_patch()

        registration = ConfigRegistrationModel(
            version=str(registry_config.version),
            submitter=submitter,
            date=datetime.now(ZoneInfo("UTC")),
            log_message=log_message,
        )
        registry_config.registration_history.insert(0, registration)
        registry_config.serialize(self.get_registry_file(config_id), force=True)

        new_config_dir = self.get_config_directory(config_id, registry_config.version)
        self.fs_interface.mkdir(new_config_dir)
        config.serialize(new_config_dir)

        self._update_registry_cache(config_id, registry_config)
        logger.info(
            "Updated registry and config information for %s ID=%s version=%s",
            self.name(),
            config_id,
            registry_config.version,
        )
        return registry_config.version

    def _check_if_already_registered(self, config_id):
        if config_id in self._registry_configs:
            raise DSGDuplicateValueRegistered(f"{self.name()}={config_id}")

    def _check_if_not_registered(self, config_id):
        if config_id not in self._registry_configs:
            raise DSGValueNotRegistered(f"{self.name()}={config_id}")

    def _log_offline_mode_prefix(self):
        return "* OFFLINE MODE * |" if self.offline_mode else ""

    def _update_registry_cache(self, config_id, registry_config):
        self._registry_configs[config_id] = registry_config

    @property
    def cloud_interface(self):
        """Return the CloudStorageInterface to sync remote data."""
        return self._params.cloud_interface

    @cloud_interface.setter
    def cloud_interface(self, cloud_interface):
        """Set the CloudStorageInterface (used in testing)"""
        self._params = self._params._replace(cloud_interface=cloud_interface)

    def dump(self, config_id, directory, version=None, force=False):
        """Dump the config file to directory.

        Parameters
        ----------
        config_id : str
        directory : str
        version : VersionInfo | None
            Defaults to current version.
        force : bool
            If True, overwrite files if they exist.

        """
        path = Path(directory)
        os.makedirs(path, exist_ok=True)
        config = self.get_by_id(config_id, version)
        filename = config.serialize(path, force=force)
        if version is None:
            version = self._registry_configs[config_id].version
        logger.info(
            "Dumped config for type=%s ID=%s version=%s to %s",
            self.name(),
            config_id,
            version,
            filename,
        )

    @abc.abstractmethod
    def finalize_registration(self, config_ids: List[str], error_occurred: bool):
        """Peform final actions after a registration process.

        Parameters
        ----------
        config_ids : List[str]
            Config IDs that were registered
        error_occurred : bool
            Set to True if an error occurred and all intermediately-registered IDs should be
            removed.

        """

    @property
    def fs_interface(self):
        """Return the FilesystemInterface to list directories and read/write files."""
        return self._params.fs_interface

    @property
    def offline_mode(self):
        """Return True if there is to be no syncing with the remote registry."""
        return self._params.offline

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
        if version is None:
            version = self.get_current_version(config_id)
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
        self._check_if_not_registered(config_id)
        return self._registry_configs[config_id]

    @abc.abstractmethod
    def acquire_registry_locks(self, config_ids: List[str]):
        """Acquire lock(s) on the registry for all config_ids.

        Parameters
        ----------
        config_ids : List[str]

        Raises
        ------
        DSGRegistryLockError
            Raised if a lock cannot be acquired.

        """

    @abc.abstractmethod
    def get_registry_lock_file(self, config_id):
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

    def get_registry_directory(self, config_id):
        """Return the directory containing config info for config_id (registry.toml and versions).

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return self._path / config_id

    def get_registry_data_directory(self, config_id):
        """Return the directory containing data for config_id (parquet files).

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return Path(self._params.base_path) / "data" / config_id

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

    def iter_configs(self):
        """Return an iterator over the registered configs."""
        for config_id in self.iter_ids():
            yield self.get_by_id(config_id)

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

    @abc.abstractmethod
    def remove(self, config_id):
        """Remove an item from the registry.

        Parameters
        ----------
        config_id : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id is not registered.

        """
        # TODO: Do we want to handle specific versions? This removes all configs.

    def _remove(self, config_id):
        self._check_if_not_registered(config_id)
        self.fs_interface.rm_tree(self.get_registry_directory(config_id))
        self._registry_configs.pop(config_id, None)
        logger.info("Removed %s from the registry.", config_id)

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
        lock_file_path = self.get_registry_lock_file(path.name)
        self.cloud_interface.check_lock_file(lock_file_path)
        try:
            self.cloud_interface.sync_push(
                remote_path=remote_path, local_path=path, exclude=SYNC_EXCLUDE_LIST
            )
        except Exception:
            logger.exception(
                "Please report this error to the dsgrid team. The registry may need recovery."
            )
            raise
