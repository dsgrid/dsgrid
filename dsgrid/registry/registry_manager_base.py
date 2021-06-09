"""Base class for all registry managers."""

import abc
import logging
import os
from datetime import datetime
from pathlib import Path

from prettytable import PrettyTable

from dsgrid import timer_stats_collector
from .common import RegistryManagerParams, ConfigRegistrationModel, VersionUpdateType
from .registry_base import RegistryBaseModel
from dsgrid.common import REGISTRY_FILENAME, SYNC_EXCLUDE_LIST
from dsgrid.exceptions import (
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
    DSGInvalidOperation,
    DSGInvalidParameter,
)
from dsgrid.utils.files import load_data
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.timing import Timer, track_timing


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
            lock_file_path = self.get_registry_lock_file(load_data(config_file)["project_id"])
            with self.cloud_interface.make_lock_file(lock_file_path):
                self._register(config_file, submitter, log_message, force=force)

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
            version=registry_config.version,
            submitter=submitter,
            date=datetime.now(),
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

    def _log_dry_run_mode_prefix(self):
        return "* DRY RUN MODE * |" if self.dry_run_mode else ""

    def _log_offline_mode_prefix(self):
        return "* OFFLINE MODE * |" if self.offline_mode else ""

    def _update_registry_cache(self, config_id, registry_config):
        self._registry_configs[config_id] = registry_config

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

    def _remove(self, config_id):
        if not self.offline_mode:
            # TODO: DSGRID-145
            raise Exception("sync-push of config removal is currently not supported")
        self._raise_if_dry_run("remove")
        self._check_if_not_registered(config_id)
        self.fs_interface.rm_tree(self.get_registry_directory(config_id))
        self._registry_configs.pop(config_id, None)
        logger.info("Removed %s from the registry.", config_id)

    def show(self, filters=None, **kwargs):
        """Show a summary of the registered items in a table."""
        if filters:
            logger.info("List registry for: %s", filters)

        table = PrettyTable(title=self.name())
        table.field_names = ("ID", "Version", "Registration Date", "Submitter", "Description")
        table._max_width = {
            "ID": 50,
            "Description": 50,
        }
        # table.max_width = 70

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for config_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[-1]

            row = (
                config_id,
                last_reg.version,
                last_reg.date.strftime("%Y-%m-%d %H:%M:%S"),
                last_reg.submitter,
                registry_config.model.description,
            )
            if not filters or matches_filters(row, field_to_index, transformed_filters):
                rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        table.align = "l"
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
        lock_file_path = self.get_registry_lock_file(path.name)
        self.cloud_interface.check_lock_file(lock_file_path)
        self.cloud_interface.sync_push(
            remote_path=remote_path, local_path=path, exclude=SYNC_EXCLUDE_LIST
        )
