"""Base class for all registry managers."""

import abc
import copy
import logging
from pathlib import Path
from typing import Any, Self, Type

from semver import VersionInfo
from sqlalchemy import Connection

from dsgrid.config.config_base import ConfigBase
from dsgrid.exceptions import (
    DSGInvalidParameter,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid.registry.registration_context import RegistrationContext
from dsgrid.registry.registry_interface import RegistryInterfaceBase
from dsgrid.registry.common import RegistryManagerParams, VersionUpdateType


logger = logging.getLogger(__name__)


class RegistryManagerBase(abc.ABC):
    """Base class for all registry managers."""

    def __init__(self, path, params: RegistryManagerParams):
        self._path = path
        self._params = params
        self._db = None

        if not path.exists():
            logger.warning(
                "The registry data path=%s does not exist. You will able to inspect the registry "
                "contents, but you will not be able to perform any data-related activities.",
                path,
            )

    @property
    @abc.abstractmethod
    def db(self) -> RegistryInterfaceBase:
        """Return the database interface."""

    @db.setter
    @abc.abstractmethod
    def db(self, db: RegistryInterfaceBase) -> None:
        """Return the database interface."""

    @classmethod
    def load(cls, path, params, db, *args: Any, **kwargs: Any) -> Self:
        """Load the registry manager.

        path : str
        params : RegistryManagerParams

        Returns
        -------
        RegistryManagerBase

        """
        mgr = cls(path, params)
        mgr.db = db
        return mgr

    @classmethod
    def _load(cls, path, params: RegistryManagerParams, *args):
        mgr = cls(path, params, *args)
        return mgr

    @staticmethod
    @abc.abstractmethod
    def config_class() -> Type:
        """Return the class used for storing the config."""

    @abc.abstractmethod
    def get_by_id(
        self, config_id: str, version: str | None = None, conn: Connection | None = None
    ) -> ConfigBase:
        """Get the item matching matching ID. Returns from cache if already loaded.

        Parameters
        ----------
        config_id : str
        version : str
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
    def name() -> str:
        """Return the name of the registry, used for reporting.

        Returns
        -------
        str

        """

    @abc.abstractmethod
    def register(self, *args: Any, **kwargs: Any) -> Any:
        """Registers a config file in the registry.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the config ID is already registered.

        """

    @abc.abstractmethod
    def register_from_config(self, config: ConfigBase, *args: Any, **kwargs) -> Any:
        """Registers a config file in the registry.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueRegistered
            Raised if the config ID is already registered.

        """

    @abc.abstractmethod
    def update_from_file(self, *args: Any, **kwargs: Any) -> ConfigBase:
        """Updates the current registry with new parameters or data from a config file.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGInvalidParameter
            Raised if config_id does not match config_file.
            Raised if the version is not the current version.

        """

    @abc.abstractmethod
    def update(
        self,
        config: ConfigBase,
        *args: Any,
        **kwargs: Any,
    ) -> ConfigBase:
        """Updates the current registry with new parameters or data.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGInvalidParameter
            Raised if config_id does not match config_file.
            Raised if the version is not the current version.

        """

    def _check_update(
        self, conn: Connection, config: ConfigBase, config_id: str, version: str
    ) -> None:
        if config.config_id != config_id:
            msg = f"ID={config_id} does not match ID in file: {config.config_id}"
            raise DSGInvalidParameter(msg)

        cur_version = self.get_latest_version(config_id, conn=conn)
        if version != cur_version:
            msg = f"version={version} is not current. Current={cur_version}"
            raise DSGInvalidParameter(msg)

    @staticmethod
    def get_next_version(version: str, update_type: VersionUpdateType):
        ver = VersionInfo.parse(version)
        if update_type == VersionUpdateType.MAJOR:
            next_version = ver.bump_major()
        elif update_type == VersionUpdateType.MINOR:
            next_version = ver.bump_minor()
        elif update_type == VersionUpdateType.PATCH:
            next_version = ver.bump_patch()
        else:
            msg = f"invalid version {update_type=}"
            raise NotImplementedError(msg)

        return str(next_version)

    def _update_config(self, config, context: RegistrationContext):
        config_id = config.config_id
        cur_version = config.model.version
        new_model = copy.deepcopy(config.model)
        new_model.version = self.get_next_version(cur_version, context.registration.update_type)
        updated_model = self.db.update(context.connection, new_model, context.registration)
        logger.info(
            "Updated registry and config information for %s ID=%s version=%s",
            self.name(),
            config_id,
            updated_model.version,
        )
        return updated_model

    def _check_if_already_registered(self, conn: Connection, config_id):
        if self.db.has(conn, config_id):
            msg = f"{self.name()}={config_id}"
            raise DSGDuplicateValueRegistered(msg)

    def _check_if_not_registered(self, conn: Connection, config_id):
        if not self.db.has(conn, config_id):
            msg = f"{self.name()}={config_id}"
            raise DSGValueNotRegistered(msg)

    def _log_offline_mode_prefix(self):
        return "* OFFLINE MODE * |" if self.offline_mode else ""

    @property
    def cloud_interface(self):
        """Return the CloudStorageInterface to sync remote data."""
        return self._params.cloud_interface

    @cloud_interface.setter
    def cloud_interface(self, cloud_interface):
        """Set the CloudStorageInterface (used in testing)"""
        self._params = self._params._replace(cloud_interface=cloud_interface)

    def dump(
        self,
        config_id,
        directory,
        version=None,
        conn: Connection | None = None,
        force: bool = False,
    ):
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
        path.mkdir(exist_ok=True, parents=True)
        config = self.get_by_id(config_id, version, conn=conn)
        filename = config.serialize(path, force=force)
        logger.info(
            "Dumped config for type=%s ID=%s version=%s to %s",
            self.name(),
            config_id,
            config.model.version,
            filename,
        )

    def finalize_registration(self, conn: Connection, config_ids: set[str], error_occurred: bool):
        """Peform final actions after a registration process.

        Parameters
        ----------
        config_ids : set[str]
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

    def get_latest_version(self, config_id, conn: Connection | None = None):
        """Return the current version in the registry.

        Returns
        -------
        str

        """
        return self.db.get_latest_version(conn, config_id)

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

    def has_id(self, config_id, version=None, conn: Connection | None = None):
        """Return True if an item matching the parameters is stored.

        Parameters
        ----------
        config_id : str
        version : str
            If None, use latest.

        Returns
        -------
        bool

        """
        return self.db.has(conn, config_id, version=version)

    def iter_configs(self, conn: Connection | None = None):
        """Return an iterator over the registered configs."""
        for config_id in self.iter_ids(conn):
            yield self.get_by_id(config_id, conn=conn)

    def iter_ids(self, conn: Connection | None = None):
        """Return an iterator over the registered dsgrid IDs."""
        yield from self.db.list_model_ids(conn)

    def list_ids(self, conn: Connection | None = None, **kwargs: Any):
        """Return the IDs.

        Returns
        -------
        list

        """
        return sorted(self.iter_ids(conn))

    def relative_remote_path(self, path):
        """Return relative remote registry path."""
        relative_path = Path(path).relative_to(self._params.base_path)
        remote_path = f"{self._params.remote_path}/{relative_path}"
        return remote_path

    @abc.abstractmethod
    def remove(self, config_id: str, conn: Connection | None = None) -> None:
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
