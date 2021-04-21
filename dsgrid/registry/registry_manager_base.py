"""Base class for all registry managers."""

import abc
import logging
from pathlib import Path

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.filesytem.aws import AwsS3Bucket
from dsgrid.filesytem.local_filesystem import LocalFilesystem


logger = logging.getLogger(__name__)


class RegistryManagerBase(abc.ABC):
    """Base class for all registry managers."""

    def __init__(self, path, fs_interface):
        if isinstance(fs_interface, AwsS3Bucket):
            self._path = fs_interface.path
        else:
            assert isinstance(fs_interface, LocalFilesystem)
            self._path = path

        self._fs_intf = fs_interface
        self._current_versions = {}  # ID to current version

    def inventory(self):
        for item_id in self._fs_intf.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            id_path = Path(self._path) / item_id
            registry = self.registry_class().load(id_path / REGISTRY_FILENAME)
            self._current_versions[item_id] = registry.model.version

    @classmethod
    def load(cls, path, fs_intferace):
        """Load the registry manager.

        path : str
        fs_intferace : FilesystemInterface

        RegistryManagerBase

        """
        mgr = cls(path, fs_intferace)
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
    def get_by_id(self, item_id, version=None):
        """Get the item matching matching ID. Returns from cache if already loaded.

        Parameters
        ----------
        item_id : str
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

    @abc.abstractmethod
    def register(self, config_file, submitter, log_message, force=False):
        """Registers a config file in the registry.

        Parameters
        ----------
        config_file : str
            Path to dimension mapping config file
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
            Raised if the mapping is already registered.

        """

    @staticmethod
    @abc.abstractmethod
    def registry_class():
        """Return the class used for the registry item."""

    def get_current_version(self, item_id):
        """Return the current version in the registry.

        Returns
        -------
        VersionInfo

        """
        return self._current_versions[item_id]

    @property
    def fs_interface(self):
        """Return the filesystem interface."""
        return self._fs_intf

    def has_id(self, item_id, version=None):
        """Return True if an item matching the parameters is stored.

        Parameters
        ----------
        item_id : str
        version : VersionInfo
            If None, use latest.

        Returns
        -------
        bool

        """
        if version is None:
            return item_id in self._current_versions
        path = self._path / item_id / str(version)
        return self._fs_intf.exists(path)

    def iter_ids(self):
        """Return an iterator over the registered IDs."""
        return self._current_versions.keys()

    def list_ids(self, **kwargs):
        """Return the IDs.

        Returns
        -------
        list

        """
        return sorted(list(self.iter_ids()))
