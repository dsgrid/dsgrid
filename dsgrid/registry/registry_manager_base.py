"""Base class for all registry managers."""

import abc
import logging
from pathlib import Path

from dsgrid.common import REGISTRY_FILENAME

# from dsgrid.filesytem.aws import AwsS3Bucket
from dsgrid.filesytem.local_filesystem import LocalFilesystem
from dsgrid.filesytem.cloud_filesystem import CloudFilesystemInterface

logger = logging.getLogger(__name__)


class RegistryManagerBase(abc.ABC):
    """Base class for all registry managers."""

    def __init__(self, path, fs_interface, cloud_interface, offline_mode, dryrun_mode):
        # if isinstance(fs_interface, CloudFilesystemInterface):
        #     self._path = fs_interface.path
        # else:
        #     assert isinstance(fs_interface, LocalFilesystem)
        #     self._path = path
        assert isinstance(fs_interface, LocalFilesystem)
        self._path = path

        if cloud_interface is not None:
            assert isinstance(cloud_interface, CloudFilesystemInterface)
            self._remote_path = cloud_interface.path

        self._fs_intf = fs_interface
        self._cld_intf = cloud_interface

        self._fs_intf = fs_interface
        self._registry_configs = {}  # ID to current version

        self._offline_mode = offline_mode
        self._dryrun_mode = dryrun_mode

    def inventory(self):
        for item_id in self._fs_intf.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            id_path = Path(self._path) / item_id
            registry = self.registry_class().load(id_path / REGISTRY_FILENAME)
            self._registry_configs[item_id] = registry

    @classmethod
    def load(cls, path, fs_intferace, cloud_interface, offline_mode, dryrun_mode):
        """Load the registry manager.

        path : str
        fs_intferace : FilesystemInterface
        cloud_interface : CloudFilesystemInterface
        offline_mode : bool
        dryrun_mode :  bool

        RegistryManagerBase

        """
        mgr = cls(path, fs_intferace, cloud_interface, offline_mode, dryrun_mode)
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

    @abc.abstractmethod
    def show(self, **kwargs):
        """Show a summary of the registered items in a table."""

    def get_current_version(self, item_id):
        """Return the current version in the registry.

        Returns
        -------
        VersionInfo

        """
        return self._registry_configs[item_id].model.version

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
            return item_id in self._registry_configs
        path = self._path / item_id / str(version)
        return self._fs_intf.exists(path)

    def iter_ids(self):
        """Return an iterator over the registered IDs."""
        return self._registry_configs.keys()

    def list_ids(self, **kwargs):
        """Return the IDs.

        Returns
        -------
        list

        """
        return sorted(list(self.iter_ids()))
