"""Base class for all registry managers."""

import abc
import logging
from pathlib import Path

from prettytable import PrettyTable

from .registry_base import RegistryBaseModel
from dsgrid.common import REGISTRY_FILENAME
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
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
        self._registry_configs = {}  # ID to registry config

    def inventory(self):
        for config_id in self._fs_intf.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            id_path = Path(self._path) / config_id
            registry = self.registry_class().load(id_path / REGISTRY_FILENAME)
            self._registry_configs[config_id] = registry

    @classmethod
    def load(
        cls, path, fs_interface, cloud_interface, offline_mode, dry_run_mode, *args, **kwargs
    ):
        """Load the registry manager.

        path : str
        fs_interface : FilesystemInterface

        RegistryManagerBase

        """
        mgr = cls(path, fs_interface)
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

    def _update_registry_cache(self, config_id, registry_model: RegistryBaseModel):
        assert config_id not in self._registry_configs, config_id
        self._registry_configs[config_id] = self.registry_class()(registry_model)

    def get_config_directory(self, config_id):
        """Return the path to the config file.

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return self._path / config_id

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
        return self._path / config_id / str(version) / self.registry_class().config_filename()

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

    def get_registry_file(self, config_id):
        """Return the path to the registry file.

        Parameters
        ----------
        config_id : str

        Returns
        -------
        str

        """
        return self._path / config_id / REGISTRY_FILENAME

    @property
    def fs_interface(self):
        """Return the filesystem interface."""
        return self._fs_intf

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
        return self._fs_intf.exists(self._path / config_id / str(version))

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
        if config_id not in self._registry_configs:
            raise DSGValueNotRegistered(f"project_id={config_id}")

        self._fs_intf.rmtree(self.get_config_directory(config_id))
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
