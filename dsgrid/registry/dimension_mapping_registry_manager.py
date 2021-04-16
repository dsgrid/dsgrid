"""Manages the registry for dimension mappings"""

import logging
import os
from pathlib import Path

from semver import VersionInfo

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.association_tables import AssociationTableModel
from dsgrid.config.dimension_mapping_config import DimensionMappingConfig
from dsgrid.exceptions import DSGValueNotStored, DSGDuplicateRecords
from dsgrid.data_models import serialize_model
from dsgrid.registry.common import ConfigKey, make_default_config_registration, ConfigKey
from dsgrid.utils.files import dump_data
from .dimension_mapping_registry import DimensionMappingRegistry
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionMappingRegistryManager(RegistryManagerBase):
    """Manages registered dimension mappings."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        # value = DimensionMappingBaseModel
        self._current_versions = {}
        self._mappings = {}  # key = ConfigKey, value = DimensionMappingModel

        for mapping_id in self._fs_intf.listdir(self._path, directories_only=True):
            id_path = Path(self._path) / mapping_id
            registry = DimensionMappingRegistry.load(id_path / REGISTRY_FILENAME)
            self._current_versions[mapping_id] = registry.model.version

    def check_unique_records(self, mapping_config: DimensionMappingConfig, warn_only=False):
        """Check if any new tables have identical records as existing tables.

        Parameters
        ----------
        mapping_config : DimensionMappingConfig
        warn_only: bool
            If True, log a warning instead of raising an exception.

        Raises
        ------
        DSGDuplicateRecords
            Raised if there are duplicates and warn_only is False.

        """
        hashes = set()
        for mapping_id, version in self._current_versions.items():
            mapping = self.get_dimension_mapping(mapping_id, version)
            hashes.add(mapping.file_hash)

        duplicates = [x.mapping_id for x in mapping_config.model.mappings if x.file_hash in hashes]
        if duplicates:
            if warn_only:
                logger.warning("Dimension mapping records are duplicated: %s", duplicates)
            else:
                raise DSGDuplicateRecords(f"duplicate dimension mapping records: {duplicates}")

    def get_dimension_mapping(self, mapping_id, version=None):
        """Get the dimension mapping matching the parameters. Returns from cache if already loaded.

        Parameters
        ----------
        mapping_id : str
        version : VersionInfo
            If None, return the latest version.

        Returns
        -------
        DimensionMappingBaseModel

        Raises
        ------
        DSGValueNotStored
            Raised if the dimension_mapping is not stored.

        """
        if version is None:
            version = sorted(list(self._current_versions[mapping_id]))[-1]
        key = ConfigKey(mapping_id, version)
        return self.get_dimension_mapping_by_key(key)

    def get_dimension_mapping_by_key(self, key):
        """Get the dimension mapping matching key. Returns from cache if already loaded.

        Parameters
        ----------
        key : ConfigKey Key

        """
        if not self.has_mapping_id(key.id, version=key.version):
            raise DSGValueNotStored(f"dimension_mapping not stored: {key}")

        mapping = self._mappings.get(key)
        if mapping is not None:
            return mapping

        filename = self._path / key.id / str(key.version) / "dimension_mapping.toml"
        dimension_mapping = AssociationTableModel.load(filename)
        self._mappings[key] = dimension_mapping
        return dimension_mapping

    def has_mapping_id(self, mapping_id, version=None):
        """Return True if an dimension mapping matching the parameters is stored.

        Parameters
        ----------
        mapping_id : str
        version : VersionInfo
            If None, use latest.

        Returns
        -------
        bool

        """
        if version is None:
            return mapping_id in self._current_versions
        path = self._path / mapping_id / str(version)
        return self._fs_intf.exists(path)

    def iter_mapping_ids(self):
        """Return an iterator over the registered dimension mapping IDs."""
        return self._current_versions.keys()

    def list_mapping_ids(self):
        """Return the dimension mapping IDs.

        Returns
        -------
        list

        """
        return sorted(list(self.iter_mapping_ids()))

    def load_dimension_mappings(self, dimension_mapping_references):
        """Load dimension_mappings from files.

        Parameters
        ----------
        dimension_mapping_references : list
            iterable of DimensionMappingReferenceModel instances

        Returns
        -------
        dict
            ConfigKey to DimensionMappingModel

        """
        mappings = {}
        for ref in dimension_mapping_references:
            key = ConfigKey(ref.id, ref.version)
            mappings[key] = self.get_dimension_mapping_by_key(key)

        return mappings

    def register_dimension_mappings(self, config_file, submitter, log_message, force=False):
        """Registers dimension mappings.

        Parameters
        ----------
        config_file : str
            Path to dimension mapping config file
        submitter : str
            Submitter name
        log_message : str
        force : bool
            If true, register the mapping even if it is duplicate.

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.
        DSGDuplicateValueStored
            Raised if the mapping is already registered.

        """
        config = DimensionMappingConfig.load(config_file)
        config.assign_ids()
        self.check_unique_records(config, warn_only=force)

        registration = make_default_config_registration(submitter, log_message)
        dest_config_filename = "dimension_mapping" + os.path.splitext(config_file)[1]
        config_dir = Path(os.path.dirname(config_file))

        for mapping in config.model.mappings:
            registry_config = RegistryBaseModel(
                version=registration.version,
                description=mapping.description.strip(),
                registration_history=[registration],
            )
            dest_dir = self._path / mapping.mapping_id / str(registration.version)
            self._fs_intf.mkdir(dest_dir)

            filename = Path(os.path.dirname(dest_dir)) / REGISTRY_FILENAME
            data = serialize_model(registry_config)
            # TODO: if we want to update AWS directly, this needs to change.
            dump_data(data, filename)

            # Leading directories from the original are not relevant in the registry.
            dest_record_file = dest_dir / os.path.basename(mapping.filename)
            self._fs_intf.copy_file(config_dir / mapping.filename, dest_record_file)

            model_data = serialize_model(mapping)
            # We have to make this change in the serialized dict instead of
            # model because Pydantic will fail the assignment due to not being
            # able to find the path.
            model_data["file"] = os.path.basename(mapping.filename)
            dump_data(model_data, dest_dir / dest_config_filename)

        logger.info(
            "Registered %s dimension mapping(s) with version=%s",
            len(config.model.mappings),
            registration.version,
        )
