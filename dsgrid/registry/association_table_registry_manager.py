"""Manages the registry for association tables"""

import logging
import os
from pathlib import Path

from semver import VersionInfo

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.association_tables import (
    AssociationTableModel,
    AssociationTableConfig,
)
from dsgrid.exceptions import DSGValueNotStored, DSGDuplicateRecords
from dsgrid.data_models import serialize_model
from dsgrid.utils.files import dump_data
from .common import (
    make_default_config_registration,
    ConfigKey,
)
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class AssociationTableRegistryManager(RegistryManagerBase):
    """Manages registered association tables."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._associations = {}  # key = (association_table_id, version)
        # value = AssociationTableBaseModel
        self._association_versions = {}
        self._associations = {}  # key = ConfigKey, value = AssociationTableModel

        for table_id in self._fs_intf.listdir(self._path, directories_only=True):
            id_path = Path(self._path) / table_id
            self._association_versions[table_id] = {
                VersionInfo.parse(x) for x in self._fs_intf.listdir(id_path, directories_only=True)
            }

    def check_unique_records(self, table_config: AssociationTableConfig, warn_only=False):
        """Check if any new tables have identical records as existing tables.

        Parameters
        ----------
        table_config : AssociationTableConfig
        warn_only: bool
            If True, log a warning instead of raising an exception.

        Raises
        ------
        DSGDuplicateRecords
            Raised if there are duplicates and warn_only is False.

        """
        hashes = set()
        for table_id, versions in self._association_versions.items():
            for version in versions:
                table = self.get_association_table(table_id, version)
                hashes.add(table.file_hash)

        duplicates = [
            x.association_table_id
            for x in table_config.model.association_tables
            if x.file_hash in hashes
        ]
        if duplicates:
            if warn_only:
                logger.warning("Association table records are duplicated: %s", duplicates)
            else:
                raise DSGDuplicateRecords(f"duplicate association table records: {duplicates}")

    def get_association_table(self, association_table_id, version):
        """Get the association_table matching the parameters. Returns from cache if already loaded.

        Parameters
        ----------
        association_table_id : str
        version : VersionInfo

        Returns
        -------
        AssociationTableBaseModel

        Raises
        ------
        DSGValueNotStored
            Raised if the association_table is not stored.

        """
        key = ConfigKey(association_table_id, version)
        return self.get_association_table_by_key(key)

    def get_association_table_by_key(self, key):
        """Get the association table matching key. Returns from cache if already loaded.

        Parameters
        ----------
        key : ConfigKey Key

        """
        if not self.has_association_table_id(key):
            raise DSGValueNotStored(f"association_table not stored: {key}")

        table = self._associations.get(key)
        if table is not None:
            return table

        filename = self._path / key.id / str(key.version) / "association_table.toml"
        association_table = AssociationTableModel.load(filename)
        self._associations[key] = association_table
        return association_table

    def has_association_table_id(self, key):
        """Return True if an association table matching the parameters is stored.

        Parameters
        ----------
        key : ConfigKey

        Returns
        -------
        bool

        """
        return (
            key.id in self._association_versions
            and key.version in self._association_versions[key.id]
        )

    def iter_association_table_ids(self):
        """Return an iterator over the registered association table IDs."""
        return self._association_versions.keys()

    def list_association_table_ids(self):
        """Return the association table IDs.

        Returns
        -------
        list

        """
        return sorted(list(self._association_versions.keys()))

    def load_association_tables(self, association_table_references):
        """Load association_tables from files.

        Parameters
        ----------
        association_table_references : list
            iterable of AssociationTableReferenceModel instances

        Returns
        -------
        dict
            ConfigKey to AssociationTableModel

        """
        tables = {}
        for ref in association_table_references:
            key = ConfigKey(ref.id, ref.version)
            tables[key] = self.get_association_table_by_key(key)

        return tables

    def register_association_tables(self, config_file, submitter, log_message, force=False):
        """Registers association tables.

        Parameters
        ----------
        config_file : str
            Path to association table config file
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        ValueError
            Raised if the config_file is invalid.

        """
        table_config = AssociationTableConfig.load(config_file)
        table_config.assign_ids()
        self.check_unique_records(table_config, warn_only=force)

        registration = make_default_config_registration(submitter, log_message)
        dest_config_filename = "association_table" + os.path.splitext(config_file)[1]
        config_dir = Path(os.path.dirname(config_file))

        for table in table_config.model.association_tables:
            registry_config = RegistryBaseModel(
                version=registration.version,
                description=table.description.strip(),
                registration_history=[registration],
            )
            dest_dir = self._path / table.association_table_id / str(registration.version)
            self._fs_intf.mkdir(dest_dir)

            filename = Path(os.path.dirname(dest_dir)) / REGISTRY_FILENAME
            data = serialize_model(registry_config)
            # TODO: if we want to update AWS directly, this needs to change.
            dump_data(data, filename)

            # Leading directories from the original are not relevant in the registry.
            dest_record_file = dest_dir / os.path.basename(table.filename)
            self._fs_intf.copy_file(config_dir / table.filename, dest_record_file)

            model_data = serialize_model(table)
            # We have to make this change in the serialized dict instead of
            # model because Pydantic will fail the assignment due to not being
            # able to find the path.
            model_data["file"] = os.path.basename(table.filename)
            dump_data(model_data, dest_dir / dest_config_filename)

        logger.info(
            "Registered %s association table(s) with version=%s",
            len(table_config.model.association_tables),
            registration.version,
        )
