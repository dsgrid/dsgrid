"""Manages the registry for dimension mappings"""

import logging
import os
from pathlib import Path

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.association_tables import AssociationTableModel
from dsgrid.config.dimension_mapping_config import DimensionMappingConfig
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
from dsgrid.data_models import serialize_model
from dsgrid.registry.common import ConfigKey, make_initial_config_registration, ConfigKey
from dsgrid.utils.files import dump_data
from .dimension_mapping_registry import DimensionMappingRegistry
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionMappingRegistryManager(RegistryManagerBase):
    """Manages registered dimension mappings."""

    def __init__(self, path, fs_interface, cloud_interface, offline_mode, dry_run_mode):
        super().__init__(path, fs_interface, cloud_interface, offline_mode, dry_run_mode)
        self._mappings = {}  # ConfigKey to DimensionMappingModel

    @staticmethod
    def registry_class():
        return DimensionMappingRegistry

    def check_unique_records(self, config: DimensionMappingConfig, warn_only=False):
        """Check if any new tables have identical records as existing tables.

        Parameters
        ----------
        config : DimensionMappingConfig
        warn_only: bool
            If True, log a warning instead of raising an exception.

        Raises
        ------
        DSGDuplicateValueRegistered
            Raised if there are duplicates and warn_only is False.

        """
        hashes = set()
        for mapping_id, registry_config in self._registry_configs.items():
            mapping = self.get_by_id(mapping_id, registry_config.model.version)
            hashes.add(mapping.file_hash)

        duplicates = [x.mapping_id for x in config.model.mappings if x.file_hash in hashes]
        if duplicates:
            if warn_only:
                logger.warning("Dimension mapping records are duplicated: %s", duplicates)
            else:
                # TODO: we need to list which ones are duplicates of which dimension records.
                #   Currently the error spits out the dimension record with the UUID but the
                #   user has no idea what that actually means. The error would be more helpful
                #   it it said dimension something like "there are 7 duplicate dimensions"
                #   record with ID "counties.us-county-census2010"
                raise DSGDuplicateValueRegistered(
                    f"duplicate dimension mapping records: {duplicates}"
                )

    def get_by_id(self, item_id, version=None):
        if version is None:
            version = sorted(list(self._registry_configs[item_id].model.version))[-1]
        key = ConfigKey(item_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id, version=key.version):
            raise DSGValueNotRegistered(f"dimension mapping={key}")

        mapping = self._mappings.get(key)
        if mapping is not None:
            return mapping

        filename = self._path / key.id / str(key.version) / "dimension_mapping.toml"
        dimension_mapping = AssociationTableModel.load(filename)
        self._mappings[key] = dimension_mapping
        return dimension_mapping

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
            mappings[key] = self.get_by_key(key)

        return mappings

    def register(self, config_file, submitter, log_message, force=False):
        config = DimensionMappingConfig.load(config_file)
        config.assign_ids()
        self.check_unique_records(config, warn_only=force)

        registration = make_initial_config_registration(submitter, log_message)
        dest_config_filename = "dimension_mapping" + os.path.splitext(config_file)[1]
        config_dir = Path(os.path.dirname(config_file))

        for mapping in config.model.mappings:
            registry_config = RegistryBaseModel(
                version=registration.version,
                description=mapping.description.strip(),
                registration_history=[registration],
            )

            if not self._dry_run_mode:
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

                if not self._offline_mode:
                    DimensionMappingRegistry.sync_push(self._path)

                logger.info(
                    "%sRegistered dimension mapping id=%s version=%s",
                    self.log_offline_message,
                    mapping.mapping_id,
                    registration.version,
                )

        if not self._dry_run_mode:
            logger.info(
                "Registered %s dimension mapping(s) with version=%s",
                len(config.model.mappings),
                registration.version,
            )
        else:
            logger.info(
                "* DRY-RUN MODE * | %s dimension mapping(s) validated for registration with "
                "version=%s",
                len(config.model.mappings),
                registration.version,
            )

    def show(self, submitter=None):
        # TODO: filter by submitter
        table = PrettyTable(title="Dimension Mappings")
        table.field_names = ("ID", "Version", "Registration Date", "Submitter", "Description")
        rows = []
        for mapping_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[-1]
            row = (
                mapping_id,
                last_reg.version,
                last_reg.date,
                last_reg.submitter,
                registry_config.model.description,
            )
            rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)

        print(table)
