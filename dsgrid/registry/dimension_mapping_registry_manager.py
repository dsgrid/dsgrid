"""Manages the registry for dimension mappings"""

import logging
import os
from pathlib import Path

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

    def __init__(self, path, params):
        super().__init__(path, params)
        self._mappings = {}  # ConfigKey to DimensionMappingModel

    @staticmethod
    def name():
        return "Dimension Mappings"

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
        hashes = {}
        for mapping_id, registry_config in self._registry_configs.items():
            mapping = self.get_by_id(mapping_id, registry_config.model.version)
            hashes[mapping.file_hash] = mapping_id

        duplicates = []
        for mapping in config.model.mappings:
            if mapping.file_hash in hashes:
                duplicates.append((mapping.mapping_id, hashes[mapping.file_hash]))

        if duplicates:
            for dup in duplicates:
                logger.error("%s duplicates existing mapping ID %s", dup[0], dup[1])
            if not warn_only:
                raise DSGDuplicateValueRegistered(
                    f"There are {len(duplicates)} duplicate dimension mapping records."
                )

    def get_by_id(self, config_id, version=None):
        if version is None:
            version = self._registry_configs[config_id].model.version
        key = ConfigKey(config_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id, version=key.version):
            raise DSGValueNotRegistered(f"dimension mapping={key}")

        mapping = self._mappings.get(key)
        if mapping is not None:
            return mapping

        filename = self.get_config_file(key.id, key.version)
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
        if self.offline_mode or self.dry_run_mode:
            self._register(config_file, submitter, log_message, force=force)
        with self.cloud_interface.make_lock(self.relative_remote_path(self._path)):
            self._register(config_file, submitter, log_message, force=force)

    def _register(self, config_file, submitter, log_message, force=False):
        config = DimensionMappingConfig.load(config_file)
        config.assign_ids()
        self.check_unique_records(config, warn_only=force)

        registration = make_initial_config_registration(submitter, log_message)
        dest_config_filename = "dimension_mapping" + os.path.splitext(config_file)[1]
        config_dir = Path(os.path.dirname(config_file))

        if self.dry_run_mode:
            for mapping in config.model.mappings:
                logger.info(
                    "%s Dimension mapping validated for registration: from=%s to=%s",
                    self._log_dry_run_mode_prefix(),
                    mapping.from_type.value,
                    mapping.to_type.value,
                )
            return

        for mapping in config.model.mappings:
            registry_model = RegistryBaseModel(
                version=registration.version,
                description=mapping.description.strip(),
                registration_history=[registration],
            )
            dest_dir = self._path / mapping.mapping_id / str(registration.version)
            self.fs_interface.mkdir(dest_dir)

            registry_file = Path(os.path.dirname(dest_dir)) / REGISTRY_FILENAME
            data = serialize_model(registry_model)
            dump_data(data, registry_file)

            # Leading directories from the original are not relevant in the registry.
            dest_record_file = dest_dir / os.path.basename(mapping.filename)
            self.fs_interface.copy_file(config_dir / mapping.filename, dest_record_file)

            model_data = serialize_model(mapping)
            # We have to make this change in the serialized dict instead of
            # model because Pydantic will fail the assignment due to not being
            # able to find the path.
            model_data["file"] = os.path.basename(mapping.filename)
            dump_data(model_data, dest_dir / dest_config_filename)
            logger.info(
                "%s Registered dimension mapping id=%s version=%s",
                self._log_offline_mode_prefix(),
                mapping.mapping_id,
                registration.version,
            )
            self._update_registry_cache(mapping.mapping_id, registry_model)

        if not self.offline_mode:
            # Sync the entire dimension mapping registry path because it's probably cheaper
            # than syncing each changed path individually.
            self.sync_push(self._path)

        logger.info(
            "%s Registered %s dimension mapping(s) with version=%s",
            self._log_offline_mode_prefix(),
            len(config.model.mappings),
            registration.version,
        )

    def update(self, config_file, submitter, update_type, log_message):
        assert False, "not supported yet"
