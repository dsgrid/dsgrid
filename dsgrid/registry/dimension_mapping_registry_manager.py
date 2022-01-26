"""Manages the registry for dimension mappings"""

import getpass
import logging
import os
from pathlib import Path

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.association_tables import AssociationTableConfig
from dsgrid.config.dimension_mappings_config import DimensionMappingsConfig
from dsgrid.exceptions import (
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid.data_models import serialize_model
from dsgrid.registry.common import ConfigKey, make_initial_config_registration, ConfigKey
from dsgrid.utils.files import dump_data
from .dimension_mapping_registry import DimensionMappingRegistry, DimensionMappingRegistryModel
from .dimension_registry_manager import DimensionRegistryManager
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters


logger = logging.getLogger(__name__)


class DimensionMappingRegistryManager(RegistryManagerBase):
    """Manages registered dimension mappings."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._mappings = {}  # ConfigKey to DimensionMappingModel
        self._dimension_mgr = None
        self._id_to_type = {}

    @classmethod
    def load(cls, path, fs_interface, dimension_manager):
        mgr = cls._load(path, fs_interface)
        mgr.dimension_manager = dimension_manager
        return mgr

    def inventory(self):
        for map_id in self.fs_interface.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            map_path = self._path / map_id
            registry = self.registry_class().load(map_path / REGISTRY_FILENAME)
            self._registry_configs[map_id] = registry
            registry_config = self.get_by_id(map_id)
            self._id_to_type[map_id] = [
                registry_config.model.from_dimension.dimension_type,
                registry_config.model.to_dimension.dimension_type,
            ]

    @staticmethod
    def name():
        return "Dimension Mappings"

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val: DimensionRegistryManager):
        self._dimension_mgr = val

    @staticmethod
    def registry_class():
        return DimensionMappingRegistry

    def _check_unique_records(self, config: DimensionMappingsConfig, warn_only=False):
        """Check if any new mapping files have identical contents as any existing files.

        Parameters
        ----------
        config : DimensionMappingsConfig
        warn_only: bool
            If True, log a warning instead of raising an exception.

        Raises
        ------
        DSGDuplicateValueRegistered
            Raised if there are duplicates and warn_only is False.

        """
        hashes = {}
        for mapping_id, registry_config in self._registry_configs.items():
            mapping = self.get_by_id(mapping_id, registry_config.model.version).model
            hashes[mapping.file_hash] = mapping_id

        duplicates = []
        for mapping in config.model.mappings:
            if mapping.file_hash in hashes:
                duplicates.append((mapping.mapping_id, hashes[mapping.file_hash]))

        if duplicates:
            for dup in duplicates:
                logger.error(
                    "%s has duplicate content with existing mapping ID %s", dup[0], dup[1]
                )
            if not warn_only:
                raise DSGDuplicateValueRegistered(
                    f"There are {len(duplicates)} dimension mappings with duplicate content (data files)."
                )

    def _check_records_against_dimension_records(self, config):
        for mapping in config.model.mappings:
            actual_from_records = {x.from_id for x in mapping.records}
            actual_to_records = {x.to_id for x in mapping.records}
            from_dimension = self._dimension_mgr.get_by_id(mapping.from_dimension.dimension_id)
            allowed_from_records = from_dimension.get_unique_ids()
            diff = actual_from_records.symmetric_difference(allowed_from_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                raise DSGInvalidDimensionMapping(
                    f"Dimension mapping 'from' records are not symmetric with dimension_id={dim_id}: invalid={diff}"
                )
            to_dimension = self._dimension_mgr.get_by_id(mapping.to_dimension.dimension_id)
            allowed_to_records = to_dimension.get_unique_ids()
            diff = actual_to_records.symmetric_difference(allowed_to_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                raise DSGInvalidDimensionMapping(
                    f"Dimension mapping 'to' records are not symmetric with dimension_id={dim_id}: invalid={diff}"
                )

    def validate_records(self, config: DimensionMappingsConfig, warn_only=False):
        """Validate dimension mapping records.

        Check:
        - from_id and to_id column names
        - check for duplicate IDs and log a warning if they exist (sometimes we want them to exist if it is an aggregation)
        """
        pass

    def get_by_id(self, config_id, version=None):
        self._check_if_not_registered(config_id)
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
        config = AssociationTableConfig.load(filename)
        self._mappings[key] = config
        return config

    def get_registry_lock_file(self, config_id):
        return "configs/.locks/dimension_mappings.lock"

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
            mappings[key] = self.get_by_key(key).model

        return mappings

    def register(self, config_file, submitter, log_message, force=False):
        lock_file_path = self.get_registry_lock_file(None)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._register(config_file, submitter, log_message, force=force)

    def _register(self, config_file, submitter, log_message, force=False):
        config = DimensionMappingsConfig.load(config_file)
        config.assign_ids()
        self._check_unique_records(config, warn_only=force)
        self._check_records_against_dimension_records(config)
        self.validate_records(config, warn_only=force)

        registration = make_initial_config_registration(submitter, log_message)
        src_dir = Path(os.path.dirname(config_file))

        if self.dry_run_mode:
            for mapping in config.model.mappings:
                logger.info(
                    "%s Dimension mapping validated for registration: from=%s to=%s",
                    self._log_dry_run_mode_prefix(),
                    mapping.from_dimension.dimension_id,
                    mapping.to_dimension.dimension_id,
                )
            return

        for mapping in config.model.mappings:
            from_id = mapping.from_dimension.dimension_id
            to_id = mapping.to_dimension.dimension_id
            if not self.dimension_manager.has_id(from_id):
                raise DSGValueNotRegistered(f"from_dimension ID {from_id} is not registered")
            if not self.dimension_manager.has_id(to_id):
                raise DSGValueNotRegistered(f"to_dimension ID {to_id} is not registered")

            registry_model = DimensionMappingRegistryModel(
                dimension_mapping_id=mapping.mapping_id,
                version=registration.version,
                description=mapping.description.strip(),
                registration_history=[registration],
            )
            registry_config = DimensionMappingRegistry(registry_model)
            dst_dir = self._path / mapping.mapping_id / str(registration.version)
            self.fs_interface.mkdir(dst_dir)

            registry_file = Path(os.path.dirname(dst_dir)) / REGISTRY_FILENAME
            registry_config.serialize(registry_file, force=True)

            at_config = AssociationTableConfig(mapping)
            at_config.src_dir = src_dir
            at_config.serialize(dst_dir)
            self._id_to_type[mapping.mapping_id] = [
                mapping.from_dimension.dimension_type,
                mapping.to_dimension.dimension_type,
            ]
            self._update_registry_cache(mapping.mapping_id, registry_config)
            logger.info(
                "%s Registered dimension mapping id=%s version=%s",
                self._log_offline_mode_prefix(),
                mapping.mapping_id,
                registration.version,
            )

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

    def dump(self, config_id, directory, version=None, force=False):
        path = Path(directory)
        os.makedirs(path, exist_ok=True)
        config = self.get_by_id(config_id, version)
        config.serialize(path, force=force)

        if version is None:
            version = self._registry_configs[config_id].version
        logger.info(
            "Dumped dimension mapping for type=%s ID=%s version=%s to %s",
            self.name(),
            config_id,
            version,
            path,
        )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = AssociationTableConfig.load(config_file)
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(None)
        with self.cloud_interface.make_lock_file(lock_file_path):
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        registry = self.get_registry_config(config.config_id)
        old_key = ConfigKey(config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.config_id, version)
        self._mappings.pop(old_key, None)
        self._mappings[new_key] = config
        return version

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._mappings if x.id == config_id]:
            self._mappings.pop(key)
            self._id_to_type.pop(key.id, None)

    def show(self, filters=None):
        if filters:
            logger.info("List registered dimension_mappings for: %s", filters)

        table = PrettyTable(title="Dimension Mappings")
        table.field_names = (
            "Type [From, To]",
            "ID",
            "Version",
            "Registration Date",
            "Submitter",
            "Description",
        )
        table._max_width = {
            "ID": 50,
            "Description": 50,
        }
        # table.max_width = 70

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for dimension_id, registry_config in self._registry_configs.items():
            reg_dim_type = [x.value for x in self._id_to_type[dimension_id]]

            last_reg = registry_config.model.registration_history[0]

            row = (
                "[" + ", ".join(reg_dim_type) + "]",  # turn list into str
                dimension_id,
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
