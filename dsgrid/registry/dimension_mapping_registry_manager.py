"""Manages the registry for dimension mappings"""

import getpass
import logging
import os
from pathlib import Path
from collections import Counter
from typing import Union, List, Dict

from prettytable import PrettyTable
import pyspark.sql.functions as F

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.mapping_tables import MappingTableConfig
from dsgrid.config.dimension_mappings_config import DimensionMappingsConfig
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel
from dsgrid.exceptions import (
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.spark import models_to_dataframe
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import display_table
from dsgrid.config.dimension_mapping_base import DimensionMappingArchetype
from .common import ConfigKey, make_initial_config_registration, ConfigKey, RegistryType
from .registration_context import RegistrationContext
from .dimension_mapping_registry import DimensionMappingRegistry, DimensionMappingRegistryModel
from .dimension_mapping_update_checker import DimensionMappingUpdateChecker
from .dimension_registry_manager import DimensionRegistryManager
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionMappingRegistryManager(RegistryManagerBase):
    """Manages registered dimension mappings."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._mappings = {}  # ConfigKey to DimensionMappingConfig
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
        """
        Check that records in mappings are subsets of from and to dimension records.
        """
        for mapping in config.model.mappings:
            actual_from_records = {x.from_id for x in mapping.records}
            from_dimension = self._dimension_mgr.get_by_id(mapping.from_dimension.dimension_id)
            allowed_from_records = from_dimension.get_unique_ids()
            diff = actual_from_records.difference(allowed_from_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                raise DSGInvalidDimensionMapping(
                    f"Dimension mapping={mapping.filename} has invalid 'from_id' records: {diff}, "
                    f"they are missing from dimension_id={dim_id}"
                )

            # Note: this code cannot complete verify 'to' records. A dataset may be registering a
            # mapping to a project's dimension for a specific data source, but that information
            # is not available here.
            actual_to_records = {x.to_id for x in mapping.records}
            to_dimension = self._dimension_mgr.get_by_id(mapping.to_dimension.dimension_id)
            allowed_to_records = to_dimension.get_unique_ids()
            if None in actual_to_records:
                actual_to_records.remove(None)
            diff = actual_to_records.difference(allowed_to_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                raise DSGInvalidDimensionMapping(
                    f"Dimension mapping={mapping.filename} has invalid 'to_id' records: {diff}, "
                    f"they are missing from dimension_id={dim_id}"
                )

    def validate_records(self, config: DimensionMappingsConfig):
        """Validate dimension mapping records.

        Check:
        - duplicate records in from_id and to_id columns per mapping archetype
        - sum of from_fraction by from_id per mapping archetype
        - special check for mapping_type=duplication

        """
        for mapping in config.model.mappings:
            actual_from_records = [x.from_id for x in mapping.records]
            self._check_for_duplicates_in_list(
                actual_from_records,
                mapping.archetype.allow_dup_from_records,
                "from_id",
                mapping.filename,
                mapping.mapping_type.value,
            )
            actual_to_records = [x.to_id for x in mapping.records if x.to_id is not None]

            self._check_for_duplicates_in_list(
                actual_to_records,
                mapping.archetype.allow_dup_to_records,
                "to_id",
                mapping.filename,
                mapping.mapping_type.value,
            )

            if mapping.archetype.check_fraction_sum_eq1:
                self._check_fraction_sum(
                    mapping.records, mapping.filename, mapping.mapping_type.value
                )

            if mapping.mapping_type.value == "duplication":
                fractions = {x.from_fraction for x in mapping.records}
                if not (len(fractions) == 1 and 1 in fractions):
                    raise DSGInvalidDimensionMapping(
                        f"dimension_mapping={mapping.filename} has mapping_type={mapping.mapping_type.value}, "
                        f"which does not allow non-one from_fractions. "
                        "\nConsider removing from_fraction column or using mapping_type: 'one_to_many_explicit_multipliers'. "
                    )

    @staticmethod
    def _check_for_duplicates_in_list(
        lst: list, allow_dup: bool, id_type: str, mapping_name: str, mapping_type: str
    ):
        """Check list for duplicates"""
        dups = [x for x, n in Counter(lst).items() if n > 1]
        if len(dups) > 0 and not allow_dup:
            raise DSGInvalidDimensionMapping(
                f"dimension_mapping={mapping_name} has mapping_type={mapping_type}, "
                f"which does not allow duplicated {id_type} records. \nDuplicated {id_type}={dups}. "
            )

    @staticmethod
    def _check_fraction_sum(mapping_records, mapping_name, mapping_type):
        mapping_df = models_to_dataframe(mapping_records)
        mapping_sum_df = (
            mapping_df.groupBy("from_id")
            .agg(F.sum("from_fraction").alias("sum_fraction"))
            .sort(F.desc("sum_fraction"), "from_id")
        )
        fracs_greater_than_one = mapping_sum_df.filter(F.col("sum_fraction") > 1)
        fracs_less_than_one = mapping_sum_df.filter(F.col("sum_fraction") < 1)
        if fracs_greater_than_one.count() > 0:
            id_greater_than_one = {
                x.from_id for x in fracs_greater_than_one[["from_id"]].distinct().collect()
            }
            raise DSGInvalidDimensionMapping(
                f"dimension_mapping={mapping_name} has mapping_type={mapping_type}, which does not allow from_fraction sum <> 1. "
                f"Mapping contains from_fraction sum greater than 1 for from_id={id_greater_than_one}. "
            )
        elif fracs_less_than_one.count() > 0:
            id_less_than_one = {
                x.from_id for x in fracs_less_than_one[["from_id"]].distinct().collect()
            }
            raise DSGInvalidDimensionMapping(
                f"{mapping_name} has mapping_type={mapping_type}, which does not allow from_fraction sum <> 1. "
                f"Mapping contains from_fraction sum less than 1 for from_id={id_less_than_one}. "
            )

    def finalize_registration(self, config_ids, error_occurred):
        if error_occurred:
            logger.info("Remove all intermediate dimension mappings after error")
            for mapping_id in config_ids:
                self.remove(mapping_id)

        if not self.offline_mode:
            lock_file = self.get_registry_lock_file(None)
            self.cloud_interface.check_lock_file(lock_file)
            # Sync the entire dimension mapping registry path because it's probably cheaper
            # than syncing each changed path individually.
            if not error_occurred:
                self.sync_push(self._path)
            self.cloud_interface.remove_lock_file(lock_file)

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
        config = MappingTableConfig.load(filename)
        self._mappings[key] = config
        return config

    def acquire_registry_locks(self, config_ids: List[str]):
        self.cloud_interface.make_lock_file(self.get_registry_lock_file(None))

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
            ConfigKey to DimensionMappingConfig

        """
        mappings = {}
        for ref in dimension_mapping_references:
            key = ConfigKey(ref.mapping_id, ref.version)
            mappings[key] = self.get_by_key(key)

        return mappings

    def make_dimension_mapping_references(self, mapping_ids: List[str]):
        """Return a list of dimension mapping references from a list of registered mapping IDs.

        Parameters
        ----------
        mapping_ids : List[str]

        Returns
        -------
        List[DimensionMappingReferenceModel]

        """
        refs = []
        for mapping_id in mapping_ids:
            mapping = self.get_by_id(mapping_id)
            refs.append(
                DimensionMappingReferenceModel(
                    from_dimension_type=mapping.model.from_dimension.dimension_type,
                    to_dimension_type=mapping.model.to_dimension.dimension_type,
                    mapping_id=mapping_id,
                    version=self.get_current_version(mapping_id),
                )
            )
        return refs

    def register_from_file(self, config_file, submitter, log_message, force=False):
        context = RegistrationContext()
        config = DimensionMappingsConfig.load(config_file)
        return self.register(config, submitter, log_message, force=force, context=context)

    @track_timing(timer_stats_collector)
    def register(
        self, config: DimensionMappingsConfig, submitter, log_message, force=False, context=None
    ):
        error_occurred = False
        need_to_finalize = context is None
        if context is None:
            context = RegistrationContext()

        try:
            return self._register(config, submitter, log_message, context, force=force)
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    def _register(self, config, submitter, log_message, context, force=False):
        config.assign_ids()
        self._check_unique_records(config, warn_only=force)
        self._check_records_against_dimension_records(config)
        self.validate_records(config)

        registration = make_initial_config_registration(submitter, log_message)
        dimension_mapping_ids = []
        try:
            # Guarantee that registration of dimension mappings is all or none.
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
                registry_config.serialize(registry_file, force=force)

                at_config = MappingTableConfig(mapping)
                at_config.src_dir = config.src_dir
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
                dimension_mapping_ids.append(mapping.mapping_id)
        except Exception:
            if dimension_mapping_ids:
                logger.warning(
                    "Exception occured after partial completion of dimension mapping registration."
                )
                for mapping_id in dimension_mapping_ids:
                    self.remove(mapping_id)
            raise

        logger.info(
            "%s Registered %s dimension mapping(s) with version=%s",
            self._log_offline_mode_prefix(),
            len(config.model.mappings),
            registration.version,
        )

        context.add_ids(RegistryType.DIMENSION_MAPPING, dimension_mapping_ids, self)
        return dimension_mapping_ids

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
        config = MappingTableConfig.load(config_file)
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    @track_timing(timer_stats_collector)
    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(None)
        with self.cloud_interface.make_lock_file_managed(lock_file_path):
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        old_config = self.get_by_id(config.config_id)
        checker = DimensionMappingUpdateChecker(old_config.model, config.model)
        checker.run()
        registry = self.get_registry_config(config.config_id)
        old_key = ConfigKey(config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.config_id, version)
        self._mappings.pop(old_key, None)
        self._mappings[new_key] = config

        if not self.offline_mode:
            self.sync_push(self._path)

        return version

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._mappings if x.id == config_id]:
            self._mappings.pop(key)
            self._id_to_type.pop(key.id, None)

    def show(
        self,
        filters: List[str] = None,
        max_width: Union[int, Dict] = None,
        drop_fields: List[str] = None,
        **kwargs,
    ):
        """Show registry in PrettyTable

        Parameters
        ----------
        filters : list or tuple
            List of filter expressions for reigstry content (e.g., filters=["Submitter==USER", "Description contains comstock"])
        max_width
            Max column width in PrettyTable, specify as a single value or as a dict of values by field name
        drop_fields
            List of field names not to show

        """

        if filters:
            logger.info("List registered dimension_mappings for: %s", filters)

        table = PrettyTable(title="Dimension Mappings")
        all_field_names = (
            "Type [From, To]",
            "ID",
            "Version",
            "Date",
            "Submitter",
            "Description",
        )
        if drop_fields is None:
            table.field_names = all_field_names
        else:
            table.field_names = tuple(x for x in all_field_names if x not in drop_fields)

        if max_width is None:
            table._max_width = {
                "ID": 34,
                "Date": 10,
                "Description": 34,
            }
        if isinstance(max_width, int):
            table.max_width = max_width
        elif isinstance(max_width, dict):
            table._max_width = max_width

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for dimension_id, registry_config in self._registry_configs.items():
            reg_dim_type = [x.value for x in self._id_to_type[dimension_id]]
            last_reg = registry_config.model.registration_history[0]

            all_fields = (
                "[" + ", ".join(reg_dim_type) + "]",  # turn list into str
                dimension_id,
                last_reg.version,
                last_reg.date.strftime("%Y-%m-%d %H:%M:%S"),
                last_reg.submitter,
                registry_config.model.description,
            )
            if drop_fields is None:
                row = all_fields
            else:
                row = tuple(
                    y for (x, y) in zip(all_field_names, all_fields) if x not in drop_fields
                )

            if not filters or matches_filters(row, field_to_index, transformed_filters):
                rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        table.align = "l"
        display_table(table)
