"""Manages the registry for dimensions"""

import getpass
import logging
import os
from pathlib import Path
from typing import Union, List, Dict, Set

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dimension_config_factory import get_dimension_config, load_dimension_config
from dsgrid.config.dimensions_config import DimensionsConfig
from dsgrid.config.dimensions import (
    DimensionType,
    TimeDimensionBaseModel,
    DimensionReferenceModel,
)
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.registry.common import DimensionKey, make_initial_config_registration
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import display_table
from .common import RegistryType
from .registration_context import RegistrationContext
from .dimension_update_checker import DimensionUpdateChecker
from .dimension_registry import DimensionRegistry, DimensionRegistryModel
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._dimensions = {}  # key = DimensionKey, value = DimensionConfig
        self._id_to_type = {}  # key = str, value = DimensionType

    def inventory(self):
        for dim_type in self.fs_interface.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            _type = DimensionType(dim_type)
            type_path = self._path / dim_type
            ids = self.fs_interface.listdir(type_path, directories_only=True, exclude_hidden=True)
            for dim_id in ids:
                dim_path = type_path / dim_id
                registry = self.registry_class().load(dim_path / REGISTRY_FILENAME)
                self._registry_configs[dim_id] = registry
                self._id_to_type[dim_id] = _type

    @staticmethod
    def name():
        return "Dimensions"

    @staticmethod
    def registry_class():
        return DimensionRegistry

    def _replace_duplicates(self, config: DimensionsConfig):
        hashes = {}
        time_dims = {}
        for dimension_id, registry_config in self._registry_configs.items():
            dimension = self.get_by_id(dimension_id, registry_config.model.version)
            if isinstance(dimension.model, TimeDimensionBaseModel):
                time_dims[dimension.model.dimension_id] = dimension.model
            else:
                hashes[dimension.model.file_hash] = dimension.model

        existing_ids = set()
        for i, dim in enumerate(config.model.dimensions):
            replace_dim = False
            existing = None
            if isinstance(dim, TimeDimensionBaseModel):
                existing = self._get_matching_time_dimension(time_dims.values(), dim)
                if existing is not None:
                    replace_dim = True
            elif dim.file_hash in hashes:
                existing = hashes[dim.file_hash]
                if dim.dimension_type == existing.dimension_type:
                    if dim.name == existing.name and dim.display_name == existing.display_name:
                        replace_dim = True
                    else:
                        logger.info(
                            "Register new dimension even though records are duplicate: %s",
                            dim.name,
                        )
            if replace_dim:
                logger.info(
                    "Replace %s with existing dimension %s", dim.name, existing.dimension_id
                )
                config.model.dimensions[i] = existing
                existing_ids.add(existing.dimension_id)
        return existing_ids

    @staticmethod
    def _get_matching_time_dimension(existing_dims, new_dim):
        for time_dim in existing_dims:
            if type(time_dim) != type(new_dim):
                continue
            match = True
            for field in type(new_dim).__fields__:
                exclude = set(("description", "dimension_id"))
                if field not in exclude and getattr(new_dim, field) != getattr(time_dim, field):
                    match = False
                    break
            if match:
                return time_dim

        return None

    def finalize_registration(self, config_ids: List[str], error_occurred: bool):
        if error_occurred:
            logger.info("Remove all intermediate dimensions after error")
            for dimension_id in config_ids:
                self.remove(dimension_id)

        if not self.offline_mode:
            lock_file = self.get_registry_lock_file(None)
            self.cloud_interface.check_lock_file(lock_file)
            # Sync the entire dimension registry path because it's probably cheaper
            # than syncing each changed path individually.
            if not error_occurred:
                self.sync_push(self._path)
            self.cloud_interface.remove_lock_file(lock_file)

    def get_by_id(self, config_id, version=None):
        self._check_if_not_registered(config_id)
        dimension_type = self._id_to_type[config_id]
        if version is None:
            version = self._registry_configs[config_id].model.version
        key = DimensionKey(dimension_type, config_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id):
            raise DSGValueNotRegistered(f"{key}")

        dimension = self._dimensions.get(key)
        if dimension is not None:
            return dimension

        src_dir = self._path / key.type.value / key.id / key.version
        filename = src_dir / self.registry_class().config_filename()
        config = load_dimension_config(filename)
        self._dimensions[key] = config
        return config

    def acquire_registry_locks(self, config_ids: List[str]):
        self.cloud_interface.make_lock_file(self.get_registry_lock_file(None))

    def get_registry_lock_file(self, config_id):
        return "configs/.locks/dimensions.lock"

    def has_id(self, config_id, version=None):
        if version is None:
            return config_id in self._registry_configs
        dimension_type = self._id_to_type[config_id]
        path = self._path / str(dimension_type) / config_id / version
        return self.fs_interface.exists(path)

    def list_types(self):
        """Return the dimension types present in the registry."""
        return {self._id_to_type[x] for x in self._registry_configs}

    def list_ids(self, dimension_type=None):
        """Return the dimension ids for the given type.

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        list

        """
        if dimension_type is None:
            return super().list_ids()

        return sorted((x for x in self._registry_configs if self._id_to_type[x] == dimension_type))

    def load_dimensions(self, dimension_references):
        """Load dimensions from files.

        Parameters
        ----------
        dimension_references : list
            iterable of DimensionReferenceModel instances


        Returns
        -------
        dict
            DimensionKey to DimensionConfig

        """
        dimensions = {}
        for dim in dimension_references:
            key = DimensionKey(dim.dimension_type, dim.dimension_id, dim.version)
            dimensions[key] = self.get_by_key(key)

        return dimensions

    @track_timing(timer_stats_collector)
    def register_from_config(
        self, config: DimensionsConfig, submitter, log_message, force=False, context=None
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

    @track_timing(timer_stats_collector)
    def register(self, config_file, submitter, log_message, force=False):
        context = RegistrationContext()
        config = DimensionsConfig.load(config_file)
        return self.register_from_config(
            config, submitter, log_message, force=force, context=context
        )

    def _register(self, config, submitter, log_message, context, force=False):
        config.assign_ids()
        existing_ids = self._replace_duplicates(config)
        # TODO: check that id does not already exist in .dsgrid-registry

        registration = make_initial_config_registration(submitter, log_message)

        dimension_ids = []
        try:
            # Guarantee that registration of dimensions is all or none.
            for dimension in config.model.dimensions:
                if dimension.dimension_id in existing_ids:
                    continue
                registry_model = DimensionRegistryModel(
                    dimension_id=dimension.dimension_id,
                    version=registration.version,
                    description=dimension.description.strip(),
                    registration_history=[registration],
                )
                registry_config = DimensionRegistry(registry_model)
                dst_dir = (
                    self._path
                    / dimension.dimension_type.value
                    / dimension.dimension_id
                    / registration.version
                )
                self.fs_interface.mkdir(dst_dir)

                registry_file = Path(os.path.dirname(dst_dir)) / REGISTRY_FILENAME
                registry_config.serialize(registry_file, force=force)

                dimension_config = get_dimension_config(dimension, config.src_dir)
                dimension_config.serialize(dst_dir)
                self._id_to_type[dimension.dimension_id] = dimension.dimension_type
                self._update_registry_cache(dimension.dimension_id, registry_config)
                logger.info(
                    "%s Registered dimension id=%s type=%s version=%s name=%s",
                    self._log_offline_mode_prefix(),
                    dimension.dimension_id,
                    dimension.dimension_type.value,
                    registration.version,
                    dimension.name,
                )
                dimension_ids.append(dimension.dimension_id)
        except Exception:
            if dimension_ids:
                logger.warning(
                    "Exception occured after partial completion of dimension registration."
                )
                for dimension_id in dimension_ids:
                    self.remove(dimension_id)
            raise

        logger.info(
            "Registered %s dimensions with version=%s",
            len(config.model.dimensions),
            registration.version,
        )

        context.add_ids(RegistryType.DIMENSION, dimension_ids, self)
        dimension_ids.extend(existing_ids)
        return dimension_ids

    def get_registry_directory(self, config_id):
        dimension_type = self._id_to_type[config_id]
        return self._path / dimension_type.value / config_id

    def make_dimension_references(self, dimension_ids: List[str]):
        """Return a list of dimension references from a list of registered dimension IDs.

        Parameters
        ----------
        dimension_ids : List[str]

        """
        refs = []
        for dim_id in dimension_ids:
            dim = self.get_by_id(dim_id)
            refs.append(
                DimensionReferenceModel(
                    dimension_id=dim_id,
                    dimension_type=dim.model.dimension_type,
                    version=self.get_current_version(dim_id),
                )
            )
        return refs

    def show(
        self,
        filters: List[str] = None,
        max_width: Union[int, Dict] = None,
        drop_fields: List[str] = None,
        dimension_ids: Set[str] = None,
        return_table: bool = False,
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
            logger.info("List registered dimensions for: %s", filters)

        table = PrettyTable(title="Dimensions")
        all_field_names = (
            "Type",
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
                "ID": 40,
                "Date": 10,
                "Description": 40,
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
            if dimension_ids and dimension_id not in dimension_ids:
                continue
            reg_dim_type = self._id_to_type[dimension_id].value
            last_reg = registry_config.model.registration_history[0]

            all_fields = (
                reg_dim_type,
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
        if return_table:
            return table
        display_table(table)

    def dump(self, config_id, directory, version=None, force=False):
        path = Path(directory)
        os.makedirs(path, exist_ok=True)
        config = self.get_by_id(config_id, version)
        config.serialize(path, force=force)

        if version is None:
            version = self._registry_configs[config_id].version
        logger.info(
            "Dumped dimension for type=%s ID=%s version=%s to %s",
            self.name(),
            config_id,
            version,
            path,
        )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = load_dimension_config(config_file)
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
        checker = DimensionUpdateChecker(old_config.model, config.model)
        checker.run()
        registry = self.get_registry_config(config.config_id)
        old_key = DimensionKey(config.model.dimension_type, config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = DimensionKey(config.model.dimension_type, config.config_id, version)
        self._dimensions.pop(old_key, None)
        self._dimensions[new_key] = config

        if not self.offline_mode:
            self.sync_push(self._path)

        return version

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._dimensions if x.id == config_id]:
            self._dimensions.pop(key)
            self._id_to_type.pop(key.id, None)
