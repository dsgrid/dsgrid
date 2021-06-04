"""Manages the registry for dimensions"""

import logging
import os
from pathlib import Path

from prettytable import PrettyTable

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dimension_config import (
    DimensionConfig,
    get_dimension_config,
    load_dimension_config,
)
from dsgrid.config.dimensions_config import DimensionsConfig
from dsgrid.config.dimensions import (
    DimensionType,
    DimensionBaseModel,
    DimensionModel,
    TimeDimensionModel,
)
from dsgrid.data_models import serialize_model
from dsgrid.exceptions import (
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
    DSGInvalidParameter,
)
from dsgrid.registry.common import (
    DimensionKey,
    make_initial_config_registration,
)
from .registry_manager_base import RegistryManagerBase
from .dimension_registry import DimensionRegistry, DimensionRegistryModel


logger = logging.getLogger(__name__)


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._dimensions = {}  # key = DimensionKey, value = DimensionConfig
        self._id_to_type = {}

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

    def check_unique_records(self, config: DimensionsConfig, warn_only=False):
        """Check if any new dimension record files have identical contents as any existing files.

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
        for dimension_id, registry_config in self._registry_configs.items():
            dimension = self.get_by_id(dimension_id, registry_config.model.version)
            if isinstance(dimension.model, TimeDimensionModel):
                continue
            hashes[dimension.model.file_hash] = dimension_id

        duplicates = []
        for dimension in config.model.dimensions:
            if not isinstance(dimension, TimeDimensionModel) and dimension.file_hash in hashes:
                duplicates.append((dimension.dimension_id, hashes[dimension.file_hash]))

        if duplicates:
            for dup in duplicates:
                logger.error(
                    "%s has duplicate content with existing dimension ID %s", dup[0], dup[1]
                )
            if not warn_only:
                raise DSGDuplicateValueRegistered(
                    f"There are {len(duplicates)} duplicate dimension mapping records."
                )

    def get_by_id(self, config_id, version=None, force=False):
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

        if key.type == DimensionType.TIME:
            cls = TimeDimensionModel
        else:
            cls = DimensionModel
        src_dir = self._path / key.type.value / key.id / str(key.version)
        filename = src_dir / self.registry_class().config_filename()
        model = cls.load(filename)
        config = get_dimension_config(model, src_dir)
        self._dimensions[key] = config
        return config

    def get_registry_lock_file(self, config_id):
        return "configs/.locks/dimensions.lock"

    def has_id(self, config_id, version=None):
        if version is None:
            return config_id in self._registry_configs
        dimension_type = self._id_to_type[config_id]
        path = self._path / str(dimension_type) / config_id / str(version)
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

        """
        dimensions = {}
        for dim in dimension_references:
            key = DimensionKey(dim.dimension_type, dim.dimension_id, dim.version)
            dimensions[key] = self.get_by_key(key)

        return dimensions

    def register(self, config_file, submitter, log_message, force=False):
        lock_file_path = self.get_registry_lock_file(None)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._register(config_file, submitter, log_message, force=force)

    def _register(self, config_file, submitter, log_message, force=False):
        config = DimensionsConfig.load(config_file)
        config.assign_ids()
        self.check_unique_records(config, warn_only=force)
        # TODO: check that id does not already exist in .dsgrid-registry

        registration = make_initial_config_registration(submitter, log_message)
        src_dir = Path(os.path.dirname(config_file))

        if self.dry_run_mode:
            for dimension in config.model.dimensions:
                logger.info(
                    "%s Dimension validated for registration: type=%s name=%s",
                    self._log_dry_run_mode_prefix(),
                    dimension.dimension_type.value,
                    dimension.name,
                )
            return

        for dimension in config.model.dimensions:
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
                / str(registration.version)
            )
            self.fs_interface.mkdir(dst_dir)

            registry_file = Path(os.path.dirname(dst_dir)) / REGISTRY_FILENAME
            registry_config.serialize(registry_file, force=True)

            dimension_config = get_dimension_config(dimension, src_dir)
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

        if not self.offline_mode:
            # Sync the entire dimension registry path because it's probably cheaper
            # than syncing each changed path individually.
            self.sync_push(self._path)

        logger.info(
            "Registered %s dimensions with version=%s",
            len(config.model.dimensions),
            registration.version,
        )

    def get_registry_directory(self, config_id):
        dimension_type = self._id_to_type[config_id]
        return self._path / dimension_type.value / config_id

    def show(self, dimension_type=None, submitter=None):
        # TODO: filter by type and submitter
        table = PrettyTable(title="Dimensions")
        table.field_names = (
            "Type",
            "ID",
            "Version",
            "Registration Date",
            "Submitter",
            "Description",
        )
        rows = []
        for dimension_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[-1]
            row = (
                self._id_to_type[dimension_id].value,
                dimension_id,
                last_reg.version,
                last_reg.date,
                last_reg.submitter,
                registry_config.model.description,
            )
            rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)

        print(table)

    def dump(self, config_id, directory, version=None, force=False):
        config = self.get_by_id(config_id, version)
        config.serialize(directory, force=force)

        if version is None:
            version = self._registry_configs[config_id].version
        logger.info(
            "Dumped dimension for type=%s ID=%s version=%s to %s",
            self.name(),
            config_id,
            version,
            directory,
        )

    def update(self, config_file, config_id, submitter, update_type, log_message, version):
        config = load_dimension_config(config_file)
        self._check_update(config, config_id, version)

        lock_file_path = self.get_registry_lock_file(None)
        with self.cloud_interface.make_lock_file(lock_file_path):
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        registry = self.get_registry_config(config.config_id)
        old_key = DimensionKey(config.config_id, config.model.dimension_type, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = DimensionKey(config.config_id, config.model.dimension_type, version)
        self._dimensions.pop(old_key, None)
        self._dimensions[new_key] = config
        return version
