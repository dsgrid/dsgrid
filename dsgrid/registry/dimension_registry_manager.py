"""Manages the registry for dimensions"""

import logging
import os
from collections import defaultdict
from pathlib import Path

from semver import VersionInfo

from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.dimensions import (
    DimensionType,
    DimensionModel,
    TimeDimensionModel,
    serialize_dimension_model,
)
from dsgrid.data_models import serialize_model
from dsgrid.exceptions import DSGValueNotRegistered, DSGDuplicateValueRegistered
from dsgrid.registry.common import (
    DimensionKey,
    make_initial_config_registration,
)
from dsgrid.utils.files import dump_data, load_data
from .registry_base import RegistryBaseModel
from .registry_manager_base import RegistryManagerBase
from .dimension_registry import DimensionRegistry


logger = logging.getLogger(__name__)


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._dimensions = {}  # key = (dimension_type, dimension_id, version)
        # value = DimensionBaseModel
        self._dimensions = {}  # key = DimensionKey, value = Dimension
        self._id_to_type = {}

    def inventory(self):
        for dim_type in self._fs_intf.listdir(
            self._path, directories_only=True, exclude_hidden=True
        ):
            _type = DimensionType(dim_type)
            type_path = self._path / dim_type
            ids = self._fs_intf.listdir(type_path, directories_only=True, exclude_hidden=True)
            for dim_id in ids:
                dim_path = type_path / dim_id
                registry = self.registry_class().load(dim_path / REGISTRY_FILENAME)
                self._current_versions[dim_id] = registry.model.version
                self._id_to_type[dim_id] = _type

    @staticmethod
    def registry_class():
        return DimensionRegistry

    def check_unique_records(self, config: DimensionConfig, warn_only=False):
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
        for dimension_id, version in self._current_versions.items():
            dimension = self.get_by_id(dimension_id, version)
            if isinstance(dimension, TimeDimensionModel):
                continue
            hashes.add(dimension.file_hash)

        duplicates = [
            x.dimension_id
            for x in config.model.dimensions
            if not isinstance(x, TimeDimensionModel) and x.file_hash in hashes
        ]
        if duplicates:
            if warn_only:
                logger.warning("Dimension records are duplicated: %s", duplicates)
            else:
                raise DSGDuplicateValueRegistered(f"duplicate dimension records: {duplicates}")

    def get_by_id(self, item_id, version=None, force=False):
        dimension_type = self._id_to_type[item_id]
        if version is None:
            version = sorted(list(self._current_versions[item_id]))[-1]
        key = DimensionKey(dimension_type, item_id, version)
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
        filename = self._path / key.type.value / key.id / str(key.version) / "dimension.toml"
        dimension = cls.load(filename)
        self._dimensions[key] = dimension
        return dimension

    def has_id(self, item_id, version=None):
        if version is None:
            return item_id in self._current_versions
        dimension_type = self._id_to_type[item_id]
        path = self._path / str(dimension_type) / item_id / str(version)
        return self._fs_intf.exists(path)

    def list_types(self):
        """Return the dimension types present in the registry."""
        return [self._id_to_type[x] for x in self._current_versions]

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

        ids = [x for x in self._current_versions if self._id_to_type[x] == dimension_type]
        ids.sort()
        return ids

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
        config = DimensionConfig.load(config_file)
        config.assign_ids()
        self.check_unique_records(config, warn_only=force)
        # TODO: check that id does not already exist in .dsgrid-registry
        # TODO: need regular expression check on name and/or limit number of chars in dim id

        registration = make_initial_config_registration(submitter, log_message)
        dest_config_filename = "dimension" + os.path.splitext(config_file)[1]
        config_dir = Path(os.path.dirname(config_file))

        for dimension in config.model.dimensions:
            registry_config = RegistryBaseModel(
                version=registration.version,
                description=dimension.description.strip(),
                registration_history=[registration],
            )
            dest_dir = (
                self._path
                / dimension.dimension_type.value
                / dimension.dimension_id
                / str(registration.version)
            )
            self._fs_intf.mkdir(dest_dir)

            filename = Path(os.path.dirname(dest_dir)) / REGISTRY_FILENAME
            data = serialize_model(registry_config)
            # TODO: if we want to update AWS directly, this needs to change.
            dump_data(data, filename)

            model_data = serialize_dimension_model(dimension)
            # Time dimensions do not have a record file.
            orig_file = getattr(dimension, "filename", None)
            if orig_file is not None:
                # Leading directories from the original are not relevant in the registry.
                dest_record_file = dest_dir / os.path.basename(orig_file)
                self._fs_intf.copy_file(config_dir / dimension.filename, dest_record_file)
                # We have to make this change in the serialized dict instead of
                # model because Pydantic will fail the assignment due to not being
                # able to find the path.
                model_data["file"] = os.path.basename(dimension.filename)

            dump_data(model_data, dest_dir / dest_config_filename)

            # export dimension record file
            # if orig_file is not None:
            #    dimension_record = Path(os.path.dirname(config_file)) / orig_file
            #    self._fs_intf.copy_file(
            #        dimension_record, data_dir / os.path.basename(dimension.filename)
            #    )

        logger.info(
            "Registered %s dimensions with version=%s",
            len(config.model.dimensions),
            registration.version,
        )

        # save a copy to
        # config_file_updated = self._config_file_extend_name(config_file, "with assigned id")
        # dump_data(serialize_model(config), config_file_updated)

        # logger.info(
        #    "--> New config file containing the dimension ID assignment exported: %s",
        #    config_file_updated,
        # )

    def _config_file_extend_name(self, config_file, name_extension):
        """Add name extension to existing config_file"""
        name_extension = str(name_extension).lower().replace(" ", "_")
        return (
            os.path.splitext(config_file)[0]
            + "_"
            + name_extension
            + os.path.splitext(config_file)[1]
        )
