import logging
import os
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union


import toml
from pydantic.fields import Field, Required
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo

from dsgrid.config.dimensions import (
    DimensionType,
    DimensionModel,
    TimeDimensionModel,
    DimensionReferenceModel,
)
from dsgrid.exceptions import DSGValueNotStored
from dsgrid.data_models import DSGBaseModel, serialize_model
from dsgrid.registry.common import (
    ConfigRegistrationModel,
    DimensionKey,
    make_filename_from_version,
    get_version_from_filename,
    RegistryType,
)
from dsgrid.registry.registry_manager_base import RegistryManagerBase
from dsgrid.utils.files import load_data, dump_data
from dsgrid.utils.versioning import make_version


logger = logging.getLogger(__name__)


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path, fs_interface):
        super().__init__(path, fs_interface)
        self._dimensions = {}  # key = (dimension_type, dimension_id, version)
        # value = DimensionBaseModel
        self._dimension_versions = defaultdict(dict)
        self._dimensions = {}  # key = DimensionKey, value = Dimension
        for dim_type in self._fs_intf.listdir(self._path):
            _type = DimensionType(dim_type)
            type_path = Path(self._path) / dim_type
            ids = self._fs_intf.listdir(type_path)
            for dim_id in ids:
                dim_path = type_path / dim_id
                self._dimension_versions[_type][dim_id] = {
                    VersionInfo.parse(x)
                    for x in self._fs_intf.listdir(dim_path, directories_only=True)
                }

    def get_dimension(self, dimension_type, dimension_id, version):
        """Get the dimension matching the parameters. Returns from cache if already loaded.

        Parameters
        ----------
        dimension_type : DimensionType
        dimension_id : str
        version : VersionInfo

        Returns
        -------
        DimensionBaseModel

        Raises
        ------
        DSGValueNotStored
            Raised if the dimension is not stored.

        """
        key = DimensionKey(dimension_type, dimension_id, version)
        return self.get_dimension_by_key(key)

    def get_dimension_by_key(self, key):
        if not self.has_dimension_id(key):
            raise DSGValueNotStored(f"dimension not stored: {key}")

        if key.type == DimensionType.TIME:
            cls = TimeDimensionModel
        else:
            cls = DimensionModel
        filename = self._path / key.type.value / key.id / str(key.version) / "dimension.toml"
        dimension = cls.load(filename)
        self._dimensions[key] = dimension
        return dimension

    def has_dimension_id(self, key):
        """Return True if a dimension matching the parameters is stored.

        Parameters
        ----------
        key : DimensionKey

        Returns
        -------
        bool

        """
        if (
            key.type in self._dimension_versions
            and key.id in self._dimension_versions[key.type]
            and key.version in self._dimension_versions[key.type][key.id]
        ):
            return True
        return False

    def list_dimension_types(self):
        """Return the dimension types present in the registry."""
        return list(self._dimension_versions.keys())

    def list_dimension_ids(self, dimension_type):
        """Return the dimension ids for the given type.

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        list

        """
        return sorted(list(self._dimension_versions[dimension_type]))

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
            dimensions[key] = self.get_dimension_by_key(key)

        return dimensions