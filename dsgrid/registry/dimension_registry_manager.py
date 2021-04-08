import logging
import os
from collections import defaultdict, namedtuple
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union


import toml
from pydantic.fields import Field, Required
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo

from dsgrid.dimension.base import DimensionType
from dsgrid.dimension.models import DimensionModel, TimeDimensionModel, DimensionReferenceModel
from dsgrid.exceptions import DSGValueNotStored
from dsgrid.models import DSGBaseModel, serialize_model
from dsgrid.config._config import ConfigRegistrationModel
from dsgrid.registry.common import (
    make_filename_from_version,
    make_version,
    get_version_from_filename,
    RegistryType,
)
from dsgrid.registry.registry_manager_base import RegistryManagerBase
from dsgrid.utils.files import load_data, dump_data
import dsgrid.utils.aws as aws


logger = logging.getLogger(__name__)

DimensionKey = namedtuple("DimensionKey", ["type", "id", "version"])


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path):
        super().__init__(path)
        self._dimensions = {}  # key = (dimension_type, dimension_id, version)
        # value = DimensionBaseModel
        self._dimension_versions = defaultdict(dict)
        self._dimensions = {}  # key = DimensionKey, value = Dimension
        if self._on_aws:
            assert False
            # for dim_type in aws.list_dir_in_bucket(self._bucket, self._path):
            #    _type = DimensionType(dim_type)
            #    path = Path(self._path) / self.DIMENSION_REGISTRY_PATH / dim_type
            #    self._dimension_ids_by_type[_type] = set(aws.list_dir_in_bucket(self._bucket, path))
        else:
            for dim_type in os.listdir(self._path):
                _type = DimensionType(dim_type)
                type_path = Path(self._path) / dim_type
                ids = os.listdir(type_path)
                for dim_id in ids:
                    dim_path = type_path / dim_id
                    self._dimension_versions[_type][dim_id] = {
                        VersionInfo.parse(x)
                        for x in os.listdir(dim_path)
                        if os.path.isdir(dim_path / x)
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
            Raised if the dimension is not store.

        """
        key = DimensionKey(dimension_type, dimension_id, version)
        if not self.has_dimension_id(key):
            raise DSGValueNotStored(f"dimension not stored: {key}")

        if dimension_type == DimensionType.TIME:
            cls = TimeDimensionModel
        else:
            cls = DimensionModel
        filename = (
            self._path / dimension_type.value / dimension_id / str(version) / "dimension.toml"
        )
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

    def replace_dimension_references(self, dimensions):
        """Replace any dimension references with actual dimension objects read from disk.

        Parameters
        ----------
        dimensions : list
            list of DSGBaseModel instances to be modified in place

        """
        for i, dim in enumerate(dimensions):
            if isinstance(dim, DimensionReferenceModel):
                dimensions[i] = self.get_dimension(
                    dim.dimension_type, dim.dimension_id, dim.version
                )
