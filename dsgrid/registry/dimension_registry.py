import os
from enum import Enum
import logging
from pathlib import Path
from typing import List, Optional, Union


import toml
from pydantic.fields import Field, Required
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo

from dsgrid.models import DSGBaseModel, serialize_model
from dsgrid.config._config import ConfigRegistrationModel
from dsgrid.registry.common import (
    make_filename_from_version,
    make_version,
    get_version_from_filename,
    RegistryType,
)
from dsgrid.utils.files import load_data, dump_data


logger = logging.getLogger(__name__)


class DimensionRegistry:

    DIMENSION_REGISTRY_PATH = Path("registry/dimensions")

    """Controls dimension (record) registration from datasets and projects"""

    def __init__(self, model):
        """Construct DimensionRegistry

        Parameters
        ----------
        model : DimensionRegistryModel

        """
        self._model = model

    @classmethod
    def load(cls, config_file):
        """Load the registry from a file.

        Parameters
        ----------
        config_file : str
            path to the file

        Returns
        -------
        DimensionRegistry

        """
        return cls(DimensionRegistryModel(**load_data(config_file)))

    @property
    def registration(self):
        return self._model.registration

    @property
    def version(self):
        return self._model.version

    @version.setter
    def version(self, val):
        self._model.dimension_version = val


class DimensionRegistryModel(DSGBaseModel):
    """dimension (record) registration class"""

    version: Union[str, VersionInfo] = Field(
        title="version",
        description="dimension version",
    )
    description: str = Field(title="description", description="detail on dimension record")
    registration_history: Optional[List[ConfigRegistrationModel]] = Field(
        title="registration_history",
        description="history of all registration updates",
        default=[],
    )

    @validator("version")
    def check_version(cls, version):
        if isinstance(version, str):
            return make_version(version)
        return version
