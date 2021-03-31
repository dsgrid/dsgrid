import os
from enum import Enum
import logging
from pathlib import Path
from typing import List, Optional, Union


import toml
from pydantic.fields import Field, Required
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo

from dsgrid.dimension.base import DSGBaseModel, serialize_model

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config._config import ConfigRegistrationModel
from dsgrid.registry.common import (
    make_filename_from_version,
    make_version,
    get_version_from_filename,
    RegistryType,
)
from dsgrid.utils.files import load_data, dump_data


logger = logging.getLogger(__name__)


# TODO: I think we need to also register the pydantic models associated with
#   the configuration OR we need to set the dataset_config type to
#   DatasetCOnfig instead of dict of the loaded toml (not the cls.dict()).


class DatasetRegistry:

    DATASET_REGISTRY_PATH = Path("registry/datasets")

    """Controls dataset registration"""

    def __init__(self, model):
        """Construct DatasetRegistry

        Parameters
        ----------
        model : DatasetRegistryModel

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
        DatasetRegistry

        """
        return cls(DatasetRegistryModel(**load_data(config_file)))

    @property
    def dataset_id(self):
        return self._model.dataset_id

    @property
    def registration(self):
        return self._model.registration

    @property
    def version(self):
        return self._model.version

    @version.setter
    def version(self, val):
        self._model.dataset_version = val


class DatasetRegistryModel(DSGBaseModel):
    """Dataset registration class"""

    dataset_id: str = Field(
        title="dataset_id", description="dataset identifier",
    )
    version: Union[str, VersionInfo] = Field(
        title="version", description="dataset version",
    )
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
