import abc
import logging
from pathlib import Path
from typing import List, Optional, Union


from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from dsgrid.config.config_base import ConfigBase
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import (
    ConfigRegistrationModel,
)
from dsgrid.data_models import serialize_model
from dsgrid.utils.files import dump_data
from dsgrid.utils.versioning import handle_version_or_str


logger = logging.getLogger(__name__)


class RegistryBaseModel(DSGBaseModel):
    """Base class for models that get registered in the registry"""

    version: Union[str, VersionInfo] = Field(
        title="version",
        description="dimension version",
    )
    description: Optional[str] = Field(
        title="description", description="description of what is stored"
    )
    registration_history: Optional[List[ConfigRegistrationModel]] = Field(
        title="registration_history",
        description="history of all registration updates",
        default=[],
    )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)


class RegistryBase(ConfigBase, abc.ABC):
    """Base class for classes that store records in the registry"""

    @staticmethod
    @abc.abstractmethod
    def config_filename():
        """Return the config filename.

        Returns
        -------
        str

        """

    @staticmethod
    @abc.abstractmethod
    def registry_path():
        """Return the path to these records in the registry.

        Returns
        -------
        str

        """

    @property
    def registration_history(self):
        return self._model.registration_history

    def serialize(self, filename):
        dump_data(serialize_model(self._model), filename)

    @property
    def version(self):
        return self._model.version

    @version.setter
    def version(self, val):
        self._model.version = val
