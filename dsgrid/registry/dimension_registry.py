import logging
from pathlib import Path
from typing import List, Optional, Union


from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import (
    ConfigRegistrationModel,
)
from dsgrid.utils.files import load_data
from dsgrid.utils.versioning import handle_version_or_str


logger = logging.getLogger(__name__)


class DimensionRegistry:
    """Controls dimension (record) registration from datasets and projects"""

    DIMENSION_REGISTRY_PATH = Path("registry/dimensions")

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
        return handle_version_or_str(version)
