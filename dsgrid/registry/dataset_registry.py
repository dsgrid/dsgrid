"""Manages registry for a dataset"""

import logging
from pathlib import Path
from typing import List, Optional, Union


from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import ConfigRegistrationModel
from dsgrid.utils.files import load_data
from dsgrid.utils.versioning import handle_version_or_str


logger = logging.getLogger(__name__)


class DatasetRegistry:
    """Controls dataset registration"""

    DATASET_REGISTRY_PATH = Path("registry/datasets")

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
        title="dataset_id",
        description="dataset identifier",
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="dataset version",
    )
    description: str = Field(title="description", description="detail on the project or dataset")
    registration_history: Optional[List[ConfigRegistrationModel]] = Field(
        title="registration_history",
        description="history of all registration updates",
        default=[],
    )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)
