"""Manages the registry for a project."""

import logging
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import (
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.versioning import make_version

logger = logging.getLogger(__name__)


class ProjectDatasetRegistryModel(DSGBaseModel):
    """Project registration details for datasets"""

    dataset_id: str = Field(
        title="dataset_id",
        description="Dataset identifier",
    )
    version: Optional[Union[None, str, VersionInfo]] = Field(
        title="dataset_version",
        description="Full dataset version to be used to find dataset registry",
    )
    status: DatasetRegistryStatus = Field(
        title="status",
        description="Dataset status within the project",
    )

    @validator("version")
    def check_version(cls, version):
        if isinstance(version, VersionInfo) or version is None:
            return version
        return make_version(version)


class ProjectRegistryModel(RegistryBaseModel):
    """Defines project registry"""

    project_id: str = Field(
        tile="project_id",
        description="Unique project identifier",
    )


class ProjectRegistry(RegistryBase):
    """Controls a project registry."""

    PROJECT_REGISTRY_PATH = Path("configs/projects")

    @staticmethod
    def config_filename():
        return "project.toml"

    @property
    def config_id(self):
        return self.model.project_id

    @staticmethod
    def model_class():
        return ProjectRegistryModel

    @staticmethod
    def registry_path():
        return ProjectRegistry.PROJECT_REGISTRY_PATH

    @property
    def project_config(self):
        """Return the ProjectConfig."""
        return self._model.project_config
