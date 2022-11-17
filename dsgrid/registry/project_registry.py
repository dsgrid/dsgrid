"""Manages the registry for a project."""

import logging
from pathlib import Path
from typing import Optional

from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import (
    DatasetRegistryStatus,
)
from .registry_base import RegistryBaseModel, RegistryBase

logger = logging.getLogger(__name__)


class ProjectDatasetRegistryModel(DSGBaseModel):
    """Project registration details for datasets"""

    dataset_id: str = Field(
        title="dataset_id",
        description="Dataset identifier",
    )
    version: Optional[str] = Field(
        title="dataset_version",
        description="Full dataset version to be used to find dataset registry",
    )
    status: DatasetRegistryStatus = Field(
        title="status",
        description="Dataset status within the project",
    )


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
        return "project.json5"

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
