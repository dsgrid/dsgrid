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


"""
RUNNING LIST OF TODOS
----------------------
# TODO: add a class to list registered projects by handle+version
# TODO: add a func to list registered projects by handle only
# TODO: add a func to view status of a registered project
# TODO: add a func to view status and missing datasets of a registered project
# TODO: add a func to see registered projects for a particular handle and the change_log
# TODO: it would be good to dynamically create the change log for a registry based on change items in config TOMLs. Would this capture dataset changes if file was renamed?

# TODO create Dataset Registration
#       - this should copy the local path or input data to s3 registry

# TODO: when a registry gets updated, we need some change_log logic that gets captured what changes; also need logic that affects the versioning (major, minor, patch)
# TODO: would be nice to have a status message like "1 more dataset to load"
"""


class ProjectRegistryModel(RegistryBaseModel):
    """Defines project registry"""

    project_id: str = Field(
        tile="project_id",
        description="unique project identifier",
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
