"""Manages the registry for a project."""

import logging
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.data_models import DSGBaseModel, serialize_model
from dsgrid.registry.common import (
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.versioning import make_version, handle_version_or_str
from dsgrid.filesytem.aws import sync
from dsgrid.common import S3_REGISTRY

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

# association_tables: Optional[List[str]] = Field(title="association_tabls", default=[])


class ProjectDatasetRegistryModel(DSGBaseModel):
    """Project registration details for datasets"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset identifier",
    )
    version: Optional[Union[None, str, VersionInfo]] = Field(
        title="dataset_version",
        description="full dataset version to be used to find dataset registry",
    )
    status: DatasetRegistryStatus = Field(
        title="status",
        description="dataset status within the project",
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
        description="unique project identifier",
    )
    status: ProjectRegistryStatus = Field(
        tile="status", description="project registry status", default="Initial Registration"
    )
    dataset_registries: Optional[List[ProjectDatasetRegistryModel]] = Field(
        title="dataset_registries",
        description="list of dataset registry",
    )


class ProjectRegistry(RegistryBase):
    """Controls a project registry."""

    PROJECT_REGISTRY_PATH = Path("projects")
    PROJECT_REGISTRY_S3_PATH = f"{S3_REGISTRY}/projects"

    @staticmethod
    def model_class():
        return ProjectRegistryModel

    @staticmethod
    def registry_path():
        return ProjectRegistry.PROJECT_REGISTRY_PATH

    @staticmethod
    def registry_s3_path():
        return ProjectRegistry.PROJECT_REGISTRY_S3_PATH

    @staticmethod
    def sync_push(local_registry_path):
        sync(
            local_registry_path / ProjectRegistry.registry_path(),
            ProjectRegistry.registry_s3_path(),
        )

    @staticmethod
    def sync_pull(local_registry_path):
        sync(
            local_registry_path / ProjectRegistry.registry_s3_path(),
            ProjectRegistry.registry_path(),
        )

    def has_dataset(self, dataset_id, status):
        """Return True if the dataset_id is stored with status."""
        for registry in self._model.dataset_registries:
            if registry.dataset_id == dataset_id:
                return registry.status == status
        return False

    def set_dataset_status(self, dataset_id, status):
        """Set the dataset status to the given value.

        Parameters
        ----------
        dataset_id : str
        status : DatasetRegistryStatus

        Raises
        ------
        ValueError
            Raised if dataset_id is not stored.

        """
        for registry in self._model.dataset_registries:
            if registry.dataset_id == dataset_id:
                registry.status = status
                logger.info(
                    "Set dataset_id=%s status=%s for project=%s",
                    dataset_id,
                    status,
                    self.project_id,
                )
                return

        raise ValueError(f"dataset_id={dataset_id} is not stored.")

    @property
    def project_id(self):
        return self._model.project_id

    def list_registered_datasets(self):
        """Get registered datasets associated with project registry.

        Returns
        -------
        list
            list of dataset IDs

        """
        status = DatasetRegistryStatus.REGISTERED
        return [x.dataset_id for x in self._iter_datasets_by_status(status)]

    def list_unregistered_datasets(self):
        """Get unregistered datasets associated with project registry.

        Returns
        -------
        list
            list of dataset IDs

        """
        status = DatasetRegistryStatus.UNREGISTERED
        return [x.dataset_id for x in self._iter_datasets_by_status(status)]

    def _iter_datasets_by_status(self, status):
        for registry in self._model.dataset_registries:
            if registry.status == status:
                yield registry

    @property
    def project_config(self):
        """Return the ProjectConfig."""
        return self._model.project_config
