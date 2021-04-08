import os
from enum import Enum
import logging
from typing import Dict, List, Optional, Union
import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo

from dsgrid.models import DSGBaseModel, serialize_model
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config._config import ConfigRegistrationModel
from dsgrid.registry.common import (
    RegistryType,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
    make_version,
)
from dsgrid.registry.versioning import versioning, PROJECT_REGISTRY_PATH
from dsgrid.utils.files import dump_data, load_data


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


class ProjectRegistryModel(DSGBaseModel):
    """Defines project registry"""

    project_id: str = Field(
        tile="project_id",
        description="unique project identifier",
    )
    version: Union[str, VersionInfo] = Field(
        tile="title",
        description="project version",
    )
    status: ProjectRegistryStatus = Field(
        tile="status", description="project registry status", default="Initial Registration"
    )
    description: str = Field(title="description", description="detail on the project or dataset")
    dataset_registries: Optional[List[ProjectDatasetRegistryModel]] = Field(
        title="dataset_registries",
        description="list of dataset registry",
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


class ProjectRegistry:
    """Controls a project registry."""

    def __init__(self, model):
        """Constructs ProjectRegistry.

        Parameters
        ----------
        model : ProjectRegistryModel

        """
        self._model = model

    @classmethod
    def load(cls, filename):
        """Loads a project registry from a file.

        Parameters
        ----------
        filename : str

        Returns
        -------
        ProjectRegistry

        """
        return cls(ProjectRegistryModel.load(filename))

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

    @property
    def version(self):
        return self._model.version

    @version.setter
    def version(self, val):
        self._model.version = val

    @property
    def registration(self):
        return self._model.registration

    def serialize(self, filename):
        dump_data(serialize_model(self._model), filename)

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
