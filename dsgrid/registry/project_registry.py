import os
from enum import Enum
from typing import Dict, List, Optional
import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.dimension.base import DSGBaseModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config._config import ConfigRegistrationDetails

from dsgrid.registry.versioning import versioning, PROJECT_REGISTRY_PATH


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


class ProjectRegistryStatus(Enum):
    # TODO: is this complete?
    INITIAL_REGISTRATION = 'Initial Registration'
    IN_PROGRESS = 'In Progress'
    COMPLETE = 'Complete'
    DEPRICATED = 'Deprecated'


class DatasetRegistryStatus(Enum):
    # TODO: is this complete?
    UNREGISTERED = 'Unregistered'
    REGISTERED = 'Registered'


class ProjectConfigRegistry(ProjectConfig):
    """When registering a project, require that registration details be in
    the project configuraiton."""
    # require registration context when registerying
    registration: ConfigRegistrationDetails = Field(
        title="registration",
        description="registration details"
    )

class ProjectDatasetRegistry(DSGBaseModel):
    """Project registration details for datasets"""
    dataset_id: str = Field(
        title='dataset_id',
        description="dataset identifier"
    )
    # TODO: type should be DataVersion; TBD
    dataset_version: Optional[str] = Field(
        title='dataset_version',
        description="full dataset version (dataset id + version) to be used to find dataset registry",
        alias="version",
    )
    status: DatasetRegistryStatus = Field(
        title="status"
    )
    association_tables: Optional[List[str]] = Field(
        title="association_tabls",
        default=[]
    )


class ProjectRegistry(DSGBaseModel):
    """Project registery class"""
    # TODO: It is prefered that when the project registration is saved that it
    #   has a different field ordering than the validation field order
    #   presented here. For example, project_id, project_version, and status
    #   are preffered to be at the top
    project_config: Dict = Field(
        tile="proejct_config",
        description="project configuration dictonary"
    )
    project_version: str = Field(  # TODO: make pydantic class type?
        tile="title",
        description="project version",
    )
    status: ProjectRegistryStatus = Field(
        tile="status",
        description="project registry status",
        default="Initial Registration"
    )
    dataset_registries: Optional[List[ProjectDatasetRegistry]] = Field( #DatasetRegistryBase, ProjectDatasetRegistry
        title="dataset_registries",
        description="list of dataset registry",
    )

    @validator('dataset_registries')
    def set_dataset_registries(cls, dataset_registries, values):
        """Set Dataset Registries given Project Config."""
        if not dataset_registries:
            dataset_registries = []
            datasets = values['project_config']['input_datasets']['datasets']
            for dataset in datasets:
                dataset_registries.append(
                    {'dataset_id': dataset['dataset_id'],
                     'status': 'Unregistered'}  # TODO: Enum?
                     )
        return dataset_registries

    def register(cls):
        """Create Project Registration TOML file."""
        # TODO: ATM this is just a local registration; need a central
        #       cloud-version next

        # remove registration settings when registering
        #   TODO: a way to exclude fields on export would be ideal.
        #       Open pydanic PR suggests it will be implemented soon.
        #       https://github.com/samuelcolvin/pydantic/pull/2231
        #       exclude registration details from being export
        cls_dict = cls.dict()
        del cls_dict['project_config']['registration']

        registry_file = f'{PROJECT_REGISTRY_PATH}/{cls.project_version}.toml'
        with open(registry_file, 'w') as j:
            toml.dump(cls_dict, j)

    def get_registered_datasets(cls):
        """Get registered datasets associated with project registry."""
        registered = []
        for i in cls.dataset_registries:
            if i.status == 'Registered':
                registered.append(i.dataset_id)
        return registered

    def get_unregistered_datasets(cls):
        """Get unregistered datasets associated with project registry."""
        unregistered = []
        for i in cls.dataset_registries:
            if i.status != 'Registered':
                unregistered.append(i.dataset_id)
        return unregistered

    def get_project_config(cls):
        return ProjectConfig(**cls.project_config)



def register_project(config_toml):
    """
    Register the dsgrid project given project configuration toml.
    """
    config_dict = toml.load(config_toml)

    # validate project config for registration
    ProjectConfigRegistry(**config_dict)

    # set project version
    project_version = versioning(
        registry_type='project',
        id_handle=config_dict['project_id'],
        update=config_dict['registration']['update'])

    # register project
    ProjectRegistry(
        project_config=config_dict, project_version=project_version).register()

    print('Done Registering Project')
