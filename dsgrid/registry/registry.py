import os
from enum import Enum
from typing import List, Optional
import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.dimension.base import DSGBaseModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.registry.dataset_registry import (DatasetRegistryStatus,
                                              DatasetRegistryBase)

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


# def RegisterDataset(config_toml):
#     """register a dataset to a project"""
#     # if dataset already exists in project and dataset config is the same,
# then throw a message that says it already is registered
#     # if dataset already exists, but the project config has changed, then
# register the dataset with the new project version
#     # if dataset already exists but the config is different, then throw an
# error telling them to update the project config
#     # if dataset already exists with the project, then throw error
#     # register with a verison?

#     # TODO: copy local path or input path to s3 registry


class ProjectRegistryStatus(Enum):
    # TODO: is this complete?
    INITIAL_REGISTRATION = 'Initial Registration'
    IN_PROGRESS = 'In Progress'
    COMPLETE = 'Complete'
    DEPRICATED = 'Deprecated'


# class DatasetRegistryStatus(Enum):
#     # TODO: is this complete?
#     UNREGISTERED = 'Unregistered'
#     REGISTERED = 'Registered'


# class DatasetRegistryBase(DSGBaseModel):
#     """Dataset registration base class"""
#     dataset_id: str = Field(
#         title='dataset_id',
#         description="dataset identifier"
#     )
#     status: DatasetRegistryStatus = Field(
#         title='status',
#         description='dataset registry status',
#     )
#     dataset_version: Optional[str] = Field(  # TODO: this needs to be generated
#         title='dataset_version',
#         description="full dataset version (dataset id + version)",
#         alias="version",
#     )
#     dataset_config: Optional[dict] = Field(
#         title='dataset_config',
#         description="dataset configuration",  # TODO: do we save config details?
#     )

class DatasetRegistryStatus(Enum):
    # TODO: is this complete?
    UNREGISTERED = 'Unregistered'
    REGISTERED = 'Registered'


class DatasetRegistryBase(DSGBaseModel):
    """Dataset registration base class"""
    dataset_id: str = Field(
        title='dataset_id',
        description="dataset identifier"
    )
    status: DatasetRegistryStatus = Field(
        title='status',
        description='dataset registry status',
    )
    dataset_version: Optional[str] = Field(  # TODO: this needs to be generated
        title='dataset_version',
        description="full dataset version (dataset id + version)",
        alias="version",
    )
    dataset_config: Optional[dict] = Field(
        title='dataset_config',
        description="dataset configuration",  # TODO: do we save config details?
    )

class DatasetRegistry(DatasetRegistryBase):
    # TODO: these need to be required when we register a dataset,
    # but NOT when we register a project
    dataset_version: str = Field(  # TODO: this needs to be generated
        title='dataset_version',
        description="full dataset version (dataset id + version)",
        alias="version",
    )
    # TODO maybe this doesn't need to be saved in the project config version?
    dataset_config: DatasetConfig = Field(
        title='dataset_config',
        description="dataset configuration class as dict",
    )


class ProjectDatasetRegistry(DatasetRegistryBase):
    dataset_id: str = Field(
        title="dataset_id"
    )
    status: DatasetRegistryStatus = Field(
        title="status"
    )



class ProjectRegistry(DSGBaseModel):
    """Project registery class"""
    # TODO: It is prefered that when the project registration is saved that it
    #   has a different field ordering than the validation field order
    #   presented here. For example, project_id, project_version, and status
    #   are preffered to be at the top
    project_config: ProjectConfig = Field(
        tile="proejct_config",
        description="project configuration dictonary"
    )
    # TODO: this is a duplicate of the project_id found in the project_config.
    #   Is this needed? I think there is some value in having the project ID
    #   at the top of the registry and level=1 accessbility
    project_id: str = Field(
        tile="project_id",
        description="project identifier",
        default=""
    )
    project_version: str = Field(
        tile="title",
        description="project version",
    )
    status: ProjectRegistryStatus = Field(
        tile="status",
        description="project registry status"
    )
    dataset_registries:  List[ProjectDatasetRegistry] = Field( #DatasetRegistryBase, ProjectDatasetRegistry
        title="dataset_registries",
        description="list of dataset registry",
        default=[]
    )

    # @validator('project_config', pre=True)
    # def test(cls, project_config, values):
    #     print(values['project_config'])
    #     print(type(values['project_config']))

    # TODO: validate that the project config is valid before registering it

    @validator('project_id', always=True)
    def set_project_id(cls, project_id, values):
        """Set Project ID given Project Config."""
        if project_id == "":
            project_id = values['project_config'].project_id
        return project_id

    @validator('dataset_registries')
    def set_dataset_registries(cls, dataset_registries, values):
        """Set Dataset Registries given Project Config."""
        if dataset_registries == []:
            dataset_registries = []
            datasets = values['project_config'].input_datasets.datasets
            for dataset in datasets:
                dataset_registries.append(
                    {'dataset_id': dataset.dataset_id,
                     'status': 'Unregistered'} # TODO
                     )
        return dataset_registries

    def register(cls, registry_path):
        """Create Project Registration TOML file."""
        # TODO: ATM this is just a local registration; need a central
        #       cloud-version next
        
        # TODO: I can't figure out how to get project_config (type=ProjectConfig) to parse properly
        cls_dict = cls.dict()
        del cls_dict['project_config']
        cls_dict['project_config'] = toml.load('./project.toml')
        # with open(registry_path.replace('.toml', '.txt'), 'w') as j:
        #     j.write(str(cls_dict['project_config']))
        # TODO: figure out a way to save status enum as value not enum. Dan help!
        for key in cls_dict:
            if key == 'status':
                cls_dict[key] = cls_dict[key].value
            # if key == 'project_config':
            #     cls_dict[key] = cls_dict[key].dict()
        with open(registry_path, 'w') as j:
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


def RegisterProject(config_toml):
    """
    Register the dsgrid project given project configuration toml.
    """
    # TODO: Can we port most of this over to ProjectRegistry() ?
    # TODO: need smarter version updating / checks; use semvar packages
    # TODO: need support for minor type updates, i.e. metadata
    #       One option could be to check to see what changed to determine
    #       the versioning update

    # validate project config
    config_dict = toml.load(config_toml)
    project_config = ProjectConfig(**config_dict)
    project_id = project_config.project_id
    registry_path = './registry/projects'

    # if update is false, then assume version is v1.0.0
    if not project_config.registration.update:
        project_version = f'{project_id}-v1.0.0'
        registry_file = f'{registry_path}/{project_version}.toml'
        # if v1.0.0 registry does not exist for project_id
        if not os.path.exists(registry_file):
            # register project
            project_registry = ProjectRegistry(
                # TODO: consider setting project version in configuration
                project_version=project_version,
                status='Initial Registration', 
                project_config=config_dict
                )
            project_registry.register(registry_file)
        else:
            raise ValueError(
                f'Project registry for "{registry_file}" already exists'
                '\nIf you want to update the project registration with a new '
                'project version, then you will need to set update=True in '
                'project config. Alternatively, if you want to initiate a new '
                'dsgrid project, you will need to specify a new version '
                'handle in the project config.'
                )

    # if update is true...
    else:
        # list existing project registries
        existing_versions = []
        for f in os.listdir(registry_path):
            if f.startswith(project_id):
                existing_versions.append(int(f.split('-v')[1].split('.')[0]))
        # check for existing project registries
        if len(existing_versions) == 0:
            raise ValueError(
                'Registration.update=True, however, no updates can be made '
                'because there are no existing registries for Project ID = '
                f'{project_id}. Check project_id or set '
                'Registration.update=True in the Project Config.')
        # find the latest registry version
        # NOTE: this is currently based on major verison only
        last_vmajor_nbr = sorted(existing_versions)[-1]
        old_project_version = f'{project_id}-v{last_vmajor_nbr}.0.0'
        old_registry_file = f'{registry_path}/{old_project_version}.toml'

        # depricate old registry
        t = toml.load(old_registry_file)
        t['status'] = 'Deprecated'
        with open(old_registry_file.format(**locals()), 'w') as f:
            toml.dump(t, f)
        # TODO: unlink dataset registries tied to this latest version

        # update version (from major version only)
        # TODO NEED REAL LOGIC FOR THIS!
        major = int(last_vmajor_nbr)+1
        minor = 0  # assume 0 for now
        patch = 0  # assume 0 for now
        project_version = f'{project_id}-v{major}.{minor}.{patch}'
        registry_file = f'{registry_path}/{project_version}.toml'

        # register new project
        project_registry = ProjectRegistry(
                project_version=project_version,
                status='Initial Registration',
                project_config=config_dict
                )
        project_registry.register(registry_file)
        return project_registry
