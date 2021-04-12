"""
Submit dataset to a project

There is a ton of clean up that still needs to happen on this file. 

This is where we validate the dataset config to the project config

    #   - if dimension != project.dimension, then a project_dimension_mapping
    #   table must be provided (for each mismatching dimension)?
    #   Else: we need some way to verify the expected project dim mappings

This is also where we update the project registry with the dataset_version??
"""
from enum import Enum
from typing import List, Optional, Dict
import toml

from pydantic import Field
from pydantic import validator, root_validator

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base import DSGBaseModel
from dsgrid.exceptions import DSGBaseException
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.registry.project_registry import ProjectRegistry
from dsgrid.registry.versioning import PROJECT_REGISTRY_PATH, DATASET_REGISTRY_PATH

"""
# TODO:
    - lot of validation checks remain to be written
    - need to think through the association/mapping files part here.
    - check path of association files
    - where do we put the association tables? do these get added to the
        project config?
    - might consider throwing a warning or error if user is trying to register
      and older verison of the dataset. Or maybe we build in deprecation
    
    - I am not IN love with the association table from county-county

# TODO CHECKS:
    - must have one association table for each of the 9 dimensions
    - need to figure out logic w/ project_dimension_mapping parquets
        - scenario: what if we have a dataset that is already published, but
            we want to associate it with a new project that has a different
            dimension than the native dataset file and it requires some kind of
            disaggregation w/ a scaling factor? Do we allow this to be a
            separate file that gets added on when registering the project?

unless otherwise specified, we assume 1:1 mapping
"""


class SubmitDataset(DSGBaseModel):
    """Submit-dataset to config class"""

    project_version: str = Field(
        title="project_version",
        description="project version",
    )
    dataset_version: str = Field(
        title="dataset_version",
        description="dataset version",
    )
    # TODO: we may call this a dataset_to_project_dimension_mapping?
    # TODO make type pydantic model
    association_tables: Optional[List[Dict]] = Field(
        title="association_tables",
        description="association tables that map from dataset to project",
    )

    # TODO: do these 2 get functions live here or in a wrapper?
    @classmethod
    def get_registry(cls, registry_type, version):
        """Get the project registry associated with a project_version"""
        # TODO: this needs to pull from S3 or some other central store
        #   at some point
        if registry_type == "project":
            pr_path = f"{PROJECT_REGISTRY_PATH}/{version}.toml"
            registry = ProjectRegistry(**toml.load(pr_path))
        if registry_type == "dataset":
            pr_path = f"{DATASET_REGISTRY_PATH}/{version}.toml"
            registry = DatasetRegistry(**toml.load(pr_path))
        return registry

    @classmethod
    def get_config(cls, config_type, version):
        registry = cls.get_registry(config_type, version)
        if config_type == "project":
            return ProjectConfig(**registry.project_config)
        if config_type == "dataset":
            return DatasetConfig(**registry.dataset_config)

        # TODO: Need help here. Issues relate to enumeration not being stored as
        #   value. Also, I don't know how to loop through these 3 fields properly.
        #   I originally tried a validator for ('dataset_type', 'model_name',
        #   'model_sector')

    @root_validator(pre=True)
    def check_for_project_config_expectations(cls, values):
        """
        Check dataset fields to project fields.
        Fields to check: id, model_name, model, sectors
        """
        # TODO: Need help here. I tried to make this a validator function but
        #   I was hitting issues with field names and the enumeration classes
        #   not storing the values. This is the best I could do ATM.
        project_config = cls.get_config("project", values["project_version"])
        dataset_config = cls.get_config("dataset", values["dataset_version"])

        for dataset in project_config.input_datasets.datasets:
            if dataset.dataset_id == dataset_config.dataset_id:
                fields = ["dataset_type", "model_name", "model_sector"]
                for field in fields:
                    data_val = getattr(dataset_config, field)
                    pc_val = getattr(dataset, field)
                    if isinstance(type(pc_val), Enum):
                        pc_val = pc_val.value
                    if pc_val != data_val:
                        raise ValueError(
                            f'Dataset {field}="{data_val}" but the project '
                            f'config expects "{pc_val}".'
                        )
        return values

    @validator("project_version")
    def check_active_project_registration(cls, project_version):
        """
        Validate that the project registration exists and that it
        is not deprecated.
        """
        pr = cls.get_registry(registry_type="project", version=project_version)
        # TODO: this does not work unless we use exception; ValueError results
        #   in error w/ next validator on dataset_id"
        if pr.status == "Deprecated":
            raise DSGBaseException("Project registration for project handle is deprecated")
        return project_version

    @root_validator()
    def check_project_registry_for_dataset_id(cls, values):
        """Check that dataset has not already been registered to project"""
        project_version = values["project_version"]
        pr = cls.get_registry(registry_type="project", version=project_version)
        dataset_ids = []
        for dataset in pr.dataset_registries:
            dataset_ids.append(dataset.dataset_id)
        data_config = cls.get_config(config_type="dataset", version=values["dataset_version"])
        dataset_id = data_config.dataset_id
        # check that dataset is expected by project
        if dataset_id not in dataset_ids:
            raise ValueError(f'Dataset ID "{dataset_id}" is not expected by project')
        # check that dataset has not already been registered to project
        for dataset in pr.dataset_registries:
            if dataset.dataset_id == dataset_id:
                if dataset.status == "Registered":
                    raise ValueError(
                        f'Dataset ID "{dataset_id}" is already registered to '
                        f'project version "{project_version}"'
                    )
        return values

    @validator("association_tables")
    def assign_dimension_mappings(cls, association_tables, values):
        # TODO if dimension mappings are not set, we assume a 1:1 mapping based on dimenstion type
        #   example: from=dataset.geography, to=project.
        # project_registry = get_project_registry(values['project_version'])
        # print(project_registry)
        # assume a 1:1 mapping if association tables are not assigned
        if not association_tables:
            project_config = cls.get_config("project", values["project_version"])
            dataset_config = cls.get_config("dataset", values["dataset_version"])
            project_dimensions = [p for p in project_config.dimensions.project_dimensions]
            mappings = []
            for dimension in dataset_config.dimensions:
                data_dim_type = dimension.dimension_type
                # TODO: @dthom do we have a wrapper tohelp with getting
                #   dimensions of a certain type?
                for i, pdimension in enumerate(project_dimensions):
                    if pdimension.dimension_type == data_dim_type:
                        mappings.append(
                            {
                                "from_dimension": dimension.name,
                                "to_dimension": pdimension.name,
                                "from_key": "id",
                                "to_key": "id",
                            }
                        )
            association_tables = mappings
        else:
            # TODO: temporary error for association tables until we build out
            #   how these are handled
            raise Exception(
                "DSG has not yet developed a methodlogy to take in custom "
                "dataset-to-project dimension associations"
            )

        return association_tables


def submit_dataset(project_version, dataset_version, association_tables=[]):
    """Submit a dataset to a project.

    Args:
        config_file (str): submission config toml
    """
    # validate dataset submission criteria
    config = SubmitDataset(
        project_version=project_version,
        dataset_version=dataset_version,
        association_tables=association_tables,
    )

    # TODO modify project-registry .toml for that dataset id
    #   - set status = 'Registered'
    #   - add new field for dataset version
