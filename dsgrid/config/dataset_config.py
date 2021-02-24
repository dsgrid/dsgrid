'''
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
'''
from enum import Enum
from typing import List, Optional, Union, Dict
from pathlib import Path
import os
import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.exceptions import DSGBaseException
from dsgrid.config.project_config import (DSGBaseModel, Dimension,
                                          TimeDimension, DimensionType)
from dsgrid.registry.load_registry import load_project_registry
from dsgrid.utils.utilities import run_command


# TODO: likely needs refinement (missing mappings)
LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"


class InputDatasetType(Enum):
    SECTOR_MODEL = 'sector_model'
    HISTORICAL = 'historical'
    BENCHMARK = 'benchmark'


class DSGDatasetParquetType(Enum):
    LOAD_DATA = 'load_data'  # required
    LOAD_DATA_LOOKUP = 'load_data_lookup'  # required
    DATASET_DIMENSION_MAPPING = 'dataset_dimension_mapping'  # optional
    PROJECT_DIMENSION_MAPPING = 'project_dimension_mapping'  # optional


class DatasetDimension(Dimension):
    use_project_dimension: Optional[bool] = Field(
        title="use_project_dimension",
        description="Boolean to use same dimension settings as the project",
    )

    @validator("use_project_dimension")
    def apply_project_dimension_if_true(cls, use_project_dimension, values):
        print(values)


class DatasetDimensions(DSGBaseModel):
    """Contains dimensions defined by a dataset"""
    dimensions: List[Union[DatasetDimension, TimeDimension]] = Field(
        title="dimensions",
        description="dimensions defined by the dataset",
    )
    # TODO: add the same handle_dimension union as project config
    # TODO: why do I not get the same errors here as were recieved
    #   at the project config level?


# TODO will need to rename this as it really should be more generic inputs
#   and not just sector inputs. "InputSectorDataset" is already taken in
#   project_config.py
class InputSectorDataset(DSGBaseModel):
    # TODO: this already assumes that the data is formatted for DSG, however,
    #       we may want the dataset config to be "before" any DSG parquets get
    #       formatted.
    data_type: DSGDatasetParquetType = Field(
        title="data_type",
        alias="type",
        description="DSG parquet input dataset type"
    )
    directory: str = Field(
        title="directory",
        description="directory with parquet files"
    )

    # TODO: validate data matches dimensions;
    #   must not have any missing dimension value;
    #   check expected counts of things?
    #   must not have any missing dimensions;
    #   time must be present for all dimension combos;
    #   - if dimension != project.dimension, then a project_dimension_mapping
    #   table must be provided (for each mismatching dimension)??
    #   Else: we need some way to verify the expected project dim mappings


class DatasetConfig(DSGBaseModel):
    """Represents model dataset configurations"""
    project_version: str = Field(
        title="project_version",
        description="project version",
    )
    dataset_id: str = Field(
        title="dataset_id",
        description="dataset identifier",
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="DSG defined input dataset type"
    )
    # TODO: is this necessary?
    model_name: str = Field(
        title="model_name",
        description="model name",
    )
    # TODO: is this necessary?
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    path: str = Field(
        title="path",
        description="path containing data",
    )
    dimensions: List[Union[DatasetDimension, TimeDimension]] = Field(
        title="dimensions",
        description="dimensions defined by the dataset",
    )
    # TODO: Metdata is TBD
    metadata: Optional[Dict] = Field(
       title='metdata',
       description='Dataset Metadata',
    )

    # TODO: can we reuse this validator? Its taken from the
    #   project_config Diemnsions model
    @validator('dimensions',
               pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, value):
        """
        Validate dimension type work around for pydantic Union bug
        related to: https://github.com/samuelcolvin/pydantic/issues/619
        """
        # NOTE: Errors inside Dimension or TimeDimension will be duplicated
        # by Pydantic
        if value['type'] == DimensionType.TIME.value:
            val = TimeDimension(**value)
        else:
            val = DatasetDimension(**value)
        return val

    # TODO: maybe this type of func lives outside of the pydantic config in
    #   the wrapper?
    def get_project_registry(project_version):
        """Get the project registry associated with the file"""
        # TODO: this needs to pull from S3 or some other central store
        #   at some point
        pr_path = f'./registry/projects/{project_version}.toml'
        # pr = ProjectRegistry(**toml.load(pr_path))
        pr = load_project_registry(pr_path)
        return pr

    @validator('project_version')
    def check_active_project_registration(cls, project_version):
        """
        Validate that the project registration exists and that it
        is not deprecated.
        """
        pr = cls.get_project_registry(project_version)
        # # TODO using .value here because enum isn't saving value by default
        # TODO: this does not work unless we use exception; ValueError results
        #   in error w/ next validator on dataset_id"
        if pr.status.value == 'Deprecated':
            raise DSGBaseException(
                'Project registration for project handle is deprecated')
        return project_version

    @validator('dataset_id')
    def check_project_registry_for_dataset_id(cls, dataset_id, values):
        """Check that dataset has not already been registered to project"""
        # if 'project_version'
        project_version = values['project_version']
        pr = cls.get_project_registry(project_version)
        dataset_ids = []
        for dataset in pr.dataset_registries:
            dataset_ids.append(dataset.dataset_id)
        # check that dataset is expected by project
        if dataset_id not in dataset_ids:
            raise ValueError(
                f'Dataset ID "{dataset_id}" is not expected by project')
        # check that dataset has not already been registered to project
        for dataset in pr.dataset_registries:
            if dataset.dataset_id == dataset_id:
                if dataset.status.value == 'Registered':
                    raise ValueError(
                        f'Dataset ID "{dataset_id}" is already registered to '
                        f'project version "{project_version}"')
        return dataset_id

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
        dataset_id = values['dataset_id']
        project_version = values['project_version']
        pr = cls.get_project_registry(project_version)
        pc = pr.project_config
        for dataset in pc.input_datasets.datasets:
            if dataset.dataset_id == dataset_id:
                fields = ['dataset_type', 'model_name', 'model_sector']
                for field in fields:
                    data_val = values[field]
                    pc_val = getattr(dataset, field)
                    if isinstance(type(pc_val), Enum):
                        pc_val = pc_val.value
                    if pc_val != data_val:
                        raise ValueError(
                            f'Dataset {field}="{data_val}" but the project '
                            f'config expects "{pc_val}".')
        return values

    @validator("path")
    def check_path(cls, path):
        """Check dataset parquet path"""
        if path.startswith('s3://'):

            s3path = path
            home = str(Path.home())
            path = f"{home}/.dsgrid-data/{s3path}".replace("s3://", "")
            sync_command = f"aws s3 sync {s3path} {path}"
            run_command(sync_command)

        else:
            if not os.path.isdir(path):
                raise ValueError(f"{path} does not exist for InputDataset")

        # check load data (required)
        load_data_path = os.path.join(path, LOAD_DATA_FILENAME)
        if not os.path.exists(load_data_path):
            raise ValueError(f"{path} does not contain {LOAD_DATA_FILENAME}")

        # check load data lookup (required)
        load_data_lookup_path = os.path.join(path, LOAD_DATA_LOOKUP_FILENAME)
        if not os.path.exists(load_data_lookup_path):
            raise ValueError(
                f"{path} does not contain {LOAD_DATA_LOOKUP_FILENAME}")

        # TODO: check dataset_dimension_mapping (optional) if exists
        # TODO: check project_dimension_mapping (optional) if exists

        if isinstance(s3path, str):
            path = s3path

        return path


# TODO where do this live? We can't put it in registry.py because
#   the dataset config needs to reference the project registry ATM
#   I can't put it in its own dataset_registry because it needs to reference
#   the project registry
#   @Dthom - help!
def RegisterDataset(config_toml):
    """
    Register a dataset with a registered dsgrid project
     given datast configuration toml.
    """
    # ----------------------------------
    # TODO ACTION ITEMS
    # assign a version
    # make dataset regitry file
    # add to project registry file and save it
    # ----------------------------------

    # validate dataset config
    dataset_config = DatasetConfig(**toml.load(config_toml))
    print("Dataset Config Validated")  # TODO: log message

    project_config = dataset_config.get_project_registry(
        dataset_config.project_version)