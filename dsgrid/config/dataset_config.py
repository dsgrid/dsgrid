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
from dsgrid.dimension.base import DimensionType, DSGBaseModel
from dsgrid.utils.utilities import run_command


from dsgrid.config._config import (
    TimeDimension, Dimension, ConfigRegistrationDetails)


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



# TODO will need to rename this as it really should be more generic inputs
#   and not just sector inputs. "InputSectorDataset" is already taken in
#   project_config.py
# TODO: this already assumes that the data is formatted for DSG, however,
#       we may want the dataset config to be "before" any DSG parquets get
#       formatted.
class InputSectorDataset(DSGBaseModel):
    """Input dataset configuration class"""
    data_type: DSGDatasetParquetType = Field(
        title="data_type",
        alias="type",
        description="DSG parquet input dataset type"
    )
    directory: str = Field(
        title="directory",
        description="directory with parquet files"
    )

    # TODO: 
    #   1. validate data matches dimensions specified in dataset config;
    #   2. check that all required dimensions exist in the data or partitioning
    #   3. check expected counts of things
    #   4. check for required tables accounted for; 
    #   5. any specific scaling factor checks?


class DatasetConfig(DSGBaseModel):
    """Represents model dataset configurations"""
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
    dimensions: List[Union[Dimension, TimeDimension]] = Field(
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
            val = Dimension(**value)
        return val

    # TODO: if local path provided, we want to upload to S3 and set the path 
    #   here to S3 path
    
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

    # TODO:
    #   - check binning/partitioning / file size requirements?
    #   - check unique names
    #   - check unique files
    #   - add similar validators as project_config Dimensions
    # NOTE: project_config.Dimensions has a lot of the
    #       validators we want here however, its unclear to me how we can
    #       apply them in both classes because they are root valitors and
    #       also because Dimensions includes project_dimension and
    #       supplemental_dimensions which are not required at the dataset
    #       level.
