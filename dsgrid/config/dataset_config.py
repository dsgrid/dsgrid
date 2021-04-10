"""
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
"""
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union, Dict
import os
import logging

import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.common import LOCAL_REGISTRY_DATA
from dsgrid.dimension.models import (
    DimensionType,
    TimeDimensionModel,
    DimensionReferenceModel,
    handle_dimension_union,
)
from dsgrid.exceptions import DSGBaseException
from dsgrid.data_models import DSGBaseModel
from dsgrid.filesytem.aws import sync


# TODO: likely needs refinement (missing mappings)
LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"

logger = logging.getLogger(__name__)


class InputDatasetType(Enum):
    SECTOR_MODEL = "sector_model"
    HISTORICAL = "historical"
    BENCHMARK = "benchmark"


class DSGDatasetParquetType(Enum):
    LOAD_DATA = "load_data"  # required
    LOAD_DATA_LOOKUP = "load_data_lookup"  # required
    DATASET_DIMENSION_MAPPING = "dataset_dimension_mapping"  # optional
    PROJECT_DIMENSION_MAPPING = "project_dimension_mapping"  # optional


# TODO will need to rename this as it really should be more generic inputs
#   and not just sector inputs. "InputSectorDataset" is already taken in
#   project_config.py
# TODO: this already assumes that the data is formatted for DSG, however,
#       we may want the dataset config to be "before" any DSG parquets get
#       formatted.
class InputSectorDataset(DSGBaseModel):
    """Input dataset configuration class"""

    data_type: DSGDatasetParquetType = Field(
        title="data_type", alias="type", description="DSG parquet input dataset type"
    )
    directory: str = Field(title="directory", description="directory with parquet files")

    # TODO:
    #   1. validate data matches dimensions specified in dataset config;
    #   2. check that all required dimensions exist in the data or partitioning
    #   3. check expected counts of things
    #   4. check for required tables accounted for;
    #   5. any specific scaling factor checks?


class DatasetConfigModel(DSGBaseModel):
    """Represents model dataset configurations"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset identifier",
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="DSG defined input dataset type",
    )
    # TODO: is this necessary?
    model_name: str = Field(
        title="model_name",
        description="model name",
    )
    # TODO: This must be validated against the same field in ProjectConfigModel at registration.
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    path: str = Field(
        title="path",
        description="path containing data",
    )
    description: str = Field(
        title="description",
        description="describe dataset in details",
    )
    dimensions: List[DimensionReferenceModel] = Field(
        title="dimensions",
        description="dimensions defined by the dataset",
    )
    # TODO: Metdata is TBD
    metadata: Optional[Dict] = Field(
        title="metdata",
        description="Dataset Metadata",
    )

    # TODO: if local path provided, we want to upload to S3 and set the path
    #   here to S3 path

    @validator("path")
    def check_path(cls, path):
        """Check dataset parquet path"""
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if path.startswith("s3://"):
            # For unit test purposes this always uses the defaul local registry instead of
            # whatever the user created with RegistryManager.
            local_path = LOCAL_REGISTRY_DATA / path.replace("s3://", "")
            # sync(path, local_path)
            logger.warning("skipping AWS sync")  # TODO DT
        else:
            local_path = Path(path)
            if not local_path.exists():
                raise ValueError(f"{local_path} does not exist for InputDataset")

        load_data_path = local_path / LOAD_DATA_FILENAME
        if not load_data_path.exists():
            raise ValueError(f"{local_path} does not contain {LOAD_DATA_FILENAME}")

        load_data_lookup_path = local_path / LOAD_DATA_LOOKUP_FILENAME
        if not os.path.exists(load_data_lookup_path):
            raise ValueError(f"{local_path} does not contain {LOAD_DATA_LOOKUP_FILENAME}")

        # TODO: check dataset_dimension_mapping (optional) if exists
        # TODO: check project_dimension_mapping (optional) if exists

        # TODO AWS
        return local_path


class DatasetConfig:
    """Provides an interface to a DatasetConfigModel."""

    def __init__(self, model):
        self._model = model

    @classmethod
    def load(cls, config_file, dimension_manager):
        model = DatasetConfigModel.load(config_file)
        config = cls(model)
        dimension_manager.replace_dimension_references(model.dimensions)
        return config

    @property
    def model(self):
        return self._model

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
