from enum import Enum
import datetime
from pathlib import Path
from typing import List, Optional, Dict
import os
import logging

from pydantic import Field
from pydantic import validator

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.common import check_config_id_strict
from .config_base import ConfigBase
from .dimensions import (
    DimensionReferenceModel,
)
from dsgrid.data_models import DSGBaseModel


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
        description="DSG parquet input dataset type",
    )
    directory: str = Field(
        title="directory",
        description="directory with parquet files",
    )

    # TODO:
    #   1. validate data matches dimensions specified in dataset config;
    #   2. check that all required dimensions exist in the data or partitioning
    #   3. check expected counts of things
    #   4. check for required tables accounted for;
    #   5. any specific scaling factor checks?


class DataClassificationType(Enum):
    """Data risk classification type

    The moderate class includes all data under an NDA, data classified as business sensitive,
    data classification as Critical Energy Infrastructure Infromation (CEII), or data with
    Personal Identifiable Information (PII).

    See https://uit.stanford.edu/guide/riskclassifications for more information.
    """

    LOW = "low"
    MODERATE = "moderate"


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
    # TODO: This must be validated against the project's dimension records for data_source
    data_source: str = Field(
        title="data_source",
        description="data source name, e.g. 'ComStock'",
    )
    # TODO: This must be validated against the project's dimension records for metric_type
    metric_type: str = Field(
        title="metric_type", description="data metric type ID, e.g., 'energy_use'"
    )
    path: str = Field(
        title="path",
        description="path containing data",
    )
    description: str = Field(
        title="description",
        description="describe dataset in details",
    )
    origin_creator: str = Field(
        title="origin_creator",
        description="Origin data creator's name (first and last)",
    )
    origin_organization: str = Field(
        title="origin_organization",
        description="Origin organization name, e.g., NREL",
    )
    origin_contributors: List[str] = Field(
        title="origin_contributors",
        description="List of origin data contributor's first and last names"
        """ e.g., ["Harry Potter", "Ronald Weasley"]""",
        required=False,
    )
    origin_project: str = Field(
        title="origin_project",
        description="Origin project name",
        optional=True,
    )
    origin_date: str = Field(
        title="origin_date",
        description="Date the source data was generated",
    )
    origin_version: str = Field(
        title="origin_version",
        description="Version of the origin data",
    )
    source: str = Field(
        title="source",
        description="Source of the data (text description or link)",
    )
    data_classification: DataClassificationType = Field(
        title="data_classification",
        description="Data security classification (e.g., low, moderate, high)",
    )
    tags: Optional[List[str]] = Field(
        title="source",
        description="List of data tags",
        required=False,
    )
    load_data_column_dimension: DimensionType = Field(
        title="load_data_column_dimension",
        description="Columns in the load_data table are records of this dimension type.",
    )
    dimensions: List[DimensionReferenceModel] = Field(
        title="dimensions",
        description="dimensions defined by the dataset",
    )
    user_defined_metadata: Optional[Dict] = Field(
        title="user_defined_metadata",
        description="Additional user defined metadata fields",
        default={},
        required=False,
    )

    # TODO: if local path provided, we want to upload to S3 and set the path
    #   in the toml file back to S3 path --> does this happen in DatasetConfig instead?

    @validator("dataset_id")
    def check_dataset_id(cls, dataset_id):
        """Check dataset ID validity"""
        check_config_id_strict(dataset_id, "Dataset")
        return dataset_id

    @validator("path")
    def check_path(cls, path):
        """Check dataset parquet path"""
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if path.startswith("s3://"):
            raise Exception(f"Loading a dataset from S3 is not currently supported: {path}")
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

        return str(local_path)

    @validator("origin_project", "origin_version")
    def check_optional_origin_fields(cls, val, values):
        """Require optional origin metadata fields if the dataset type is sector model"""
        if values.get("dataset_type") == InputDatasetType.SECTOR_MODEL:
            if not val:
                raise ValueError(f"{val} must be defined if the dataset_type is sector_model")
        return val


class DatasetConfig(ConfigBase):
    """Provides an interface to a DatasetConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._dimensions = {}

    @staticmethod
    def config_filename():
        return "dataset.toml"

    @property
    def config_id(self):
        return self._model.dataset_id

    @staticmethod
    def model_class():
        return DatasetConfigModel

    @classmethod
    def load(cls, config_file, dimension_manager):
        config = cls._load(config_file)
        config.load_dimensions(dimension_manager)
        return config

    def load_dimensions(self, dimension_manager):
        """Load all dataset dimensions.

        Parameters
        ----------
        dimension_manager : DimensionRegistryManager

        """
        self._dimensions.update(dimension_manager.load_dimensions(self.model.dimensions))

    @property
    def dimensions(self):
        return self._dimensions

    def get_dimension(self, dimension_type: DimensionType):
        """Return the dimension matching dimension_type.

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        DimensionConfig

        """
        for key, dim_config in self.dimensions.items():
            if key.type == dimension_type:
                return dim_config
        assert False, key

    # TODO:
    #   - check binning/partitioning / file size requirements?
    #   - check unique files
    #   - add similar validators as project_config Dimensions
    # NOTE: project_config.Dimensions has a lot of the
    #       validators we want here however, its unclear to me how we can
    #       apply them in both classes because they are root valitors and
    #       also because Dimensions includes project_dimension and
    #       supplemental_dimensions which are not required at the dataset
    #       level.
