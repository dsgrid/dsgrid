from enum import Enum
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
from dsgrid.data_models import DSGBaseModel, DSGEnum, EnumValue


LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"

logger = logging.getLogger(__name__)


class InputDatasetType(DSGEnum):
    """Dataset types for a input dataset."""

    SECTOR_MODEL = "sector_model"
    HISTORICAL = "historical"
    BENCHMARK = "benchmark"


class DSGDatasetParquetType(DSGEnum):
    """Dataset parquet types."""

    LOAD_DATA = EnumValue(
        value="load_data",
        description="Load data file with id, timestamp, and load value columns",
    )
    LOAD_DATA_LOOKUP = EnumValue(
        value="load_data_lookup",
        description="Load data file with dimension metadata and and ID which maps to load_data"
        "file",
    )
    # # These are not currently supported by dsgrid but may be needed in the near future
    # DATASET_DIMENSION_MAPPING = EnumValue(
    #     value="dataset_dimension_mapping",
    #     description="",
    # )  # optional
    # PROJECT_DIMENSION_MAPPING = EnumValue(
    #     value="project_dimension_mapping",
    #     description="",
    # )  # optional


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
        options=DSGDatasetParquetType.format_for_docs(),
    )
    directory: str = Field(
        title="directory",
        description="Directory with parquet files",
    )


class DatasetConfigModel(DSGBaseModel):
    """Represents dataset configurations."""

    dataset_id: str = Field(
        title="dataset_id",
        description="Unique dataset identifier.",
        requirements=(
            "When registering a dataset to a project, the dataset_id must match the expected ID "
            "defined in the project config.",
            "For posterity, dataset_id cannot be the same as the model name (e.g., dataset cannot"
            " be 'ComStock')",
        )
        # TODO: what are the requirements again? list them in the notes
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="Input dataset type.",
        options=f"{InputDatasetType.format_for_docs()}",
    )
    # TODO: This must be validated against the project's dimension records for data_source
    # TODO: This must also be validated against the project_config
    data_source: str = Field(
        title="data_source",
        description="Data source name, e.g. 'ComStock'.",
        requirements=(
            "When registering a dataset to a project, the `data_source` field must match one of "
            "the dimension ID records defined by the project's base data source dimension.",
        ),
        # TODO: it would be nice to extend the description here with a CLI example of how to list the project's data source IDs.
    )
    path: str = Field(
        title="path",
        description="Local path containing data to be registered on the remote registry.",
    )
    description: str = Field(
        title="description",
        description="A detailed description of the dataset.",
    )
    load_data_column_dimension: DimensionType = Field(
        title="load_data_column_dimension",
        description="Columns in the load_data table are records of this dimension type.",
    )
    dimensions: List[DimensionReferenceModel] = Field(
        title="dimensions",
        description="List of registered dimension references that make up the dimensions of dataset.",
        requirements=(
            "* All :class:`~dsgrid.dimension.base_models.DimensionType` must be defined",
            "* Only one dimension reference per type is allowed",
            "* Each reference is to an existing registered dimension.",
        )
        # TODO: Add to notes - link to registering dimensions page
        # TODO: Add to notes - link to example of how to list dimensions to find existing registered dimensions
    )
    # TODO: Metdata is TBD
    metadata: Optional[Dict] = Field(
        title="metdata",
        description=" TBD. metadata information such as origin_date, creator, contacts, organization, tags, etc.",
    )

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
