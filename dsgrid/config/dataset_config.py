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


ALLOWED_LOAD_DATA_FILENAMES = ("load_data.parquet", "load_data.csv")
ALLOWED_LOAD_DATA_LOOKUP_FILENAMES = (
    "load_data_lookup.parquet",
    "load_data_lookup.csv",
    "load_data_lookup.json",
)

logger = logging.getLogger(__name__)


def check_load_data_filename(path: Path):
    """Return the load data filename in path. Supports Parquet and CSV.

    Parameters
    ----------
    path : Path

    Returns
    -------
    Path

    Raises
    ------
    ValueError
        Raised if no supported load data filename exists.

    """
    for allowed_name in ALLOWED_LOAD_DATA_FILENAMES:
        filename = path / allowed_name
        if filename.exists():
            return filename

    # Use ValueError because this gets called in Pydantic model validation.
    raise ValueError(f"no load_data file exists in {path}")


def check_load_data_lookup_filename(path: Path):
    """Return the load data lookup filename in path. Supports Parquet, CSV, and JSON.

    Parameters
    ----------
    path : Path

    Returns
    -------
    Path

    Raises
    ------
    ValueError
        Raised if no supported load data lookup filename exists.

    """
    for allowed_name in ALLOWED_LOAD_DATA_LOOKUP_FILENAMES:
        filename = path / allowed_name
        if filename.exists():
            return filename

    # Use ValueError because this gets called in Pydantic model validation.
    raise ValueError(f"no load_data lookup file exists in {path}")


class InputDatasetType(DSGEnum):
    SECTOR_MODEL = "sector_model"
    HISTORICAL = "historical"
    BENCHMARK = "benchmark"


class DataSchemaType(DSGEnum):
    """Data schema types."""

    STANDARD = EnumValue(
        value="standard",
        description="""
        Standard data schema with load_data and load_data_lookup tables. 
        Applies to datasets for which the data are provided in full.
        """,
    )


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


class DataClassificationType(DSGEnum):
    """Data risk classification type.

    See https://uit.stanford.edu/guide/riskclassifications for more information.
    """

    # TODO: can we get NREL/DOE definitions for these instead of standford's?

    LOW = EnumValue(
        value="low",
        description="Low risk data that does not require special data management",
    )
    MODERATE = EnumValue(
        value="moderate",
        description=(
            "The moderate class includes all data under an NDA, data classified as business sensitive, data classification as Critical Energy Infrastructure Infromation (CEII), or data with Personal Identifiable Information (PII)."
        ),
    )


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


class StandardDataSchemaModel(DSGBaseModel):
    load_data_column_dimension: DimensionType = Field(
        title="load_data_column_dimension",
        description="Columns in the load_data table are records of this dimension type.",
    )


class DatasetConfigModel(DSGBaseModel):
    """Represents dataset configurations."""

    dataset_id: str = Field(
        title="dataset_id",
        description="Unique dataset identifier.",
        requirements=(
            "When registering a dataset to a project, the dataset_id must match the expected ID "
            "defined in the project config.",
            "For posterity, dataset_id cannot be the same as the ``data_source``"
            " (e.g., dataset cannot be 'ComStock')",
        ),
    )
    dataset_type: InputDatasetType = Field(
        title="dataset_type",
        description="Input dataset type.",
        options=InputDatasetType.format_for_docs(),
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
        options=DataClassificationType.format_for_docs(),
    )
    tags: Optional[List[str]] = Field(
        title="source",
        description="List of data tags",
        required=False,
    )
    data_schema_type: DataSchemaType = Field(
        title="data_schema_type",
        description="Discriminator for data schema",
        options=DataSchemaType.format_for_docs(),
    )
    data_schema: StandardDataSchemaModel = Field(
        title="data_schema",
        description="Schema (table layouts) used for writing out the dataset",
    )  # Once we have another schema type this will become Union[StandardDataSchemaModel, OtherSchemaModel]
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
    user_defined_metadata: Optional[Dict] = Field(
        title="user_defined_metadata",
        description="Additional user defined metadata fields",
        default={},
        required=False,
    )

    @validator("data_schema", pre=True)
    def check_data_schema(cls, schema, values):
        """Check and deserialize model for data_schema"""
        # placeholder for when there's more data_schema_type
        if values["data_schema_type"] == DataSchemaType.STANDARD:
            schema = StandardDataSchemaModel(**schema)
        else:
            raise ValueError(
                f'Cannot load data_schema model for data_schema_type={values["data_schema_type"]}'
            )
        return schema

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

        check_load_data_filename(local_path)
        check_load_data_lookup_filename(local_path)

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
