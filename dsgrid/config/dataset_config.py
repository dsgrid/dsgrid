from pathlib import Path
from typing import List, Optional, Dict, Union
import os
import logging
import pyspark.sql.functions as F

from pydantic import Field
from pydantic import validator
from semver import VersionInfo

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.common import check_config_id_strict
from .config_base import ConfigBase
from .dimensions import (
    DimensionReferenceModel,
)
from dsgrid.data_models import DSGBaseModel, DSGEnum, EnumValue
from dsgrid.exceptions import DSGInvalidDimension


ALLOWED_LOAD_DATA_FILENAMES = ("load_data.parquet", "load_data.csv")
ALLOWED_LOAD_DATA_LOOKUP_FILENAMES = (
    "load_data_lookup.parquet",
    "load_data_lookup.csv",
    "load_data_lookup.json",
)
ALLOWED_LOAD_DATA_ONE_TABLE_FILENAMES = ("load_data_one_table.parquet", "load_data_one_table.csv")

logger = logging.getLogger(__name__)


def check_load_data_filename(path: Path):
    """Return the load_data filename in path. Supports Parquet and CSV.

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
    """Return the load_data_lookup filename in path. Supports Parquet, CSV, and JSON.

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
    raise ValueError(f"no load_data_lookup file exists in {path}")


def check_load_data_one_table_filename(path: Path):
    """Return the load_data_one_table filename in path. Supports Parquet and CSV."""
    for allowed_name in ALLOWED_LOAD_DATA_ONE_TABLE_FILENAMES:
        filename = path / allowed_name
        if filename.exists():
            return filename

    # Use ValueError because this gets called in Pydantic model validation.
    raise ValueError(f"no load_data_one_table file exists in {path}")


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
    ONE_TABLE = EnumValue(
        value="one_table",
        description="""
        One_table data schema with load_data_one_table table. 
        Applies to small, non-timeseries datasets.
        """,
    )


class DSGDatasetParquetType(DSGEnum):
    """Dataset parquet types."""

    LOAD_DATA = EnumValue(
        value="load_data",
        description="load_data file with id, timestamp, and load value columns",
    )
    LOAD_DATA_LOOKUP = EnumValue(
        value="load_data_lookup",
        description="load_data_lookup file with dimension metadata and and ID which maps to load_data"
        "file",
    )
    LOAD_DATA_ONE_TABLE = EnumValue(
        value="load_data_one_table",
        description="load_data_one_table file with timestamp, load value, and metadata columns",
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


class OneTableDataSchemaModel(DSGBaseModel):
    """ data schema model for one table load data format """


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
    data_schema_type: DataSchemaType = Field(
        title="data_schema_type",
        description="Discriminator for data schema",
        options=DataSchemaType.format_for_docs(),
    )
    data_schema: Union[StandardDataSchemaModel, OneTableDataSchemaModel] = Field(
        title="data_schema",
        description="Schema (table layouts) used for writing out the dataset",
    )
    path: str = Field(
        title="path",
        description="Local path containing data to be registered on the remote registry.",
    )
    dataset_version: Optional[Union[VersionInfo, str]] = Field(
        title="dataset_version",
        description="The version of the dataset.",
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
    trivial_dimensions: Optional[List[DimensionType]] = Field(
        title="trivial_dimensions",
        default=[],
        description="List of trivial dimensions (i.e., 1-element dimensions) that"
        " do not exist in the load_data_lookup. List the dimensions by dimension type.",
        notes=(
            "Trivial dimensions are 1-element dimensions that are not present in the parquet data"
            " columns. Instead they are added by dsgrid as an alias column.",
        ),
    )

    @validator("data_schema", pre=True)
    def check_data_schema(cls, schema, values):
        """Check and deserialize model for data_schema"""
        # placeholder for when there's more data_schema_type
        if values["data_schema_type"] == DataSchemaType.STANDARD:
            schema = StandardDataSchemaModel(**schema)
        elif values["data_schema_type"] == DataSchemaType.ONE_TABLE:
            schema = OneTableDataSchemaModel(**schema)
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
    def check_path(cls, path, values):
        """Check dataset parquet path"""
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if path.startswith("s3://"):
            raise Exception(f"Loading a dataset from S3 is not currently supported: {path}")
        else:
            local_path = Path(path)
            if not local_path.exists():
                raise ValueError(f"{local_path} does not exist for InputDataset")

        if values["data_schema_type"] == DataSchemaType.STANDARD:
            check_load_data_filename(local_path)
            check_load_data_lookup_filename(local_path)
        elif values["data_schema_type"] == DataSchemaType.ONE_TABLE:
            check_load_data_one_table_filename(local_path)
        # else:
        #     raise ValueError(f'data_schema_type={values["data_schema_type"]} not supported.')

        # TODO: check dataset_dimension_mapping (optional) if exists
        # TODO: check project_dimension_mapping (optional) if exists

        return str(local_path)

    @validator("trivial_dimensions")
    def check_time_not_trivial(cls, trivial_dimensions):
        for dim in trivial_dimensions:
            if dim == DimensionType.TIME:
                raise ValueError(
                    "The time dimension is currently not a dsgrid supported trivial dimension."
                )
        return trivial_dimensions


class DatasetConfig(ConfigBase):
    """Provides an interface to a DatasetConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._dimensions = {}
        self._src_dir = None

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
        config.src_dir = config_file.parent
        config.load_dimensions(dimension_manager)
        return config

    @property
    def src_dir(self):
        """Return the directory containing the config file. Data files inside the config file
        are relative to this.

        """
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        """Set the source directory. Must be the directory containing the config file."""
        self._src_dir = Path(src_dir)

    @property
    def load_data_path(self):
        return check_load_data_filename(self._src_dir / self.model.path)

    @property
    def load_data_lookup_path(self):
        return check_load_data_lookup_filename(self._src_dir / self.model.path)

    @property
    def load_data_one_table_path(self):
        return check_load_data_one_table_filename(self._src_dir / self.model.path)

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
        assert False, dimension_type

    def add_trivial_dimensions(self, load_data_lookup):
        """Add trivial 1-element dimensions to load_data_lookup."""
        for dim in self._dimensions.values():
            if dim.model.dimension_type in self.model.trivial_dimensions:
                self._check_trivial_record_length(dim.model.records)
                val = dim.model.records[0].id
                col = dim.model.dimension_type.value
                load_data_lookup = load_data_lookup.withColumn(col, F.lit(val))
        return load_data_lookup

    def _check_trivial_record_length(self, records):
        """Check that trivial dimensions have only 1 record."""
        if len(records) > 1:
            raise DSGInvalidDimension(
                f"Trivial dimensions must have only 1 record but {len(records)} records found for dimension: {records}"
            )
