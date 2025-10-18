import logging
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Union

from pydantic import field_validator, model_validator, Field

from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.config.common import make_base_dimension_template
from dsgrid.config.dimension_config import (
    DimensionBaseConfig,
    DimensionBaseConfigWithFiles,
)
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.models import (
    TableFormatModelBase,
    PivotedTableFormatModel,
    UnpivotedTableFormatModel,
    TableFormatModel,
    TableFormatType,
)
from dsgrid.dimension.base_models import DimensionType, check_timezone_in_geography
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.common import check_config_id_strict
from dsgrid.data_models import DSGBaseDatabaseModel, DSGBaseModel, DSGEnum, EnumValue
from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.spark.types import (
    DataFrame,
    F,
)
from dsgrid.utils.spark import get_unique_values, read_dataframe
from dsgrid.utils.utilities import check_uniqueness
from .config_base import ConfigBase
from .dimensions import (
    DimensionsListModel,
    DimensionReferenceModel,
    DimensionModel,
    TimeDimensionBaseModel,
)


# Note that there is special handling for S3 at use sites.
ALLOWED_LOAD_DATA_FILENAMES = ("load_data.parquet", "load_data.csv", "table.parquet")
ALLOWED_LOAD_DATA_LOOKUP_FILENAMES = (
    "load_data_lookup.parquet",
    "lookup_table.parquet",
    # The next two are only used for test data.
    "load_data_lookup.csv",
    "load_data_lookup.json",
)
ALLOWED_DATA_FILES = ALLOWED_LOAD_DATA_FILENAMES + ALLOWED_LOAD_DATA_LOOKUP_FILENAMES
ALLOWED_MISSING_DIMENSION_ASSOCATIONS_FILENAMES = (
    "missing_associations.csv",
    "missing_associations.parquet",
)

logger = logging.getLogger(__name__)


def check_load_data_filename(path: str | Path) -> Path:
    """Return the load_data filename in path. Supports Parquet and CSV.

    Parameters
    ----------
    path : str | Path

    Returns
    -------
    Path

    Raises
    ------
    ValueError
        Raised if no supported load data filename exists.

    """
    path_ = path if isinstance(path, Path) else Path(path)
    if str(path_).startswith("s3://"):
        # Only Parquet is supported on AWS.
        return path_ / "/load_data.parquet"

    for allowed_name in ALLOWED_LOAD_DATA_FILENAMES:
        filename = path_ / allowed_name
        if filename.exists():
            return filename

    # Use ValueError because this gets called in Pydantic model validation.
    msg = f"no load_data file exists in {path_}"
    raise ValueError(msg)


def check_load_data_lookup_filename(path: str | Path) -> Path:
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
    path_ = path if isinstance(path, Path) else Path(path)
    if str(path_).startswith("s3://"):
        # Only Parquet is supported on AWS.
        return path_ / "/load_data_lookup.parquet"

    for allowed_name in ALLOWED_LOAD_DATA_LOOKUP_FILENAMES:
        filename = path_ / allowed_name
        if filename.exists():
            return filename

    # Use ValueError because this gets called in Pydantic model validation.
    msg = f"no load_data_lookup file exists in {path_}"
    raise ValueError(msg)


class InputDatasetType(DSGEnum):
    MODELED = "modeled"
    HISTORICAL = "historical"
    BENCHMARK = "benchmark"
    UNSPECIFIED = "unspecified"


class DataSchemaType(str, Enum):
    """Data schema types."""

    STANDARD = "standard"
    ONE_TABLE = "one_table"


class DSGDatasetParquetType(DSGEnum):
    """Dataset parquet types."""

    LOAD_DATA = EnumValue(
        value="load_data",
        description="""
        In STANDARD data_schema_type, load_data is a file with ID, timestamp, and metric value columns.
        In ONE_TABLE data_schema_type, load_data is a file with multiple data dimension and metric value columns.
        """,
    )
    LOAD_DATA_LOOKUP = EnumValue(
        value="load_data_lookup",
        description="""
        load_data_lookup is a file with multiple data dimension columns and an ID column that maps to load_data file.
        """,
    )


class DataClassificationType(DSGEnum):
    """Data risk classification type.

    See FIPS 199, https://csrc.nist.gov/files/pubs/fips/199/final/docs/fips-pub-199-final.pdf
    for more information. In general these classifications describe potential impact on
    organizations and individuals. In more detailed schemes a separate classification could
    be applied to confidentiality, integrity, and availability.
    """

    LOW = EnumValue(
        value="low",
        description=(
            "The loss of confidentiality, integrity, or availability could be "
            "expected to have a limited adverse effect on organizational operations, "
            "organizational assets, or individuals."
        ),
    )
    MODERATE = EnumValue(
        value="moderate",
        description=(
            "The loss of confidentiality, integrity, or availability could be expected "
            "to have a serious adverse effect on organizational operations, organizational "
            "assets, or individuals."
        ),
    )


class StandardDataSchemaModel(DSGBaseModel):
    data_schema_type: Literal[DataSchemaType.STANDARD]
    table_format: TableFormatModel

    @model_validator(mode="before")
    @classmethod
    def handle_legacy(cls, values: dict) -> dict:
        if "load_data_column_dimension" in values:
            values["table_format"] = PivotedTableFormatModel(
                pivoted_dimension_type=values.pop("load_data_column_dimension")
            )
        return values


class OneTableDataSchemaModel(DSGBaseModel):
    data_schema_type: Literal[DataSchemaType.ONE_TABLE]
    table_format: TableFormatModel

    @model_validator(mode="before")
    @classmethod
    def handle_legacy(cls, values: dict) -> dict:
        if "load_data_column_dimension" in values:
            values["table_format"] = PivotedTableFormatModel(
                pivoted_dimension_type=values.pop("load_data_column_dimension")
            )
        return values


class DatasetQualifierType(str, Enum):
    QUANTITY = "quantity"
    GROWTH_RATE = "growth_rate"


class GrowthRateType(str, Enum):
    EXPONENTIAL_ANNUAL = "exponential_annual"
    EXPONENTIAL_MONTHLY = "exponential_monthly"


class QuantityModel(DSGBaseModel):
    dataset_qualifier_type: Literal[DatasetQualifierType.QUANTITY]


class GrowthRateModel(DSGBaseModel):
    dataset_qualifier_type: Literal[DatasetQualifierType.GROWTH_RATE]
    growth_rate_type: GrowthRateType = Field(
        title="growth_rate_type",
        description="Type of growth rates, e.g., exponential_annual",
    )


class DatasetConfigModel(DSGBaseDatabaseModel):
    """Represents dataset configurations."""

    dataset_id: str = Field(
        title="dataset_id",
        description="Unique dataset identifier.",
    )
    dataset_type: InputDatasetType = Field(
        default=InputDatasetType.UNSPECIFIED,
        title="dataset_type",
        description="Input dataset type.",
        json_schema_extra={
            "options": InputDatasetType.format_for_docs(),
        },
    )
    dataset_qualifier_metadata: Union[QuantityModel, GrowthRateModel] = Field(
        default=QuantityModel(dataset_qualifier_type=DatasetQualifierType.QUANTITY),
        title="dataset_qualifier_metadata",
        description="Additional metadata to include related to the dataset_qualifier",
        discriminator="dataset_qualifier_type",
    )
    data_schema: Union[StandardDataSchemaModel, OneTableDataSchemaModel] = Field(
        title="data_schema",
        description="Schema (table layouts) used for writing out the dataset",
        discriminator="data_schema_type",
    )
    description: str | None = Field(
        default=None,
        title="description",
        description="A detailed description of the dataset",
    )
    sector_description: str | None = Field(
        default=None,
        title="sector_description",
        description="Sectoral description (e.g., residential, commercial, industrial, "
        "transportation, electricity)",
    )
    data_source: str | None = Field(
        default=None,
        title="data_source",
        description="Original data source name, e.g. 'ComStock', 'EIA 861'.",
    )
    # for old data, port from origin_date
    data_source_date: str | None = Field(
        default=None,
        title="data_source_date",
        description="Date or year the original source data were published, e.g., '2021' for 'EIA AEO 2021'.",
    )
    # for old data, port from origin_version or drop
    data_source_version: str | None = Field(
        default=None,
        title="data_source_version",
        description=(
            "Source data version, if applicable. For example, could specify preliminary "
            "versus final data."
        ),
    )
    data_source_authors: list[str] | None = Field(
        default=None,
        title="data_source_authors",
        description="List of authors for the original data source.",
    )
    data_source_doi_url: str | None = Field(
        default=None,
        title="data_source_doi_url",
        description="Original data source doi or other url",
    )
    origin_creator: str | None = Field(
        default=None,
        title="origin_creator",
        description="First and last name of the person who formatted this dataset for dsgrid",
    )
    origin_organization: str | None = Field(
        default=None,
        title="origin_organization",
        description="Organization name of the origin_creator, e.g., 'NREL'",
    )
    origin_contributors: list[str] | None = Field(
        default=None,
        title="origin_contributors",
        description=(
            "List of contributors to the compilation of this dataset for dsgrid, "
            " e.g., ['Harry Potter', 'Ronald Weasley']"
        ),
    )
    origin_project: str | None = Field(
        default=None,
        title="origin_project",
        description=(
            "Name of the project for/from which this dataset was compiled, e.g., "
            "'IEF', 'Building Standard Scenarios'."
        ),
    )
    # ETH@20251008 - Although we could define a default DataClassificationType,
    # it seems better to default to 'low' by priniting in the template, so that
    # the base assumption of low risk is clear to dataset contributors.
    user_defined_metadata: dict[str, Any] = Field(
        title="user_defined_metadata",
        description="Additional user defined metadata fields",
        default={},
    )
    tags: list[str] | None = Field(
        default=None,
        title="tags",
        description="List of data tags",
    )
    data_classification: DataClassificationType = Field(
        title="data_classification",
        description="Data security classification (e.g., low, moderate).",
        json_schema_extra={
            "options": DataClassificationType.format_for_docs(),
        },
    )
    enable_unit_conversion: bool = Field(
        default=True,
        description="If the dataset uses its dimension mapping for the metric dimension to also "
        "perform unit conversion, then this value should be false.",
    )
    # This field must be listed before dimensions.
    use_project_geography_time_zone: bool = Field(
        default=False,  # ETH@20251008 - Default here conflicts with make_unvalidated_dataset_config
        description="If true, time zones will be applied from the project's geography dimension. "
        "If false, the dataset's geography dimension records must provide a time zone column.",
    )
    dimensions: DimensionsListModel = Field(
        title="dimensions",
        description="List of dimensions that make up the dimensions of dataset. They will be "
        "automatically registered during dataset registration and then converted "
        "to dimension_references.",
        default=[],
    )
    dimension_references: list[DimensionReferenceModel] = Field(
        title="dimensions",
        description="List of registered dimension references that make up the dimensions of dataset.",
        default=[],
    )
    trivial_dimensions: list[DimensionType] = Field(
        title="trivial_dimensions",
        default=[],
        description="List of trivial dimensions (i.e., 1-element dimensions) that "
        "do not exist in the load_data_lookup. List the dimensions by dimension type. "
        "Trivial dimensions are 1-element dimensions that are not present in the parquet data "
        "columns. Instead they are added by dsgrid as an alias column.",
    )

    # This function can be deleted once all dataset repositories have been updated.
    @model_validator(mode="before")
    @classmethod
    def handle_legacy_fields(cls, values):
        if "dataset_version" in values:
            val = values.pop("dataset_version")
            if val is not None:
                values["version"] = val

        if "data_schema_type" in values:
            if "data_schema_type" in values["data_schema"]:
                msg = f"Unknown data_schema format: {values=}"
                raise ValueError(msg)
            values["data_schema"]["data_schema_type"] = values.pop("data_schema_type")

        if "leap_day_adjustment" in values:
            if values["leap_day_adjustment"] != "none":
                msg = f"Unknown leap day adjustment: {values=}"
                raise ValueError(msg)
            values.pop("leap_day_adjustment")

        if "source" in values:
            values.pop("source")

        if "origin_date" in values:
            val = values.pop("origin_date")
            if val is not None:
                values["data_source_date"] = val

        if "origin_version" in values:
            val = values.pop("origin_version")
            if val is not None:
                values["data_source_version"] = val

        return values

    @field_validator("dataset_id")
    @classmethod
    def check_dataset_id(cls, dataset_id):
        """Check dataset ID validity"""
        check_config_id_strict(dataset_id, "Dataset")
        return dataset_id

    @field_validator("trivial_dimensions")
    @classmethod
    def check_time_not_trivial(cls, trivial_dimensions):
        for dim in trivial_dimensions:
            if dim == DimensionType.TIME:
                msg = "The time dimension is currently not a dsgrid supported trivial dimension."
                raise ValueError(msg)
        return trivial_dimensions

    @field_validator("dimensions")
    @classmethod
    def check_files(cls, values: list) -> list:
        """Validate dimension files are unique across all dimensions"""
        check_uniqueness(
            (
                x.filename
                for x in values
                if isinstance(x, DimensionModel) and x.filename is not None
            ),
            "dimension record filename",
        )
        return values

    @field_validator("dimensions")
    @classmethod
    def check_names(cls, values: list) -> list:
        """Validate dimension names are unique across all dimensions."""
        check_uniqueness(
            [dim.name for dim in values],
            "dimension record name",
        )
        return values

    @model_validator(mode="after")
    def check_time_zone(self) -> "DatasetConfigModel":
        """Validate whether required time zone information is present."""
        geo_requires_time_zone = False
        time_dim = None
        if not self.use_project_geography_time_zone:
            for dimension in self.dimensions:
                if dimension.dimension_type == DimensionType.TIME:
                    assert isinstance(dimension, TimeDimensionBaseModel)
                    geo_requires_time_zone = dimension.is_time_zone_required_in_geography()
                    time_dim = dimension
                    break

        if geo_requires_time_zone:
            for dimension in self.dimensions:
                if dimension.dimension_type == DimensionType.GEOGRAPHY:
                    check_timezone_in_geography(
                        dimension,
                        err_msg=f"Dataset with time dimension {time_dim} requires that its "
                        "geography dimension records include a time_zone column.",
                    )

        return self


def make_unvalidated_dataset_config(
    dataset_id,
    metric_type: str,
    table_format: TableFormatModelBase = UnpivotedTableFormatModel(),
    data_classification=DataClassificationType.LOW.value,
    dataset_type=InputDatasetType.UNSPECIFIED,
    included_dimensions: list[DimensionType] | None = None,
    time_type: TimeDimensionType | None = None,
    use_project_geography_time_zone: bool = False,
    dimension_references: list[DimensionReferenceModel] | None = None,
    trivial_dimensions: list[DimensionType] | None = None,
    slim: bool = True,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a dataset config as a dictionary, skipping validation."""
    trivial_dimensions_ = trivial_dimensions or []
    exclude_dimension_types = {x.dimension_type for x in dimension_references or []}
    if included_dimensions is not None:
        for dim_type in set(DimensionType).difference(included_dimensions):
            exclude_dimension_types.add(dim_type)

    dimensions = make_base_dimension_template(
        [metric_type],
        exclude_dimension_types=exclude_dimension_types,
        time_type=time_type,
    )

    result = None
    if slim:
        result = {
            "dataset_id": dataset_id,
            "version": "1.0.0",
            "dataset_type": dataset_type.value,
            "data_schema": {
                "data_schema_type": DataSchemaType.ONE_TABLE.value,
                "table_format": table_format.model_dump(mode="json"),
            },
            "description": "",
            "data_classification": data_classification,
            "use_project_geography_time_zone": use_project_geography_time_zone,
            "dimensions": dimensions,
            "dimension_references": [
                x.model_dump(mode="json") for x in dimension_references or []
            ],
            "trivial_dimensions": [x.value for x in trivial_dimensions_],
        }
    else:
        result = {
            "dataset_id": dataset_id,
            "version": "1.0.0",
            "dataset_type": dataset_type.value,
            "dataset_qualifier_metadata": {
                "dataset_qualifier_type": DatasetQualifierType.QUANTITY.value
            },
            "data_schema": {
                "data_schema_type": DataSchemaType.ONE_TABLE.value,
                "table_format": table_format.model_dump(mode="json"),
            },
            "description": "",
            "sector_description": "",
            "data_source": "",
            "data_source_date": "",
            "data_source_version": "",
            "data_source_authors": [],
            "data_source_doi_url": "",
            "origin_creator": "",
            "origin_organization": "",
            "origin_contributors": [],
            "origin_project": "",
            "user_defined_metadata": {},
            "tags": [],
            "data_classification": data_classification,
            "enable_unit_conversion": True,
            "use_project_geography_time_zone": use_project_geography_time_zone,
            "dimensions": dimensions,
            "dimension_references": [
                x.model_dump(mode="json") for x in dimension_references or []
            ],
            "trivial_dimensions": [x.value for x in trivial_dimensions_],
        }

    if metadata:
        result.update(metadata)

    return result


class DatasetConfig(ConfigBase):
    """Provides an interface to a DatasetConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._dimensions = {}  # ConfigKey to DimensionConfig
        self._dataset_path: Path | None = None

    @staticmethod
    def config_filename():
        return "dataset.json5"

    @property
    def config_id(self):
        return self._model.dataset_id

    @staticmethod
    def model_class():
        return DatasetConfigModel

    @classmethod
    def load_from_user_path(cls, config_file, dataset_path) -> "DatasetConfig":
        config = cls.load(config_file)
        schema_type = config.get_data_schema_type()
        if str(dataset_path).startswith("s3://"):
            # TODO: This may need to handle AWS s3 at some point.
            msg = "Registering a dataset from an S3 path is not supported."
            raise DSGInvalidParameter(msg)
        if not dataset_path.exists():
            msg = f"Dataset {dataset_path} does not exist"
            raise DSGInvalidParameter(msg)
        dataset_path = str(dataset_path)
        if schema_type == DataSchemaType.STANDARD:
            check_load_data_filename(dataset_path)
            check_load_data_lookup_filename(dataset_path)
        elif schema_type == DataSchemaType.ONE_TABLE:
            check_load_data_filename(dataset_path)
        else:
            msg = f"data_schema_type={schema_type} not supported."
            raise DSGInvalidParameter(msg)

        config.dataset_path = dataset_path
        return config

    @property
    def dataset_path(self) -> Path | None:
        """Return the directory containing the dataset file(s)."""
        return self._dataset_path

    @dataset_path.setter
    def dataset_path(self, dataset_path: Path | str | None) -> None:
        """Set the dataset path."""
        if isinstance(dataset_path, str):
            dataset_path = Path(dataset_path)
        self._dataset_path = dataset_path

    @property
    def load_data_path(self):
        assert self._dataset_path is not None
        return check_load_data_filename(self._dataset_path)

    @property
    def load_data_lookup_path(self):
        assert self._dataset_path is not None
        return check_load_data_lookup_filename(self._dataset_path)

    def update_dimensions(self, dimensions):
        """Update all dataset dimensions."""
        self._dimensions.update(dimensions)

    @property
    def dimensions(self):
        return self._dimensions

    def get_dimension(self, dimension_type: DimensionType) -> DimensionBaseConfig | None:
        """Return the dimension matching dimension_type."""
        for dim_config in self.dimensions.values():
            if dim_config.model.dimension_type == dimension_type:
                return dim_config
        return None

    def get_time_dimension(self) -> TimeDimensionBaseConfig | None:
        """Return the time dimension of the dataset."""
        dim = self.get_dimension(DimensionType.TIME)
        assert dim is None or isinstance(dim, TimeDimensionBaseConfig)
        return dim

    def get_dimension_with_records(
        self, dimension_type: DimensionType
    ) -> DimensionBaseConfigWithFiles | None:
        """Return the dimension matching dimension_type."""
        for dim_config in self.dimensions.values():
            if dim_config.model.dimension_type == dimension_type and isinstance(
                dim_config, DimensionBaseConfigWithFiles
            ):
                return dim_config
        return None

    def get_pivoted_dimension_type(self) -> DimensionType | None:
        """Return the table's pivoted dimension type or None if the table isn't pivoted."""
        if self.get_table_format_type() != TableFormatType.PIVOTED:
            return None
        return self.model.data_schema.table_format.pivoted_dimension_type

    def get_pivoted_dimension_columns(self) -> list[str]:
        """Return the table's pivoted dimension columns or an empty list if the table isn't
        pivoted.
        """
        if self.get_table_format_type() != TableFormatType.PIVOTED:
            return []
        dim_type = self.model.data_schema.table_format.pivoted_dimension_type
        dim = self.get_dimension_with_records(dim_type)
        assert dim is not None
        return sorted(list(dim.get_unique_ids()))

    def get_value_columns(self) -> list[str]:
        """Return the table's columns that contain values."""
        match self.get_table_format_type():
            case TableFormatType.PIVOTED:
                return self.get_pivoted_dimension_columns()
            case TableFormatType.UNPIVOTED:
                return [VALUE_COLUMN]
            case _:
                raise NotImplementedError(str(self.get_table_format_type()))

    def get_data_schema_type(self) -> DataSchemaType:
        """Return the schema type of the table."""
        return DataSchemaType(self.model.data_schema.data_schema_type)

    def get_table_format_type(self) -> TableFormatType:
        """Return the format type of the table."""
        return TableFormatType(self._model.data_schema.table_format.format_type)

    def add_trivial_dimensions(self, df: DataFrame):
        """Add trivial 1-element dimensions to load_data_lookup."""
        for dim in self._dimensions.values():
            if dim.model.dimension_type in self.model.trivial_dimensions:
                self._check_trivial_record_length(dim.model.records)
                val = dim.model.records[0].id
                col = dim.model.dimension_type.value
                df = df.withColumn(col, F.lit(val))
        return df

    def remove_trivial_dimensions(self, df):
        trivial_cols = {d.value for d in self.model.trivial_dimensions}
        select_cols = [col for col in df.columns if col not in trivial_cols]
        return df[select_cols]

    def _check_trivial_record_length(self, records):
        """Check that trivial dimensions have only 1 record."""
        if len(records) > 1:
            msg = f"Trivial dimensions must have only 1 record but {len(records)} records found for dimension: {records}"
            raise DSGInvalidDimension(msg)


def get_unique_dimension_record_ids(
    path: Path,
    schema_type: DataSchemaType,
    pivoted_dimension_type: DimensionType | None,
    time_columns: set[str],
) -> dict[DimensionType, list[str]]:
    """Get the unique dimension record IDs from a table."""
    if schema_type == DataSchemaType.STANDARD:
        ld = read_dataframe(check_load_data_filename(path))
        lk = read_dataframe(check_load_data_lookup_filename(path))
        df = ld.join(lk, on="id").drop("id")
    elif schema_type == DataSchemaType.ONE_TABLE:
        ld_path = check_load_data_filename(path)
        df = read_dataframe(ld_path)
    else:
        msg = f"Unsupported schema type: {schema_type}"
        raise NotImplementedError(msg)

    ids_by_dimension_type: dict[DimensionType, list[str]] = {}
    for dimension_type in DimensionType:
        if dimension_type.value in df.columns:
            ids_by_dimension_type[dimension_type] = sorted(
                get_unique_values(df, dimension_type.value)
            )
    if pivoted_dimension_type is not None:
        if pivoted_dimension_type.value in df.columns:
            msg = f"{pivoted_dimension_type=} cannot be in the dataframe columns."
            raise DSGInvalidParameter(msg)
        dimension_type_columns = {x.value for x in DimensionType}
        dimension_type_columns.update(time_columns)
        dimension_type_columns.update({"id", SCALING_FACTOR_COLUMN})
        pivoted_columns = set(df.columns) - dimension_type_columns
        ids_by_dimension_type[pivoted_dimension_type] = sorted(pivoted_columns)

    return ids_by_dimension_type
