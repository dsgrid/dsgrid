import logging
from pathlib import Path
from typing import Self

from pydantic import Field, field_validator, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidField
from dsgrid.spark.functions import read_csv_duckdb, read_json, read_parquet
from dsgrid.spark.types import DataFrame
from dsgrid.utils.utilities import check_uniqueness


SUPPORTED_TYPES = set(
    (
        "BOOLEAN",
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "TIMESTAMP_TZ",
        "TIMESTAMP_NTZ",
        "STRING",
        "TEXT",
        "VARCHAR",
    )
)

DUCKDB_COLUMN_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "TINYINT": "TINYINT",
    "SMALLINT": "INTEGER",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "VARCHAR": "VARCHAR",
}

SPARK_COLUMN_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "INT": "INT",
    "INTEGER": "INT",
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "STRING": "STRING",
    "TEXT": "STRING",
    "VARCHAR": "STRING",
    "TIMESTAMP_TZ": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
}

assert sorted(DUCKDB_COLUMN_TYPES.keys()) == sorted(SPARK_COLUMN_TYPES.keys())
assert not SUPPORTED_TYPES.difference(DUCKDB_COLUMN_TYPES.keys())

logger = logging.getLogger(__name__)


class Column(DSGBaseModel):
    name: str = Field(description="Name of the column")
    dimension_type: DimensionType | None = Field(
        default=None,
        description="Dimension represented by the data in the column. Optional if this is a "
        "time column or pivoted column. Required if the column represents a stacked dimension "
        "but an alternate name is being used, such as 'county' instead of 'geography'. "
        "dsgrid will rename any column that is set at runtime.",
    )
    data_type: str | None = Field(
        description="Type of the data in the column. If None, infer the type."
    )

    @field_validator("data_type")
    @classmethod
    def check_data_type(cls, data_type: str | None) -> str | None:
        if data_type is None:
            return None

        type_upper = data_type.upper()
        if type_upper not in SUPPORTED_TYPES:
            supported_data_types = sorted(SUPPORTED_TYPES)
            msg = f"{data_type=} is not one of {supported_data_types=}"
            raise ValueError(msg)
        return type_upper


class FileSchema(DSGBaseModel):
    """Defines the format of a data file (CSV, JSON, Parquet)."""

    path: str | None = Field(description="Path to the file. Must be assigned during registration.")
    columns: list[Column] = Field(
        default=[], description="Custom schema for the columns in the file."
    )
    ignore_columns: list[str] = Field(
        default=[],
        description="List of column names to ignore (drop) when reading the file.",
    )

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if len(self.columns) > 1:
            check_uniqueness((x.name for x in self.columns), "column names")

        # Check that ignore_columns don't overlap with columns
        column_names = {x.name for x in self.columns}
        ignore_set = set(self.ignore_columns)
        overlap = column_names & ignore_set
        if overlap:
            msg = f"Columns cannot be in both 'columns' and 'ignore_columns': {overlap}"
            raise ValueError(msg)

        return self

    def get_data_type_mapping(self) -> dict[str, str]:
        """Return the mapping of column to data type."""
        return {x.name: x.data_type for x in self.columns if x.data_type is not None}


def read_data_file(schema: FileSchema) -> DataFrame:
    """Read a data file from a schema.

    Parameters
    ----------
    schema : FileSchema
        Schema defining the file path and column types.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing the file data.
    """
    if schema.path is None:
        msg = "File path is not assigned"
        raise DSGInvalidDataset(msg)

    path = Path(schema.path)
    if not path.exists():
        msg = f"{path} does not exist"
        raise FileNotFoundError(msg)

    expected_columns = {x.name for x in schema.columns}

    match path.suffix:
        case ".parquet":
            df = read_parquet(path)
        case ".csv":
            column_schema = _get_column_schema(schema, DUCKDB_COLUMN_TYPES)
            df = read_csv_duckdb(path, schema=column_schema)
        case ".json":
            df = read_json(path)
        case _:
            msg = f"Unsupported file type: {path.suffix}"
            raise DSGInvalidDataset(msg)

    actual_columns = set(df.columns)
    diff = expected_columns.difference(actual_columns)
    if diff:
        msg = f"Expected columns {diff} are not in {actual_columns=}"
        raise DSGInvalidDataset(msg)

    df = _drop_ignored_columns(df, schema.ignore_columns)
    renames = _get_column_renames(schema)
    df = _rename_columns(df, renames)
    return df


def _get_column_renames(schema: FileSchema) -> dict[str, str]:
    """Return a mapping of columns to rename."""
    mapping: dict[str, str] = {}
    for column in schema.columns:
        if column.dimension_type is not None and column.name != column.dimension_type.value:
            mapping[column.name] = column.dimension_type.value
    return mapping


def _rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
        logger.info("Renamed column %s to %s", old_name, new_name)
    return df


def _drop_ignored_columns(df: DataFrame, ignore_columns: list[str]) -> DataFrame:
    if not ignore_columns:
        return df

    existing_columns = set(df.columns)
    for col in ignore_columns:
        if col in existing_columns:
            df = df.drop(col)
            logger.info("Dropped ignored column: %s", col)
        else:
            logger.warning("Ignored column '%s' not found in file", col)
    return df


def _get_column_schema(schema: FileSchema, backend_mapping: dict) -> dict[str, str] | None:
    column_types = schema.get_data_type_mapping()
    if not column_types:
        return None

    mapped_schema: dict[str, str] = {}
    for key, val in column_types.items():
        col_type = val.upper()
        if col_type not in backend_mapping:
            options = " ".join(sorted(backend_mapping.keys()))
            msg = f"column type = {val} is not supported. {options=}"
            raise DSGInvalidField(msg)
        mapped_schema[key] = backend_mapping[col_type]
    return mapped_schema
