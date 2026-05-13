import ibis
import logging
from pathlib import Path
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidField
from dsgrid.ibis.io import read_csv, read_json, read_parquet
from dsgrid.ibis.operations import drop_columns, rename_columns
from dsgrid.ibis.types import DUCKDB_COLUMN_TYPES, SPARK_COLUMN_TYPES, SUPPORTED_TYPES, use_duckdb
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.session import write_dataframe
from dsgrid.utils.utilities import check_uniqueness


logger = logging.getLogger(__name__)


class Column(DSGBaseModel):
    name: str = Field(description="Name of the column")
    dimension_type: DimensionType | None = Field(
        default=None,
        description="Dimension represented by the data in the column. Optional if this is a "
        "time column or pivoted column. Required if the column represents a stacked dimension "
        "but an alternate name is being used, such as 'county' instead of 'geography'. "
        "dsgrid will rename any column that is set at runtime, writing out the result to the "
        "registry's data directory. The original dataset is not modified.",
    )
    data_type: str | None = Field(
        default=None, description="Type of the data in the column. If None, infer the type."
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


def read_data_file(
    schema: FileSchema, scratch_dir_context: ScratchDirContext | None = None
) -> ibis.Table:
    """Read a data file from a schema.

    Parameters
    ----------
    schema : FileSchema
        Schema defining the file path and column types.
    scratch_dir_context : ScratchDirContext
        Optional location to write temporary files.

    Returns
    -------
    Ibis table
        An Ibis table containing the file data.
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
            # Parquet is self-describing; honor its on-disk schema (including
            # precision, e.g. timestamp microseconds) verbatim. The FileSchema
            # declaration is documentation/validation for Parquet, not a
            # runtime type override.
            df = read_parquet(path)
        case ".csv":
            backend_types = DUCKDB_COLUMN_TYPES if use_duckdb() else SPARK_COLUMN_TYPES
            column_schema = _get_column_schema(schema, backend_types)
            df = read_csv(path, schema=column_schema)
        case ".json":
            # JSON readers infer types from content (DuckDB) or default to
            # strings (Spark). Apply user-declared types after the read so a
            # FileSchema is the single source of truth for JSON inputs the
            # same way it is for CSV.
            df = read_json(path)
            df = _apply_declared_types_post_read(df, schema)
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
    if renames:
        df = _rename_columns(df, renames)
        if scratch_dir_context is None:
            renamed_path = path.with_stem(path.stem + "_renamed")
            logger.warning(
                "Creating temporary file at %s. Pass scratch_dir_context to avoid this.",
                renamed_path,
            )
        else:
            renamed_path = scratch_dir_context.get_temp_filename(suffix=path.suffix)
        write_dataframe(df, renamed_path, overwrite=True)
        schema.path = str(renamed_path)
        for column in schema.columns:
            if column.name in renames:
                column.name = renames[column.name]
                column.dimension_type = None
    return df


def _get_column_renames(schema: FileSchema) -> dict[str, str]:
    """Return a mapping of columns to rename."""
    mapping: dict[str, str] = {}
    for column in schema.columns:
        if column.dimension_type is not None and column.name != column.dimension_type.value:
            mapping[column.name] = column.dimension_type.value
    return mapping


def _rename_columns(df: ibis.Table, mapping: dict[str, str]) -> ibis.Table:
    df = rename_columns(df, mapping)
    for old_name, new_name in mapping.items():
        logger.info("Renamed column %s to %s", old_name, new_name)
    return df


def _drop_ignored_columns(df: ibis.Table, ignore_columns: list[str]) -> ibis.Table:
    if not ignore_columns:
        return df

    existing_columns = set(df.columns)
    for col in ignore_columns:
        if col in existing_columns:
            df = drop_columns(df, col)
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


# Map the FileSchema data_type vocabulary (see dsgrid.ibis.types.SUPPORTED_TYPES)
# to Ibis dtype strings accepted by ``Column.cast``. Used to apply declared
# types after reading JSON, since the JSON readers don't accept a read-time
# schema argument the way CSV does.
_USER_TYPE_TO_IBIS_DTYPE = {
    "BOOLEAN": "bool",
    "INT": "int32",
    "INTEGER": "int32",
    "TINYINT": "int8",
    "SMALLINT": "int16",
    "BIGINT": "int64",
    "FLOAT": "float32",
    "DOUBLE": "float64",
    "STRING": "string",
    "TEXT": "string",
    "VARCHAR": "string",
    "TIMESTAMP_NTZ": "timestamp",
    "TIMESTAMP_TZ": "timestamp('UTC')",
}


def _declared_type_family(declared: str) -> str:
    """Bucket a user-facing type name into a coarse compatibility family.

    Only casts within the same family are considered safe enough to apply
    silently. Cross-family declarations (e.g. declared VARCHAR but the data
    came in as int64) are left alone so existing validators can surface the
    real error instead of dsgrid silently coercing the data.
    """
    if declared in {"BOOLEAN"}:
        return "bool"
    if declared in {"INT", "INTEGER", "TINYINT", "SMALLINT", "BIGINT"}:
        return "integer"
    if declared in {"FLOAT", "DOUBLE"}:
        return "floating"
    if declared in {"STRING", "TEXT", "VARCHAR"}:
        return "string"
    if declared in {"TIMESTAMP_NTZ", "TIMESTAMP_TZ"}:
        return "timestamp"
    msg = f"Declared data_type={declared!r} has no family mapping."
    raise DSGInvalidField(msg)


def _actual_type_family(dtype) -> str:
    """Coarse family bucket for an Ibis runtime dtype."""
    if dtype.is_boolean():
        return "bool"
    if dtype.is_integer():
        return "integer"
    if dtype.is_floating():
        return "floating"
    if dtype.is_string():
        return "string"
    if dtype.is_timestamp():
        return "timestamp"
    return "other"


def apply_declared_types(
    df: ibis.Table,
    columns: list[Column],
    *,
    strict_family: bool = True,
) -> ibis.Table:
    """Cast columns of ``df`` to match user-declared types in ``columns``.

    Used in two contexts that differ in how much the framework trusts the
    declaration:

    - Registered datasets (after a JSON read): the FileSchema is declarative;
      cross-family mismatches usually indicate bad input data and should fail
      loudly via dsgrid's downstream validators. Pass ``strict_family=True``
      so this function only normalizes width within the same type family
      (e.g. int32 ↔ int64) and skips int → string or similar cross-family
      casts.
    - CLI ``generate-config --schema-file``: the user is feeding the readers
      authoritative hints for raw files that have no registered schema yet
      (e.g. a CSV read as all-VARCHAR that the user knows is an integer ID).
      Pass ``strict_family=False`` so declared types always take effect.

    Columns declared in ``columns`` but missing from the table are silently
    ignored; downstream validation surfaces missing required columns with a
    more useful message. Columns without a ``data_type`` keep their existing
    type.

    Parameters
    ----------
    df : ibis.Table
    columns : list[Column]
    strict_family : bool, optional
        If True, skip casts that cross type families. By default True.

    Returns
    -------
    ibis.Table

    Raises
    ------
    DSGInvalidField
        If a declared ``data_type`` has no mapping in
        :data:`_USER_TYPE_TO_IBIS_DTYPE`.
    """
    if not columns:
        return df
    schema = df.schema()
    casts: dict[str, Any] = {}
    for col in columns:
        if col.data_type is None or col.name not in schema:
            continue
        dtype = _USER_TYPE_TO_IBIS_DTYPE.get(col.data_type)
        if dtype is None:
            msg = (
                f"Declared data_type={col.data_type!r} for column {col.name!r} "
                f"has no Ibis dtype mapping."
            )
            raise DSGInvalidField(msg)
        if strict_family and _declared_type_family(col.data_type) != _actual_type_family(
            schema[col.name]
        ):
            continue
        casts[col.name] = df[col.name].cast(dtype)
    return df.mutate(**casts) if casts else df


def _apply_declared_types_post_read(df: ibis.Table, schema: FileSchema) -> ibis.Table:
    """Internal: apply declared column types after reading a JSON file (strict)."""
    return apply_declared_types(df, schema.columns, strict_family=True)
