import ibis
import logging
import warnings
from pathlib import Path
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidField
from dsgrid.ibis.io import read_csv, read_json, read_parquet, write_dataframe
from dsgrid.ibis.operations import drop_columns, rename_columns
from dsgrid.ibis.tz import get_current_time_zone
from dsgrid.ibis.types import SUPPORTED_TYPES, TypeSpec, spec_for_name, use_duckdb
from dsgrid.utils.scratch_dir_context import ScratchDirContext
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
            # precision, e.g. timestamp microseconds) verbatim. Declared types
            # are checked against that schema — a declaration that disagrees
            # with the file is a config error — but never cast.
            df = read_parquet(path)
            validate_declared_types(df, schema.columns)
        case ".csv":
            column_schema = _get_column_schema(schema)
            df = read_csv(path, schema=column_schema)
        case ".json":
            # JSON cannot encode type intent: both backends infer 64-bit
            # numerics from literals and cannot mark a number as a string ID.
            # Apply user-declared types after the read so a FileSchema is
            # authoritative for JSON inputs the same way it is for CSV.
            df = read_json(path)
            df = apply_declared_types(df, schema.columns)
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


def _get_column_schema(schema: FileSchema) -> dict[str, str] | None:
    column_types = schema.get_data_type_mapping()
    if not column_types:
        return None

    mapped_schema: dict[str, str] = {}
    for key, val in column_types.items():
        try:
            spec = spec_for_name(val)
        except KeyError as exc:
            raise DSGInvalidField(str(exc)) from exc
        mapped_schema[key] = spec.duckdb_sql if use_duckdb() else spec.spark_sql
    return mapped_schema


def _actual_type_family(dtype: Any) -> str:
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


def _check_narrowing(spec: TypeSpec, column_name: str, actual_dtype: Any) -> None:
    """Raise if the declared type is too narrow to hold the column's actual values.

    Compares declared vs. actual bit width and rejects a declaration that would
    drop bits — e.g. ``INT`` (32-bit) declared on an int64 column, or ``FLOAT``
    (32-bit) declared on a float64 column. Equal-or-wider declarations pass,
    including widening casts that bridge type families (e.g. int32 -> float64).

    Only width is checked here. Cross-family conversion failures (e.g.
    non-numeric text declared BIGINT) surface from the backend when the cast
    executes.
    """
    if spec.bit_width is None:
        return
    actual_bits = actual_dtype.nbytes * 8 if hasattr(actual_dtype, "nbytes") else None
    if actual_bits is None:
        return
    if spec.bit_width < actual_bits:
        msg = (
            f"Declared data_type={spec.name!r} ({spec.bit_width}-bit) is narrower than "
            f"column {column_name!r}'s actual {actual_dtype} ({actual_bits}-bit) and would "
            f"drop data. Declare a type at least as wide (e.g. BIGINT for 64-bit integers), "
            f"or remove the declaration to keep the column's actual type."
        )
        raise DSGInvalidField(msg)


def apply_declared_types(df: ibis.Table, columns: list[Column]) -> ibis.Table:
    """Cast columns of ``df`` to match user-declared types in ``columns``.

    The declaration is authoritative: it is how a user assigns types the
    source format cannot express (a CSV or JSON number that is really a
    string ID, a timestamp's TZ-awareness). Casts apply even across type
    families. A declaration the data cannot satisfy fails loudly: narrowing
    declarations are rejected here, and invalid conversions (e.g. non-numeric
    text declared BIGINT) raise in the backend when the cast executes.

    Columns declared in ``columns`` but missing from the table are silently
    ignored; downstream validation surfaces missing required columns with a
    more useful message. Columns without a ``data_type`` keep their existing
    type.

    Parameters
    ----------
    df : ibis.Table
    columns : list[Column]

    Returns
    -------
    ibis.Table

    Raises
    ------
    DSGInvalidField
        If a declared ``data_type`` has no Ibis dtype mapping, or is narrower
        than the column's actual type.
    """
    if not columns:
        return df
    schema = df.schema()
    casts: dict[str, Any] = {}
    for col in columns:
        if col.data_type is None or col.name not in schema:
            continue
        spec = _spec_for_column(col.name, col.data_type)
        _check_narrowing(spec, col.name, schema[col.name])
        _warn_if_timestamp_tz_lossy_on_spark(spec, col.name)
        casts[col.name] = df[col.name].cast(spec.ibis_dtype)
    return df.mutate(**casts) if casts else df


def _spec_for_column(column_name: str, data_type: str) -> TypeSpec:
    try:
        return spec_for_name(data_type)
    except KeyError as exc:
        msg = (
            f"Declared data_type={data_type!r} for column {column_name!r} "
            f"has no Ibis dtype mapping."
        )
        raise DSGInvalidField(msg) from exc


def validate_declared_types(df: ibis.Table, columns: list[Column]) -> None:
    """Check declared types against a self-describing file's actual schema.

    Used for Parquet reads, whose on-disk schema is honored verbatim and never
    cast (see :func:`read_data_file`). A declaration there is documentation, so
    one that disagrees with the file — a different type family, or a narrower
    width — is a config error and raises instead of being silently ignored.
    Equal-or-wider same-family declarations pass; the column keeps the file's
    type either way.

    Raises
    ------
    DSGInvalidField
        If a declared ``data_type`` conflicts with the column's actual type.
    """
    schema = df.schema()
    for col in columns:
        if col.data_type is None or col.name not in schema:
            continue
        spec = _spec_for_column(col.name, col.data_type)
        actual_dtype = schema[col.name]
        actual_family = _actual_type_family(actual_dtype)
        if spec.family != actual_family:
            msg = (
                f"Declared data_type={spec.name!r} ({spec.family}) for column "
                f"{col.name!r} conflicts with the Parquet file's actual type "
                f"{actual_dtype} ({actual_family}). Parquet files are read with "
                f"their own schema; dsgrid does not cast them. Update the "
                f"declaration to match the file, or remove it."
            )
            raise DSGInvalidField(msg)
        _check_narrowing(spec, col.name, actual_dtype)


def _warn_if_timestamp_tz_lossy_on_spark(spec: "TypeSpec", column_name: str) -> None:
    """Warn when TIMESTAMP_TZ is declared on Spark with a non-UTC session TZ.

    Spark's ``TIMESTAMP`` type cannot carry a per-row TZ tag; it stores the
    instant as UTC microseconds and renders it through
    ``spark.sql.session.timeZone``. When the session TZ is not UTC, a
    declared TIMESTAMP_TZ silently loses the original TZ offset at render
    time. Emit a warning so callers can pin the session TZ to UTC (or
    switch to DuckDB) if they care about the rendered offset.
    """
    if spec.name != "TIMESTAMP_TZ" or use_duckdb():
        return
    try:
        current_tz = get_current_time_zone()
    except Exception:  # noqa: BLE001 - getter can fail mid-bootstrap
        return
    if current_tz.upper() == "UTC":
        return
    msg = (
        f"Column {column_name!r} declared TIMESTAMP_TZ on the Spark backend, "
        f"but spark.sql.session.timeZone={current_tz!r} (not UTC). The instant is "
        "preserved, but the per-row TZ offset will render in the session TZ. "
        "Pin the session TZ to UTC or switch to the DuckDB backend if you need "
        "the original TZ offset to survive in rendered output."
    )
    warnings.warn(msg, UserWarning, stacklevel=4)
