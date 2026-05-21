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
            # precision, e.g. timestamp microseconds) verbatim. The FileSchema
            # declaration is documentation/validation for Parquet, not a
            # runtime type override.
            df = read_parquet(path)
        case ".csv":
            column_schema = _get_column_schema(schema)
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
    """Raise if the declared type would narrow the column's actual width.

    Detects two failure modes: an integer/float declared narrower than the
    actual width (e.g. declared INT on an int64 column), and a numeric
    declaration against a non-numeric actual type within the same family
    (e.g. declared FLOAT on an int column). The latter is intentionally not
    flagged here because it crosses subtypes within ``floating``/``integer``
    families that callers may legitimately want to bridge.
    """
    if spec.bit_width is None:
        return
    actual_bits = actual_dtype.nbytes * 8 if hasattr(actual_dtype, "nbytes") else None
    if actual_bits is None:
        return
    if spec.bit_width < actual_bits:
        msg = (
            f"Declared data_type={spec.name!r} ({spec.bit_width}-bit) would narrow "
            f"column {column_name!r} from its actual {actual_dtype} ({actual_bits}-bit). "
            f"Use a wider declaration (e.g. BIGINT for 64-bit integers) or remove "
            f"the declaration if narrowing is intentional."
        )
        raise DSGInvalidField(msg)


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
      (e.g. a string-typed column that the user knows is an integer ID).
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
        try:
            spec = spec_for_name(col.data_type)
        except KeyError as exc:
            msg = (
                f"Declared data_type={col.data_type!r} for column {col.name!r} "
                f"has no Ibis dtype mapping."
            )
            raise DSGInvalidField(msg) from exc
        actual_dtype = schema[col.name]
        if strict_family and spec.family != _actual_type_family(actual_dtype):
            continue
        _check_narrowing(spec, col.name, actual_dtype)
        _warn_if_timestamp_tz_lossy_on_spark(spec, col.name)
        casts[col.name] = df[col.name].cast(spec.ibis_dtype)
    return df.mutate(**casts) if casts else df


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


def _apply_declared_types_post_read(df: ibis.Table, schema: FileSchema) -> ibis.Table:
    """Internal: apply declared column types after reading a JSON file (strict)."""
    return apply_declared_types(df, schema.columns, strict_family=True)
