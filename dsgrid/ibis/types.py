"""Type definitions for the dsgrid Ibis abstraction layer.

This module is the single source of truth for the user-facing type vocabulary
that may appear in :class:`dsgrid.config.file_schema.Column` declarations.
Every per-type representation (DuckDB SQL string, Spark SQL string, Ibis
dtype string, declared-family bucket, PySpark ``*Type`` class name for
reverse lookup) lives on a :class:`TypeSpec`.

Callers should not branch on backend or build their own translation tables;
use :func:`spec_for_name` (or :func:`spec_for_spark_type` for reverse
lookups) and read the field they need.
"""

from dataclasses import dataclass, field

from typing import Any

from ibis.expr.datatypes import Timestamp

import dsgrid
from dsgrid.common import BackendEngine


@dataclass(frozen=True)
class TypeSpec:
    """A type that dsgrid users may declare in a :class:`Column`.

    Parameters
    ----------
    name
        The user-facing token written in a FileSchema (e.g. ``"INT"``).
    duckdb_sql
        The DuckDB SQL type string used in ``CAST`` / ``read_csv`` calls.
    spark_sql
        The Spark SQL type string used in Spark CSV schema literals.
    ibis_dtype
        The Ibis dtype string used in ``column.cast(...)``.
    family
        Coarse compatibility bucket used to compare a declaration against a
        column's actual runtime type (e.g. when validating a declaration
        against a self-describing Parquet schema).
    bit_width
        For fixed-width numeric types, the bit width. Used to detect
        narrowing casts. ``None`` for non-numeric or variable-width types.
    spark_type_names
        PySpark ``*Type`` class names that resolve to this spec on reverse
        lookup. May be empty for alias names that share a Spark class with
        a canonical spec (e.g. ``INTEGER`` defers to ``INT``).
    """

    name: str
    duckdb_sql: str
    spark_sql: str
    ibis_dtype: str
    family: str
    bit_width: int | None = None
    spark_type_names: tuple[str, ...] = field(default_factory=tuple)


# The first value of each TypeSpec listed below is the user-facing name to use in :class:`Column`.
# The rest of the values provide synonyms and related information for use in various backend
# and query contexts.
TYPE_SPECS: tuple[TypeSpec, ...] = (
    TypeSpec(
        "BOOLEAN", "BOOLEAN", "BOOLEAN", "boolean", "bool", spark_type_names=("BooleanType",)
    ),
    TypeSpec(
        "TINYINT", "TINYINT", "TINYINT", "int8", "integer", 8, spark_type_names=("ByteType",)
    ),
    TypeSpec(
        "SMALLINT",
        "SMALLINT",
        "SMALLINT",
        "int16",
        "integer",
        16,
        spark_type_names=("ShortType",),
    ),
    TypeSpec("INT", "INTEGER", "INT", "int32", "integer", 32, spark_type_names=("IntegerType",)),
    TypeSpec("INTEGER", "INTEGER", "INT", "int32", "integer", 32),
    TypeSpec("BIGINT", "BIGINT", "BIGINT", "int64", "integer", 64, spark_type_names=("LongType",)),
    TypeSpec(
        "FLOAT", "FLOAT", "FLOAT", "float32", "floating", 32, spark_type_names=("FloatType",)
    ),
    TypeSpec(
        "DOUBLE", "DOUBLE", "DOUBLE", "float64", "floating", 64, spark_type_names=("DoubleType",)
    ),
    TypeSpec("STRING", "VARCHAR", "STRING", "string", "string", spark_type_names=("StringType",)),
    TypeSpec("TEXT", "VARCHAR", "STRING", "string", "string"),
    TypeSpec("VARCHAR", "VARCHAR", "STRING", "string", "string"),
    TypeSpec(
        "TIMESTAMP_TZ",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP",
        "timestamp('UTC')",
        "timestamp",
        spark_type_names=("TimestampType",),
    ),
    TypeSpec(
        "TIMESTAMP_NTZ",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "timestamp",
        "timestamp",
        spark_type_names=("TimestampNTZType",),
    ),
)


_BY_NAME: dict[str, TypeSpec] = {spec.name: spec for spec in TYPE_SPECS}
_BY_SPARK_TYPE: dict[str, TypeSpec] = {
    name: spec for spec in TYPE_SPECS for name in spec.spark_type_names
}
# Reverse mapping from a Spark SQL string (the value stored in
# SPARK_COLUMN_TYPES) back to a canonical TypeSpec. Only specs with a
# defined PySpark type-class name are considered canonical, so alias
# specs like INTEGER (alias for INT) and TEXT/VARCHAR (aliases for
# STRING) are not chosen as the reverse-lookup target.
_BY_SPARK_SQL: dict[str, TypeSpec] = {
    spec.spark_sql: spec for spec in TYPE_SPECS if spec.spark_type_names
}

SUPPORTED_TYPES: frozenset[str] = frozenset(_BY_NAME)

DUCKDB_COLUMN_TYPES: dict[str, str] = {spec.name: spec.duckdb_sql for spec in TYPE_SPECS}
SPARK_COLUMN_TYPES: dict[str, str] = {spec.name: spec.spark_sql for spec in TYPE_SPECS}


def spec_for_name(name: str) -> TypeSpec:
    """Look up a :class:`TypeSpec` by user-facing name.

    Raises
    ------
    KeyError
        If ``name`` is not a supported dsgrid type. Callers that surface a
        user-facing error should catch and convert to ``DSGInvalidField``.
    """
    try:
        return _BY_NAME[name.upper()]
    except KeyError as exc:
        msg = f"Unsupported dsgrid type {name!r}; supported: {sorted(SUPPORTED_TYPES)}"
        raise KeyError(msg) from exc


def spec_for_spark_sql(spark_sql: str) -> TypeSpec:
    """Look up the canonical :class:`TypeSpec` for a Spark SQL type string.

    Parameters
    ----------
    spark_sql
        A Spark SQL type identifier (e.g. ``"INT"``, ``"TIMESTAMP"``,
        ``"TIMESTAMP_NTZ"``).

    Raises
    ------
    KeyError
        If the Spark SQL string has no canonical mapping. Alias names
        (``INTEGER``, ``TEXT``, ``VARCHAR``) are not accepted because the
        canonical spec is selected by ``spec.spark_type_names``.
    """
    try:
        return _BY_SPARK_SQL[spark_sql.upper()]
    except KeyError as exc:
        msg = (
            f"No canonical TypeSpec for Spark SQL type {spark_sql!r}; "
            f"known: {sorted(_BY_SPARK_SQL)}"
        )
        raise KeyError(msg) from exc


def spec_for_spark_type(data_type: Any) -> TypeSpec:
    """Look up the :class:`TypeSpec` that corresponds to a PySpark ``*Type``.

    Parameters
    ----------
    data_type
        Any object whose ``__class__.__name__`` matches a PySpark type
        (e.g. an ``IntegerType()`` instance).

    Raises
    ------
    KeyError
        If the Spark type has no registered mapping.
    """
    class_name = data_type.__class__.__name__
    try:
        return _BY_SPARK_TYPE[class_name]
    except KeyError as exc:
        msg = f"Unsupported schema data type: {data_type}"
        raise KeyError(msg) from exc


def use_duckdb() -> bool:
    """Return True if the configured execution backend is DuckDB."""
    return dsgrid.runtime_config.backend_engine == BackendEngine.DUCKDB


def get_str_type() -> str:
    """Return the SQL string type used by the current database system."""
    spec = spec_for_name("STRING")
    return spec.duckdb_sql if use_duckdb() else spec.spark_sql


def is_tz_aware_timestamp(dtype: Any) -> bool:
    """Return True if an Ibis dtype is a timestamp carrying a time zone.

    On DuckDB this distinguishes ``TIMESTAMP WITH TIME ZONE`` (tz-aware) from a naive
    ``TIMESTAMP``; only the former honors the connection time zone on extractions such as
    ``.year()``. Spark timestamps are instant-based and report as tz-naive here even
    though they render via the session time zone.
    """
    return isinstance(dtype, Timestamp) and dtype.timezone is not None


def is_string_type(data_type: Any) -> bool:
    """Return True if a Spark-style schema data type represents a string."""
    if hasattr(data_type, "is_string"):
        return data_type.is_string()
    return data_type.__class__.__name__ == "StringType"


def is_string_column(table: Any, column: str) -> bool:
    """Return True if the table column has a string data type."""
    schema = table.schema
    if callable(schema):
        schema = schema()
    data_type = schema[column]
    if hasattr(data_type, "dataType"):
        data_type = data_type.dataType
    return is_string_type(data_type)


def _duckdb_type_from_spark_type(data_type: Any) -> str:
    try:
        return spec_for_spark_type(data_type).duckdb_sql
    except KeyError as exc:
        raise NotImplementedError(str(exc)) from exc


def _ibis_type_from_spark_type(data_type: Any) -> str:
    try:
        spec = spec_for_spark_type(data_type)
    except KeyError as exc:
        raise NotImplementedError(str(exc)) from exc
    # Strip any tz-info suffix from the dtype string (TIMESTAMP_TZ maps to
    # "timestamp('UTC')" for declared-cast purposes, but the inferred dtype
    # for an existing Spark column has no tz attached).
    return spec.ibis_dtype.split("(", 1)[0]
