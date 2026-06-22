"""Tests for the TypeSpec table in :mod:`dsgrid.ibis.types`.

Acts as a parity check across the four ways dsgrid talks about column
types (user-facing name, DuckDB SQL, Spark SQL, Ibis dtype) so that all
four stay consistent as new types are added.
"""

import ibis.expr.datatypes as dt
import pytest

from dsgrid.ibis.session import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampNTZType,
    TimestampType,
)
from dsgrid.ibis.types import (
    DUCKDB_COLUMN_TYPES,
    SPARK_COLUMN_TYPES,
    SUPPORTED_TYPES,
    TYPE_SPECS,
    TypeSpec,
    spec_for_name,
    spec_for_spark_sql,
    spec_for_spark_type,
    _duckdb_type_from_spark_type,
    _ibis_type_from_spark_type,
)


def test_supported_types_match_spec_table():
    assert SUPPORTED_TYPES == {spec.name for spec in TYPE_SPECS}


def test_duckdb_and_spark_export_dicts_match_specs():
    assert DUCKDB_COLUMN_TYPES == {spec.name: spec.duckdb_sql for spec in TYPE_SPECS}
    assert SPARK_COLUMN_TYPES == {spec.name: spec.spark_sql for spec in TYPE_SPECS}
    assert sorted(DUCKDB_COLUMN_TYPES) == sorted(SPARK_COLUMN_TYPES)


@pytest.mark.parametrize("spec", TYPE_SPECS, ids=lambda s: s.name)
def test_each_spec_has_consistent_ibis_dtype(spec: TypeSpec):
    """Each TypeSpec's ibis_dtype must parse cleanly."""
    # tz-tagged timestamps (e.g. "timestamp('UTC')") parse via dt.dtype.
    parsed = dt.dtype(spec.ibis_dtype)
    if spec.family == "integer":
        assert parsed.is_integer()
        assert parsed.nbytes * 8 == spec.bit_width
    elif spec.family == "floating":
        assert parsed.is_floating()
        assert parsed.nbytes * 8 == spec.bit_width
    elif spec.family == "bool":
        assert parsed.is_boolean()
    elif spec.family == "string":
        assert parsed.is_string()
    elif spec.family == "timestamp":
        assert parsed.is_timestamp()


def test_spark_type_names_round_trip():
    """Every spec.spark_type_names entry maps back to the same spec via spec_for_spark_type."""
    for spec in TYPE_SPECS:
        for class_name in spec.spark_type_names:
            # Build a stub object whose __class__.__name__ matches.
            stub = type(class_name, (), {})()
            assert spec_for_spark_type(stub) is spec


def test_spec_for_spark_sql_returns_canonical_spec():
    # TIMESTAMP_TZ is canonical for Spark's "TIMESTAMP" SQL string;
    # TIMESTAMP_NTZ is canonical for "TIMESTAMP_NTZ".
    assert spec_for_spark_sql("TIMESTAMP").name == "TIMESTAMP_TZ"
    assert spec_for_spark_sql("TIMESTAMP_NTZ").name == "TIMESTAMP_NTZ"
    # INT is canonical (INTEGER is an alias without spark_type_names).
    assert spec_for_spark_sql("INT").name == "INT"
    # STRING is canonical (TEXT, VARCHAR are aliases).
    assert spec_for_spark_sql("STRING").name == "STRING"


def test_spec_for_spark_sql_rejects_alias():
    """Alias values should not appear as keys (aliases share a spark_sql with a canonical spec)."""
    # No alias should win; the canonical spec is always returned.
    canonical = spec_for_spark_sql("STRING")
    assert canonical.name == "STRING"
    # Alias names like TEXT/VARCHAR must still resolve via spec_for_name.
    assert spec_for_name("TEXT").spark_sql == "STRING"
    assert spec_for_name("VARCHAR").spark_sql == "STRING"


def test_spec_for_name_is_case_insensitive():
    assert spec_for_name("int") is spec_for_name("INT")
    assert spec_for_name("timestamp_tz") is spec_for_name("TIMESTAMP_TZ")


def test_smallint_no_longer_aliased_to_integer():
    """Regression: pre-TypeSpec branch mapped DUCKDB SMALLINT to INTEGER."""
    assert DUCKDB_COLUMN_TYPES["SMALLINT"] == "SMALLINT"


def test_int_int32_bit_width():
    assert spec_for_name("INT").bit_width == 32
    assert spec_for_name("INTEGER").bit_width == 32
    assert spec_for_name("BIGINT").bit_width == 64


def test_schema_type_mappings():
    expected = [
        (BooleanType(), "boolean", "BOOLEAN"),
        (ByteType(), "int8", "TINYINT"),
        (ShortType(), "int16", "SMALLINT"),
        (IntegerType(), "int32", "INTEGER"),
        (LongType(), "int64", "BIGINT"),
        (FloatType(), "float32", "FLOAT"),
        (DoubleType(), "float64", "DOUBLE"),
        (StringType(), "string", "VARCHAR"),
        # TimestampType is Spark's TZ-aware instant type; map to DuckDB's
        # TIMESTAMP WITH TIME ZONE for round-trip parity. The prior code
        # mapped both Timestamp variants to "TIMESTAMP" and lost the TZ
        # distinction; TypeSpec separates them.
        (TimestampType(), "timestamp", "TIMESTAMP WITH TIME ZONE"),
        (TimestampNTZType(), "timestamp", "TIMESTAMP"),
    ]
    for data_type, ibis_type, duckdb_type in expected:
        assert _ibis_type_from_spark_type(data_type) == ibis_type
        assert _duckdb_type_from_spark_type(data_type) == duckdb_type


def test_schema_type_invalid():
    class UnsupportedType:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported schema data type"):
        _ibis_type_from_spark_type(UnsupportedType())


def test_duckdb_type_from_spark_type_invalid():
    class UnsupportedType:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported schema data type"):
        _duckdb_type_from_spark_type(UnsupportedType())
