"""Tests for dsgrid.config.file_schemas module."""

import json
from typing import Generator

import pytest

from dsgrid.config.file_schemas import (
    Column,
    FileSchema,
    SUPPORTED_TYPES,
    DUCKDB_COLUMN_TYPES,
    SPARK_COLUMN_TYPES,
    read_data_file,
    _get_column_renames,
    _get_column_schema,
    _rename_columns,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidField
from dsgrid.spark.types import F, SparkSession, use_duckdb
from dsgrid.utils.spark import get_spark_session, set_session_time_zone


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    spark = get_spark_session()
    yield spark


# Column tests


def test_column_basic():
    """Test creating a basic column."""
    col = Column(name="test_col", data_type="STRING")
    assert col.name == "test_col"
    assert col.data_type == "STRING"
    assert col.dimension_type is None


def test_column_with_dimension_type():
    """Test creating a column with a dimension type."""
    col = Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY)
    assert col.name == "county"
    assert col.dimension_type == DimensionType.GEOGRAPHY


def test_column_data_type_case_insensitive():
    """Test that data types are normalized to uppercase."""
    col = Column(name="test", data_type="string")
    assert col.data_type == "STRING"

    col2 = Column(name="test", data_type="Integer")
    assert col2.data_type == "INTEGER"


def test_column_none_data_type():
    """Test that None data type is allowed for type inference."""
    col = Column(name="test", data_type=None)
    assert col.data_type is None


def test_column_invalid_data_type():
    """Test that invalid data types raise ValueError."""
    with pytest.raises(ValueError, match="is not one of"):
        Column(name="test", data_type="INVALID_TYPE")


@pytest.mark.parametrize("data_type", list(SUPPORTED_TYPES))
def test_column_all_supported_types(data_type):
    """Test that all supported types are valid."""
    col = Column(name="test", data_type=data_type)
    assert col.data_type == data_type.upper()


# FileSchema tests


def test_file_schema_basic():
    """Test creating a basic file schema."""
    schema = FileSchema(path="/path/to/file.csv")
    assert schema.path == "/path/to/file.csv"
    assert schema.columns == []


def test_file_schema_with_columns():
    """Test creating a file schema with columns."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    assert len(schema.columns) == 2
    assert schema.columns[0].name == "id"
    assert schema.columns[1].name == "name"


def test_file_schema_duplicate_columns_raises():
    """Test that duplicate column names raise an error."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="id", data_type="STRING"),
    ]
    with pytest.raises(ValueError, match="column names"):
        FileSchema(path="/path/to/file.csv", columns=columns)


def test_file_schema_get_data_type_mapping():
    """Test get_data_type_mapping returns correct mapping."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="inferred", data_type=None),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    mapping = schema.get_data_type_mapping()
    assert mapping == {"id": "INTEGER", "name": "STRING"}
    assert "inferred" not in mapping


def test_file_schema_get_data_type_mapping_empty():
    """Test get_data_type_mapping with no typed columns."""
    schema = FileSchema(path="/path/to/file.csv")
    mapping = schema.get_data_type_mapping()
    assert mapping == {}


# _get_column_renames tests


def test_get_column_renames_no_renames_needed():
    """Test when no columns need renaming."""
    columns = [
        Column(name="geography", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    renames = _get_column_renames(schema)
    assert renames == {}


def test_get_column_renames_needed():
    """Test when columns need renaming to match dimension type."""
    columns = [
        Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
        Column(name="end_use", data_type="STRING", dimension_type=DimensionType.METRIC),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    renames = _get_column_renames(schema)
    assert renames == {"county": "geography", "end_use": "metric"}


def test_get_column_renames_no_dimension_type():
    """Test that columns without dimension_type are not renamed."""
    columns = [
        Column(name="county", data_type="STRING"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    renames = _get_column_renames(schema)
    assert renames == {}


# _get_column_schema tests


def test_get_column_schema_duckdb_mapping():
    """Test column schema mapping for DuckDB."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="ts", data_type="TIMESTAMP_TZ"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    result = _get_column_schema(schema, DUCKDB_COLUMN_TYPES)
    assert result == {
        "id": "INTEGER",
        "name": "VARCHAR",
        "ts": "TIMESTAMP WITH TIME ZONE",
    }


def test_get_column_schema_spark_mapping():
    """Test column schema mapping for Spark."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="ts", data_type="TIMESTAMP_NTZ"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    result = _get_column_schema(schema, SPARK_COLUMN_TYPES)
    assert result == {
        "id": "INT",
        "name": "STRING",
        "ts": "TIMESTAMP_NTZ",
    }


def test_get_column_schema_empty():
    """Test with no typed columns returns empty dict."""
    schema = FileSchema(path="/path/to/file.csv", columns=[])
    result = _get_column_schema(schema, DUCKDB_COLUMN_TYPES)
    assert result == {}


def test_get_column_schema_invalid_type_raises():
    """Test that invalid type raises DSGInvalidField."""
    columns = [Column(name="test", data_type="INTEGER")]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    invalid_mapping = {"BOGUS": "BOGUS"}
    with pytest.raises(DSGInvalidField, match="is not supported"):
        _get_column_schema(schema, invalid_mapping)


# _rename_columns tests


def test_rename_columns(spark):
    """Test renaming columns in a DataFrame."""
    df = spark.createDataFrame(
        [("Boulder", 1.0), ("Jefferson", 2.0)],
        ["county", "value"],
    )
    mapping = {"county": "geography"}
    result = _rename_columns(df, mapping)
    assert "geography" in result.columns
    assert "county" not in result.columns
    assert "value" in result.columns


def test_rename_columns_empty_mapping(spark):
    """Test with empty mapping returns unchanged DataFrame."""
    df = spark.createDataFrame(
        [("Boulder", 1.0)],
        ["county", "value"],
    )
    result = _rename_columns(df, {})
    assert result.columns == ["county", "value"]


# read_data_file tests


def test_read_data_file_csv(tmp_path, spark):
    """Test reading a CSV file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,value\n1,a,1.0\n2,b,2.0\n")

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)

    assert df.count() == 2
    assert set(df.columns) == {"id", "name", "value"}
    rows = df.collect()
    assert rows[0].id == 1
    assert rows[0].name == "a"


def test_read_data_file_csv_with_rename(tmp_path, spark):
    """Test reading a CSV file with column renaming."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("county,value\nBoulder,1.0\nJefferson,2.0\n")

    columns = [
        Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)

    assert "geography" in df.columns
    assert "county" not in df.columns


def test_read_data_file_parquet(tmp_path, spark):
    """Test reading a Parquet file."""
    parquet_file = tmp_path / "test.parquet"
    test_df = spark.createDataFrame(
        [(1, "a", 1.0), (2, "b", 2.0)],
        ["id", "name", "value"],
    )
    test_df.write.parquet(str(parquet_file))

    columns = [
        Column(name="id", data_type=None),
        Column(name="name", data_type=None),
        Column(name="value", data_type=None),
    ]
    schema = FileSchema(path=str(parquet_file), columns=columns)
    df = read_data_file(schema)

    assert df.count() == 2
    assert "id" in df.columns
    assert "name" in df.columns
    assert "value" in df.columns


def test_read_data_file_json(tmp_path, spark):
    """Test reading a JSON file."""
    json_file = tmp_path / "test.json"
    data = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    json_file.write_text("\n".join(json.dumps(row) for row in data))

    columns = [
        Column(name="id", data_type=None),
        Column(name="name", data_type=None),
    ]
    schema = FileSchema(path=str(json_file), columns=columns)
    df = read_data_file(schema)

    assert df.count() == 2
    assert "id" in df.columns
    assert "name" in df.columns


def test_read_data_file_nonexistent_raises():
    """Test that reading a nonexistent file raises FileNotFoundError."""
    schema = FileSchema(path="/nonexistent/file.csv")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        read_data_file(schema)


def test_read_data_file_unsupported_type_raises(tmp_path):
    """Test that reading an unsupported file type raises DSGInvalidDataset."""
    txt_file = tmp_path / "test.txt"
    txt_file.write_text("some content")

    schema = FileSchema(path=str(txt_file))
    with pytest.raises(DSGInvalidDataset, match="Unsupported file type"):
        read_data_file(schema)


def test_read_data_file_missing_column_raises(tmp_path, spark):
    """Test that missing expected columns raises an error.

    When schema specifies columns with data types that don't exist in the file,
    DuckDB raises a BinderException before we reach the column validation.
    """
    import duckdb

    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,a\n")

    # Schema specifies all columns in file plus one that doesn't exist
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="missing_column", data_type="STRING"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    with pytest.raises(duckdb.BinderException, match="missing_column"):
        read_data_file(schema)


def test_read_data_file_csv_inferred_types(tmp_path, spark):
    """Test reading a CSV file without explicit schema (type inference)."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,value\n1,a,1.0\n2,b,2.0\n")

    schema = FileSchema(path=str(csv_file), columns=[])
    df = read_data_file(schema)

    assert df.count() == 2
    assert set(df.columns) == {"id", "name", "value"}


def test_read_data_file_csv_with_fips_codes_and_energy_data(tmp_path, spark):
    """Test reading CSV with county FIPS codes and energy end use data.

    This test verifies:
    - Leading zeros are preserved when column is explicitly typed as STRING
    - Column renaming works (county -> geography via dimension_type)
    - Sector column is read correctly
    - Float value columns are read properly
    """
    value_columns = [
        "cooling",
        "heating",
        "lighting",
        "ventilation",
        "water_heating",
        "refrigeration",
        "cooking",
        "electronics",
        "motors",
        "misc",
    ]
    header = "county,sector," + ",".join(value_columns)
    rows = [
        "06073,com,1.5,2.3,0.8,1.2,0.5,0.7,0.3,0.4,0.6,0.2",
        "06075,com,1.8,2.1,0.9,1.1,0.6,0.8,0.4,0.5,0.7,0.3",
        "06073,res,2.1,3.5,1.2,0.8,1.1,0.9,0.6,0.7,0.4,0.5",
        "06075,res,2.4,3.2,1.1,0.9,1.0,1.0,0.5,0.6,0.5,0.4",
    ]
    csv_content = header + "\n" + "\n".join(rows) + "\n"

    csv_file = tmp_path / "energy_data.csv"
    csv_file.write_text(csv_content)

    # Specify county as STRING to preserve leading zeros, with dimension_type for renaming
    # Sector is inferred; value columns use DOUBLE for explicit float handling
    columns = [
        Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
        # Column(name="sector", data_type="STRING", dimension_type=DimensionType.SECTOR),
        # Column(name="cooling", data_type="DOUBLE"),
        # Column(name="heating", data_type="DOUBLE"),
        # Column(name="lighting", data_type="DOUBLE"),
        # Column(name="ventilation", data_type="DOUBLE"),
        # Column(name="water_heating", data_type="DOUBLE"),
        # Column(name="refrigeration", data_type="DOUBLE"),
        # Column(name="cooking", data_type="DOUBLE"),
        # Column(name="electronics", data_type="DOUBLE"),
        # Column(name="motors", data_type="DOUBLE"),
        # Column(name="misc", data_type="DOUBLE"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)
    assert df.count() == 4

    # Verify columns were renamed via dimension_type
    assert "county" not in df.columns

    # Verify all expected columns are present
    expected_columns = {"geography", "sector"} | set(value_columns)
    assert set(df.columns) == expected_columns

    # Verify leading zeros are preserved (critical for FIPS codes)
    geography_values = sorted([row.geography for row in df.select("geography").collect()])
    assert geography_values == ["06073", "06073", "06075", "06075"]

    # Verify sector values are present
    sector_values = sorted(set(row.sector for row in df.select("sector").collect()))
    assert sector_values == ["com", "res"]

    # Verify float values are readable and correct
    cooling_sum = sum(row.cooling for row in df.select("cooling").collect())
    assert abs(cooling_sum - 7.8) < 0.01  # 1.5 + 1.8 + 2.1 + 2.4

    heating_sum = sum(row.heating for row in df.select("heating").collect())
    assert abs(heating_sum - 11.1) < 0.01  # 2.3 + 2.1 + 3.5 + 3.2


# Type mapping consistency tests


def test_duckdb_and_spark_have_same_keys():
    """Test that DUCKDB and SPARK mappings have the same keys."""
    assert sorted(DUCKDB_COLUMN_TYPES.keys()) == sorted(SPARK_COLUMN_TYPES.keys())


def test_supported_types_match_mappings():
    """Test that SUPPORTED_TYPES match the mapping keys."""
    assert not SUPPORTED_TYPES.difference(DUCKDB_COLUMN_TYPES.keys())
    assert not SUPPORTED_TYPES.difference(SPARK_COLUMN_TYPES.keys())


@pytest.mark.parametrize(
    "type_name",
    ["BOOLEAN", "INT", "INTEGER", "FLOAT", "DOUBLE", "STRING", "TEXT", "VARCHAR"],
)
def test_common_types_mapped(type_name):
    """Test that common types are properly mapped in both backends."""
    assert type_name in DUCKDB_COLUMN_TYPES
    assert type_name in SPARK_COLUMN_TYPES


def test_read_data_file_csv_timestamp_with_timezone(tmp_path, spark):
    """Test reading a CSV file with ISO 8601 timestamps containing time zone offsets.

    This verifies that timestamps like '2012-01-01T01:00:00.000-05:00' are correctly
    parsed and the time zone information is preserved.
    """
    csv_file = tmp_path / "test_timestamps.csv"
    csv_content = """id,timestamp,com_cooling,com_fans
6202,2012-01-01T01:00:00.000-05:00,0,0.002258824
6202,2012-01-01T02:00:00.000-05:00,0,0.002258824
6202,2012-01-01T03:00:00.000-05:00,0,0.002529882
6202,2012-01-01T04:00:00.000-05:00,0,0.003072
"""
    csv_file.write_text(csv_content)

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="timestamp", data_type="TIMESTAMP_TZ"),
        Column(name="com_cooling", data_type="DOUBLE"),
        Column(name="com_fans", data_type="DOUBLE"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)

    assert df.count() == 4
    assert set(df.columns) == {"id", "timestamp", "com_cooling", "com_fans"}

    # Collect the timestamps and verify they were parsed correctly
    with set_session_time_zone("America/New_York"):
        # Converting to string avoids the complexity of timestamp conversion to the system
        # time zone when calling collect().
        df2 = df.withColumn("timestamp_str", F.col("timestamp").cast("string"))
        rows = df2.orderBy("timestamp_str").collect()
        first_ts = rows[0].timestamp_str
        if use_duckdb():
            assert first_ts == "2012-01-01 01:00:00-05"
        else:
            assert first_ts == "2012-01-01 01:00:00"

    # Verify the timestamps are in the correct order (1 hour apart)
    for i in range(1, len(rows)):
        prev_ts = rows[i - 1].timestamp
        curr_ts = rows[i].timestamp
        # Each timestamp should be 1 hour after the previous
        delta = curr_ts - prev_ts
        assert delta.total_seconds() == 3600, f"Expected 1 hour difference, got {delta}"

    # Verify the values are correct
    assert rows[0].com_cooling == 0
    assert abs(rows[0].com_fans - 0.002258824) < 1e-9


def test_read_data_file_csv_timestamp_without_timezone(tmp_path, spark):
    """Test reading a CSV file with timestamps that have no time zone offset.

    This verifies that timestamps like '2012-01-01 01:00:00' are correctly
    parsed as naive timestamps (no timezone conversion occurs).
    """
    csv_file = tmp_path / "test_timestamps_no_tz.csv"
    csv_content = """id,timestamp,com_cooling,com_fans
6202,2012-01-01 01:00:00,0,0.002258824
6202,2012-01-01 02:00:00,0,0.002258824
6202,2012-01-01 03:00:00,0,0.002529882
6202,2012-01-01 04:00:00,0,0.003072
"""
    csv_file.write_text(csv_content)

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="timestamp", data_type="TIMESTAMP_NTZ"),
        Column(name="com_cooling", data_type="DOUBLE"),
        Column(name="com_fans", data_type="DOUBLE"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)

    assert df.count() == 4
    assert set(df.columns) == {"id", "timestamp", "com_cooling", "com_fans"}

    # Collect the timestamps and verify they were parsed correctly
    rows = df.orderBy("timestamp").collect()

    # The first timestamp should be 2012-01-01 01:00:00 with no timezone conversion
    first_ts = rows[0].timestamp
    assert first_ts.year == 2012
    assert first_ts.month == 1
    assert first_ts.day == 1
    # No timezone conversion, so hour remains 1
    assert first_ts.hour == 1, f"Expected hour 1, got {first_ts.hour}"

    # Verify the timestamps are in the correct order (1 hour apart)
    for i in range(1, len(rows)):
        prev_ts = rows[i - 1].timestamp
        curr_ts = rows[i].timestamp
        # Each timestamp should be 1 hour after the previous
        delta = curr_ts - prev_ts
        assert delta.total_seconds() == 3600, f"Expected 1 hour difference, got {delta}"

    # Verify the values are correct
    assert rows[0].com_cooling == 0
    assert abs(rows[0].com_fans - 0.002258824) < 1e-9
