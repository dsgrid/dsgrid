"""Tests for dsgrid.config.file_schema module."""

import json
import warnings
from datetime import datetime
from typing import Generator
from zoneinfo import ZoneInfo

import ibis
import pytest

from dsgrid.config import file_schema as _fs
from dsgrid.config.file_schema import (
    Column,
    FileSchema,
    apply_declared_types,
    read_data_file,
    _actual_type_family,
    _drop_ignored_columns,
    _get_column_renames,
    _get_column_schema,
    _rename_columns,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidField
from dsgrid.ibis.types import SUPPORTED_TYPES, spec_for_name
from dsgrid.ibis.session import (
    SparkSession,
    get_runtime_session,
    use_duckdb,
)
from dsgrid.ibis.io import read_parquet, write_dataframe
from dsgrid.ibis.tz import custom_time_zone

from dsgrid.ibis.table_utils import count_rows
from tests._helpers import collect as _collect, order_by as _order_by


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    spark = get_runtime_session()
    yield spark


def _with_string_column(df: ibis.Table, column: str, alias: str):
    return df.mutate(**{alias: df[column].cast("string")})

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

    # FileSchema ignore_columns tests


def test_file_schema_ignore_columns_basic():
    """Test creating a file schema with ignore_columns."""
    schema = FileSchema(
        path="/path/to/file.csv",
        ignore_columns=["col1", "col2"],
    )
    assert schema.ignore_columns == ["col1", "col2"]


def test_file_schema_ignore_columns_default_empty():
    """Test that ignore_columns defaults to empty list."""
    schema = FileSchema(path="/path/to/file.csv")
    assert schema.ignore_columns == []


def test_file_schema_ignore_columns_overlap_with_columns_raises():
    """Test that overlapping columns and ignore_columns raises an error."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
    ]
    with pytest.raises(ValueError, match="cannot be in both"):
        FileSchema(
            path="/path/to/file.csv",
            columns=columns,
            ignore_columns=["name", "other"],
        )


def test_file_schema_ignore_columns_no_overlap_allowed():
    """Test that non-overlapping columns and ignore_columns is valid."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
    ]
    schema = FileSchema(
        path="/path/to/file.csv",
        columns=columns,
        ignore_columns=["extra_col"],
    )
    assert schema.ignore_columns == ["extra_col"]

    # _drop_ignored_columns tests


def test_drop_ignored_columns_basic(spark):
    """Test dropping columns from an Ibis table."""
    df = spark.createDataFrame(
        [(1, "a", 1.0), (2, "b", 2.0)],
        ["id", "name", "value"],
    )
    result = _drop_ignored_columns(df, ["name"])
    assert set(result.columns) == {"id", "value"}
    assert count_rows(result) == 2


def test_drop_ignored_columns_multiple(spark):
    """Test dropping multiple columns."""
    df = spark.createDataFrame(
        [(1, "a", 1.0, "x"), (2, "b", 2.0, "y")],
        ["id", "name", "value", "extra"],
    )
    result = _drop_ignored_columns(df, ["name", "extra"])
    assert set(result.columns) == {"id", "value"}


def test_drop_ignored_columns_empty_list(spark):
    """Test with empty ignore list returns unchanged Ibis table."""
    df = spark.createDataFrame(
        [(1, "a", 1.0)],
        ["id", "name", "value"],
    )
    result = _drop_ignored_columns(df, [])
    assert set(result.columns) == {"id", "name", "value"}


def test_drop_ignored_columns_nonexistent_column_warns(spark, caplog):
    """Test that dropping a nonexistent column logs a warning."""
    df = spark.createDataFrame(
        [(1, "a")],
        ["id", "name"],
    )
    result = _drop_ignored_columns(df, ["nonexistent"])
    assert set(result.columns) == {"id", "name"}
    assert "not found" in caplog.text


def test_drop_ignored_columns_mixed_existing_nonexistent(spark, caplog):
    """Test dropping mix of existing and nonexistent columns."""
    df = spark.createDataFrame(
        [(1, "a", 1.0)],
        ["id", "name", "value"],
    )
    result = _drop_ignored_columns(df, ["name", "nonexistent"])
    assert set(result.columns) == {"id", "value"}
    assert "not found" in caplog.text

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


def test_get_column_schema_picks_backend_specific_strings():
    """Each declared type maps to its backend-specific SQL string."""
    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="ts", data_type="TIMESTAMP_TZ"),
    ]
    schema = FileSchema(path="/path/to/file.csv", columns=columns)
    result = _get_column_schema(schema)
    if use_duckdb():
        assert result == {
            "id": "INTEGER",
            "name": "VARCHAR",
            "ts": "TIMESTAMP WITH TIME ZONE",
        }
    else:
        # Spark TIMESTAMP cannot carry a per-row TZ tag — TIMESTAMP_TZ
        # degrades to plain TIMESTAMP; STRING is the Spark canonical name.
        assert result == {
            "id": "INT",
            "name": "STRING",
            "ts": "TIMESTAMP",
        }


def test_get_column_schema_empty():
    """Test with no typed columns returns None."""
    schema = FileSchema(path="/path/to/file.csv", columns=[])
    result = _get_column_schema(schema)
    assert not result

    # _rename_columns tests


def test_rename_columns(spark):
    """Test renaming columns in an Ibis table."""
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
    """Test with empty mapping returns unchanged Ibis table."""
    df = spark.createDataFrame(
        [("Boulder", 1.0)],
        ["county", "value"],
    )
    result = _rename_columns(df, {})
    assert list(result.columns) == ["county", "value"]

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

    assert count_rows(df) == 2
    assert set(df.columns) == {"id", "name", "value"}
    rows = _collect(df)
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
    write_dataframe(test_df, parquet_file)

    columns = [
        Column(name="id", data_type=None),
        Column(name="name", data_type=None),
        Column(name="value", data_type=None),
    ]
    schema = FileSchema(path=str(parquet_file), columns=columns)
    df = read_data_file(schema)

    assert count_rows(df) == 2
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

    assert count_rows(df) == 2
    assert "id" in df.columns
    assert "name" in df.columns


def test_read_data_file_json_declared_string_casts(tmp_path, spark):
    """A STRING declaration overrides JSON numeric inference.

    JSON cannot mark a number as a string ID, so the FileSchema declaration
    is authoritative — the same contract as CSV.
    """
    json_file = tmp_path / "test.json"
    data = [{"model_year": 2030, "value": 1.5}, {"model_year": 2040, "value": 2.5}]
    json_file.write_text("\n".join(json.dumps(row) for row in data))

    columns = [Column(name="model_year", data_type="STRING")]
    schema = FileSchema(path=str(json_file), columns=columns)
    df = read_data_file(schema)

    assert str(df.schema()["model_year"]) == "string"
    assert sorted(row.model_year for row in _collect(df)) == ["2030", "2040"]


def test_read_data_file_json_narrowing_declaration_raises(tmp_path, spark):
    """JSON inference always yields 64-bit numerics, so a 32-bit declaration
    is rejected rather than silently narrowing."""
    json_file = tmp_path / "test.json"
    json_file.write_text(json.dumps({"model_year": 2030}))

    columns = [Column(name="model_year", data_type="INT")]
    schema = FileSchema(path=str(json_file), columns=columns)
    with pytest.raises(DSGInvalidField, match="is narrower than"):
        read_data_file(schema)


def test_read_data_file_parquet_declared_types_validated_not_cast(tmp_path, spark):
    """Matching or wider same-family declarations pass; the file's type wins.

    Parquet is self-describing, so declarations are validated against the
    on-disk schema but never cast — a declared BIGINT on an int32 column
    passes and the column stays int32.
    """
    parquet_file = tmp_path / "test.parquet"
    test_df = ibis.memtable({"id": [1, 2], "name": ["a", "b"]}).cast({"id": "int32"})
    write_dataframe(test_df, parquet_file)

    columns = [
        Column(name="id", data_type="BIGINT"),
        Column(name="name", data_type="STRING"),
    ]
    schema = FileSchema(path=str(parquet_file), columns=columns)
    df = read_data_file(schema)

    assert str(df.schema()["id"]) == "int32"
    assert str(df.schema()["name"]) == "string"


def test_read_data_file_parquet_family_mismatch_raises(tmp_path, spark):
    """A declaration whose type family disagrees with the Parquet schema is a
    config error, not a silent no-op."""
    parquet_file = tmp_path / "test.parquet"
    test_df = spark.createDataFrame([(6037,)], ["geography"])
    write_dataframe(test_df, parquet_file)

    columns = [Column(name="geography", data_type="STRING")]
    schema = FileSchema(path=str(parquet_file), columns=columns)
    with pytest.raises(DSGInvalidField, match="conflicts with the Parquet"):
        read_data_file(schema)


def test_read_data_file_parquet_narrowing_declaration_raises(tmp_path, spark):
    """A same-family declaration narrower than the Parquet type is rejected."""
    parquet_file = tmp_path / "test.parquet"
    test_df = spark.createDataFrame([(1,)], ["id"])  # int64
    write_dataframe(test_df, parquet_file)

    columns = [Column(name="id", data_type="INT")]
    schema = FileSchema(path=str(parquet_file), columns=columns)
    with pytest.raises(DSGInvalidField, match="is narrower than"):
        read_data_file(schema)


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

    DuckDB binds column types at read time and raises BinderException; Spark's
    CSV reader silently drops schema fields not present in the file, so
    dsgrid's own column validation raises DSGInvalidDataset instead.
    """
    import duckdb

    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,a\n")

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="missing_column", data_type="STRING"),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    with pytest.raises((duckdb.BinderException, DSGInvalidDataset), match="missing_column"):
        read_data_file(schema)


def test_read_data_file_csv_inferred_types(tmp_path, spark):
    """Test reading a CSV file without explicit schema (type inference)."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,value\n1,a,1.0\n2,b,2.0\n")

    schema = FileSchema(path=str(csv_file), columns=[])
    df = read_data_file(schema)

    assert count_rows(df) == 2
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
    columns = [
        Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
    ]
    schema = FileSchema(path=str(csv_file), columns=columns)
    df = read_data_file(schema)
    assert count_rows(df) == 4

    # Verify columns were renamed via dimension_type
    assert "county" not in df.columns

    # Verify all expected columns are present
    expected_columns = {"geography", "sector"} | set(value_columns)
    assert set(df.columns) == expected_columns

    # Verify leading zeros are preserved (critical for FIPS codes)
    geography_values = sorted([row.geography for row in _collect(df.select("geography"))])
    assert geography_values == ["06073", "06073", "06075", "06075"]

    # Verify sector values are present
    sector_values = sorted(set(row.sector for row in _collect(df.select("sector"))))
    assert sector_values == ["com", "res"]

    # Verify float values are readable and correct
    cooling_sum = sum(row.cooling for row in _collect(df.select("cooling")))
    assert abs(cooling_sum - 7.8) < 0.01  # 1.5 + 1.8 + 2.1 + 2.4

    heating_sum = sum(row.heating for row in _collect(df.select("heating")))
    assert abs(heating_sum - 11.1) < 0.01  # 2.3 + 2.1 + 3.5 + 3.2


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

    assert count_rows(df) == 4
    assert set(df.columns) == {"id", "timestamp", "com_cooling", "com_fans"}

    # Collect the timestamps and verify they were parsed correctly
    with custom_time_zone("America/New_York"):
        # Converting to string avoids the complexity of timestamp conversion to the system
        # time zone when calling collect().
        df2 = _with_string_column(df, "timestamp", "timestamp_str")
        rows = _collect(_order_by(df2, "timestamp_str"))
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
    parsed as naive timestamps (no time_zone conversion occurs).
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

    assert count_rows(df) == 4
    assert set(df.columns) == {"id", "timestamp", "com_cooling", "com_fans"}

    # Collect the timestamps and verify they were parsed correctly
    rows = _collect(_order_by(df, "timestamp"))

    # The first timestamp should be 2012-01-01 01:00:00 with no time_zone conversion
    first_ts = rows[0].timestamp
    assert first_ts.year == 2012
    assert first_ts.month == 1
    assert first_ts.day == 1
    # No time_zone conversion, so hour remains 1
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

    # read_data_file with ignore_columns tests


def test_read_data_file_csv_with_ignore_columns(tmp_path, spark):
    """Test reading a CSV file with columns to ignore."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name,extra,value\n1,a,x,1.0\n2,b,y,2.0\n")

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(
        path=str(csv_file),
        columns=columns,
        ignore_columns=["extra"],
    )
    df = read_data_file(schema)

    assert count_rows(df) == 2
    assert set(df.columns) == {"id", "name", "value"}
    assert "extra" not in df.columns


def test_read_data_file_csv_ignore_multiple_columns(tmp_path, spark):
    """Test ignoring multiple columns when reading CSV."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,skip1,name,skip2,value\n1,a,test,b,1.0\n")

    columns = [
        Column(name="id", data_type="INTEGER"),
        Column(name="name", data_type="STRING"),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(
        path=str(csv_file),
        columns=columns,
        ignore_columns=["skip1", "skip2"],
    )
    df = read_data_file(schema)

    assert set(df.columns) == {"id", "name", "value"}
    assert "skip1" not in df.columns
    assert "skip2" not in df.columns


def test_read_data_file_csv_ignore_columns_before_rename(tmp_path, spark):
    """Test that columns are ignored before renaming occurs."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("county,extra,value\nBoulder,x,1.0\nJefferson,y,2.0\n")

    columns = [
        Column(name="county", data_type="STRING", dimension_type=DimensionType.GEOGRAPHY),
        Column(name="value", data_type="DOUBLE"),
    ]
    schema = FileSchema(
        path=str(csv_file),
        columns=columns,
        ignore_columns=["extra"],
    )
    df = read_data_file(schema)

    # Column renamed from county to geography
    assert "geography" in df.columns
    assert "county" not in df.columns
    # Extra column was ignored
    assert "extra" not in df.columns
    assert set(df.columns) == {"geography", "value"}


def test_read_data_file_parquet_with_ignore_columns(tmp_path, spark):
    """Test reading a Parquet file with columns to ignore."""
    parquet_file = tmp_path / "test.parquet"
    test_df = spark.createDataFrame(
        [(1, "a", "extra", 1.0), (2, "b", "extra", 2.0)],
        ["id", "name", "to_ignore", "value"],
    )
    write_dataframe(test_df, parquet_file)

    columns = [
        Column(name="id", data_type=None),
        Column(name="name", data_type=None),
        Column(name="value", data_type=None),
    ]
    schema = FileSchema(
        path=str(parquet_file),
        columns=columns,
        ignore_columns=["to_ignore"],
    )
    df = read_data_file(schema)

    assert count_rows(df) == 2
    assert set(df.columns) == {"id", "name", "value"}
    assert "to_ignore" not in df.columns


# apply_declared_types tests


def test_apply_declared_types_empty_columns_returns_unchanged(spark):
    """No declarations means no casts."""
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    before = df.schema()
    after = apply_declared_types(df, []).schema()
    assert before == after


def test_apply_declared_types_skips_columns_without_data_type(spark):
    """Columns with data_type=None keep their existing type."""
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    columns = [Column(name="id", data_type=None), Column(name="name", data_type=None)]
    assert apply_declared_types(df, columns).schema() == df.schema()


def test_apply_declared_types_skips_columns_not_in_table(spark):
    """Declared columns absent from the table are silently ignored.

    Downstream validators surface missing required columns with a useful
    message; apply_declared_types is not the place to catch that.
    """
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    columns = [Column(name="ghost", data_type="BIGINT")]
    assert apply_declared_types(df, columns).schema() == df.schema()


def test_apply_declared_types_same_family_int_widening(spark):
    """Same-family widening (e.g. int32 → int64) is applied."""
    # The default createDataFrame path produces int64, so build via an
    # explicit int32 memtable to exercise the widening path.
    df = ibis.memtable({"id": [1]}).cast({"id": "int32"})
    columns = [Column(name="id", data_type="BIGINT")]  # int32 → int64 (widening)
    result = apply_declared_types(df, columns)
    assert str(result.schema()["id"]) == "int64"


def test_apply_declared_types_narrowing_raises(spark):
    """Same-family narrowing (e.g. int64 → int32) raises DSGInvalidField.

    Previously this silently truncated dimension IDs encoded as int64; now
    callers must declare BIGINT or widen explicitly.
    """
    df = spark.createDataFrame([(1,)], ["id"])  # Spark createDataFrame → int64
    columns = [Column(name="id", data_type="INT")]  # INT → int32 (would narrow)
    with pytest.raises(DSGInvalidField, match="is narrower than"):
        apply_declared_types(df, columns)


def test_apply_declared_types_int_to_string(spark):
    """A declared string type coerces numeric data across families.

    This is the main cross-family use case: dimension record IDs are strings,
    and JSON/CSV numbers cannot be marked as strings in the file itself.
    """
    df = spark.createDataFrame([(6037,)], ["geography"])
    columns = [Column(name="geography", data_type="VARCHAR")]
    result = apply_declared_types(df, columns)
    assert str(result.schema()["geography"]) == "string"
    assert [row.geography for row in _collect(result)] == ["6037"]


def test_apply_declared_types_string_to_int(spark):
    """A declared integer type coerces numeric text across families."""
    df = spark.createDataFrame([("6202",)], ["id"])  # e.g. an ID column read as string
    columns = [Column(name="id", data_type="BIGINT")]
    result = apply_declared_types(df, columns)
    assert str(result.schema()["id"]) == "int64"
    assert [row.id for row in _collect(result)] == [6202]


def test_apply_declared_types_partial_declaration_only_casts_declared(spark):
    """Only the declared subset is touched; the rest keeps inferred types."""
    df = spark.createDataFrame([(1, "a", 1.5)], ["id", "name", "value"])
    columns = [Column(name="id", data_type="BIGINT")]  # only id; matches actual int64
    result = apply_declared_types(df, columns)
    schema = result.schema()
    assert str(schema["id"]) == "int64"
    assert str(schema["name"]) == "string"
    assert "float" in str(schema["value"])  # float32 or float64


# TypeSpec family + _actual_type_family tests


@pytest.mark.parametrize(
    "declared,expected_family",
    [
        ("BOOLEAN", "bool"),
        ("INT", "integer"),
        ("INTEGER", "integer"),
        ("TINYINT", "integer"),
        ("SMALLINT", "integer"),
        ("BIGINT", "integer"),
        ("FLOAT", "floating"),
        ("DOUBLE", "floating"),
        ("STRING", "string"),
        ("TEXT", "string"),
        ("VARCHAR", "string"),
        ("TIMESTAMP_NTZ", "timestamp"),
        ("TIMESTAMP_TZ", "timestamp"),
    ],
)
def test_declared_type_family_covers_all_supported(declared, expected_family):
    assert spec_for_name(declared).family == expected_family


def test_declared_type_family_unknown_raises():
    with pytest.raises(KeyError, match="Unsupported dsgrid type"):
        spec_for_name("BOGUS")


def test_actual_type_family_covers_common_dtypes():
    """Run through the family branches on real Ibis dtypes."""
    assert _actual_type_family(ibis.dtype("bool")) == "bool"
    assert _actual_type_family(ibis.dtype("int64")) == "integer"
    assert _actual_type_family(ibis.dtype("int32")) == "integer"
    assert _actual_type_family(ibis.dtype("float64")) == "floating"
    assert _actual_type_family(ibis.dtype("string")) == "string"
    assert _actual_type_family(ibis.dtype("timestamp")) == "timestamp"


def test_actual_type_family_unknown_returns_other():
    """Anything outside the recognized families falls into ``other`` so
    apply_declared_types' strict-family check leaves the column alone."""
    assert _actual_type_family(ibis.dtype("array<int64>")) == "other"


# TIMESTAMP_TZ-on-Spark warning tests


def _make_timestamp_tz_schema() -> list:
    return [Column(name="ts", data_type="TIMESTAMP_TZ")]


def test_timestamp_tz_warning_on_duckdb_is_silent(spark):
    """DuckDB carries per-row TZ tags; no warning expected."""
    if not use_duckdb():
        pytest.skip("Test exercises DuckDB-specific silence")
    df = ibis.memtable({"ts": [datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))]}).cast(
        {"ts": "timestamp('UTC')"}
    )
    import warnings as _warnings

    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        apply_declared_types(df, _make_timestamp_tz_schema())
    relevant = [w for w in caught if "TIMESTAMP_TZ" in str(w.message)]
    assert relevant == []


def test_timestamp_tz_warning_on_spark_non_utc_emits(monkeypatch, spark):
    """When the session TZ is non-UTC on Spark, declaring TIMESTAMP_TZ warns."""
    if use_duckdb():
        pytest.skip("Warning is Spark-only behavior")
    monkeypatch.setattr(_fs, "use_duckdb", lambda: False)
    # file_schema does ``from dsgrid.ibis.tz import get_current_time_zone`` at
    # module top, so the consumer holds its own reference; patching the source
    # module wouldn't affect it. Patch the consumer's reference instead.
    monkeypatch.setattr(_fs, "get_current_time_zone", lambda: "America/Denver")
    df = ibis.memtable({"ts": [datetime(2024, 1, 1)]}).cast({"ts": "timestamp"})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        apply_declared_types(df, _make_timestamp_tz_schema())
    relevant = [w for w in caught if "TIMESTAMP_TZ" in str(w.message)]
    assert len(relevant) == 1
    assert "Denver" in str(relevant[0].message)


def test_timestamp_tz_warning_on_spark_utc_is_silent(monkeypatch, spark):
    """When the session TZ is UTC on Spark, declaring TIMESTAMP_TZ is silent."""
    if use_duckdb():
        pytest.skip("Warning is Spark-only behavior")
    monkeypatch.setattr(_fs, "use_duckdb", lambda: False)
    monkeypatch.setattr(_fs, "get_current_time_zone", lambda: "UTC")
    df = ibis.memtable({"ts": [datetime(2024, 1, 1)]}).cast({"ts": "timestamp"})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        apply_declared_types(df, _make_timestamp_tz_schema())
    relevant = [w for w in caught if "TIMESTAMP_TZ" in str(w.message)]
    assert relevant == []


def test_timestamp_tz_parquet_roundtrip_preserves_utc_instants(tmp_path, spark):
    """A TIMESTAMP_TZ column written by the runtime backend and re-read must
    preserve UTC instants exactly. On Spark, ``TIMESTAMP`` cannot carry a
    per-row TZ tag; the instants survive
    because ``spark.sql.parquet.outputTimestampType`` is pinned to
    ``TIMESTAMP_MICROS`` in ``_create_spark_session``. On DuckDB,
    ``TIMESTAMPTZ`` is written as adjusted UTC. We pin both behaviors so a
    backend change cannot silently shift instants across the Parquet boundary.
    """
    # Four instants chosen to include UTC, UTC-08 (winter PST), and the
    # spring-forward / fall-back boundary in America/Los_Angeles. Using the
    # offset-explicit ZoneInfo avoids the ambiguous-local-time pitfall.
    la = ZoneInfo("America/Los_Angeles")
    instants = [
        datetime(2024, 1, 15, 8, 0, 0, tzinfo=ZoneInfo("UTC")),
        datetime(2024, 1, 15, 0, 0, 0, tzinfo=la),  # 08:00 UTC
        datetime(2024, 3, 10, 1, 30, 0, tzinfo=la),  # pre-spring-forward
        datetime(2024, 11, 3, 1, 30, 0, tzinfo=la),  # ambiguous fall-back hr
    ]
    df = spark.createDataFrame([(i, ts) for i, ts in enumerate(instants)], ["idx", "ts"])
    parquet_file = tmp_path / "tstz_roundtrip.parquet"
    write_dataframe(df, parquet_file)

    read_back = read_parquet(parquet_file)
    rows = sorted(_collect(_order_by(read_back, "idx")), key=lambda r: r.idx)
    # Normalize each round-tripped timestamp to UTC epoch seconds. Spark
    # returns tz-naive datetime in the session TZ (UTC, pinned); DuckDB
    # returns tz-aware. Both convert cleanly to UTC seconds for parity.
    got_utc_secs = [
        int(
            (r.ts.replace(tzinfo=ZoneInfo("UTC")) if r.ts.tzinfo is None else r.ts)
            .astimezone(ZoneInfo("UTC"))
            .timestamp()
        )
        for r in rows
    ]
    expected_utc_secs = [int(ts.astimezone(ZoneInfo("UTC")).timestamp()) for ts in instants]
    assert got_utc_secs == expected_utc_secs, (
        "TIMESTAMP_TZ Parquet round-trip must preserve UTC instants on both "
        "backends. A drift here indicates either Spark's outputTimestampType "
        "is no longer pinned to TIMESTAMP_MICROS or DuckDB's TIMESTAMPTZ "
        "serialization changed."
    )
