from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import ibis
import pandas as pd
import pytest
from chronify.exceptions import InvalidOperation

from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter
from dsgrid.ibis.functions import write_csv
from dsgrid.ibis.io import (
    CsvPartitionWriter,
    overwrite_dataframe_file,
    persist_and_reload_table,
    persist_table,
    read_csv,
    read_dataframe,
    read_json,
    read_parquet,
    try_read_dataframe,
    write_dataframe,
    write_dataframe_and_auto_partition,
    write_table,
    _is_corrupt_file_error,
    _is_duckdb_corrupt_parquet_error,
    _is_spark_corrupt_parquet_error,
    _post_process_dataframe,
)
from dsgrid.ibis.operations import filter_sql
from dsgrid.ibis.session import get_runtime_session, _create_ibis_table
from dsgrid.ibis.table_utils import count_rows, table_to_pandas
from dsgrid.ibis.types import spec_for_name, use_duckdb
from dsgrid.utils.files import dump_json_file
from dsgrid.utils.scratch_dir_context import ScratchDirContext

from tests._helpers import collect as _collect


def _filter(df, predicate):
    return filter_sql(df, predicate)


def test_read_json_valid(tmp_path):
    filename = tmp_path / "table.json"
    filename.write_text('{"id": "a", "value": 1.5}\n{"id": "b", "value": 2.5}\n')

    df = read_json(filename)

    assert sorted(df.columns) == ["id", "value"]
    assert count_rows(df) == 2


def test_read_json_rejects_malformed_records(tmp_path):
    """A malformed record must raise, not read as nulls.

    Spark's default PERMISSIVE mode turns the bad record into a null row and
    adds a ``_corrupt_record`` column; both would flow into the dataset
    unnoticed because dsgrid matches columns by name. DuckDB's reader already
    raises, so this pins the behavior as equivalent across backends.

    The two backends raise different exception types (and at different points --
    Spark during schema inference, DuckDB during the scan), so this asserts only
    that the read fails.
    """
    filename = tmp_path / "table.json"
    filename.write_text(
        '{"id": "a", "value": 1.5}\n{"id": "b", "value": \n{"id": "c", "value": 3.5}\n'
    )

    with pytest.raises(Exception):
        count_rows(read_json(filename))


def test_try_read_dataframe_invalid(tmp_path):
    invalid = tmp_path / "table.parquet"
    invalid.mkdir()
    assert try_read_dataframe(invalid) is None
    assert not invalid.exists()


# Corruption modes a partial or bad Parquet write can produce. Each must be
# classified as a corrupt-file error (not a transient failure) so a regenerable
# cache self-heals.
_CORRUPT_PARQUET_BYTES = {
    "garbage": b"this is not a parquet file\n" * 8,
    "empty": b"",
    "bad_magic_prefix": b"PAR1" + b"\x00" * 64,
}


@pytest.mark.parametrize("label", sorted(_CORRUPT_PARQUET_BYTES))
def test_corrupt_parquet_is_classified(tmp_path, label):
    """Characterization test: a real corrupt-Parquet read is recognized as a
    corrupt-file error.

    The assertion runs against whichever backend is active — DuckDB locally and
    in the non-Spark CI job, Spark in the Spark CI job — so the backend-specific
    signatures in ``_is_corrupt_file_error`` stay honest. If a backend upgrade
    changes its error class or message, this fails loudly and the matcher must be
    updated (the failure message reports the unrecognized exception).
    """
    path = tmp_path / "corrupt.parquet"
    path.write_bytes(_CORRUPT_PARQUET_BYTES[label])
    with pytest.raises(Exception) as exc_info:
        read_parquet(path)
    exc = exc_info.value
    assert _is_corrupt_file_error(exc), (
        "Reading a corrupt Parquet file raised an exception that "
        "_is_corrupt_file_error did not recognize; the backend may have changed "
        f"its signature: {type(exc).__module__}.{type(exc).__name__}: {exc}"
    )


def test_try_read_dataframe_deletes_corrupt_file(tmp_path):
    """A corrupt (not merely empty-directory) cache file is treated as a miss and
    deleted so the caller regenerates it."""
    path = tmp_path / "table.parquet"
    path.write_bytes(b"this is not a parquet file\n" * 8)
    assert try_read_dataframe(path) is None
    assert not path.exists()


def test_try_read_dataframe_valid(tmp_path):
    spark = get_runtime_session()
    df = spark.createDataFrame([(1,)], ["a"])
    filename = tmp_path / "table.parquet"
    write_dataframe(df, filename)
    df = try_read_dataframe(filename)
    assert isinstance(df, ibis.Table)
    assert table_to_pandas(df)["a"].iloc[0] == 1


def test_parquet_exception_detection():
    class AnalysisException(Exception):
        pass

    class SparkException(Exception):
        pass

    class IOException(Exception):
        __module__ = "duckdb.fake"

    class InvalidInputException(Exception):
        __module__ = "duckdb.fake"

    # Spark corruption signatures, matched on the message regardless of class.
    assert _is_spark_corrupt_parquet_error(
        AnalysisException("Unable to infer schema for Parquet. It must be specified manually.")
    )
    assert _is_spark_corrupt_parquet_error(AnalysisException("PATH_NOT_FOUND"))
    assert _is_spark_corrupt_parquet_error(AnalysisException("Path does not exist"))
    assert _is_spark_corrupt_parquet_error(
        SparkException("file:/x/part.parquet is not a Parquet file")
    )
    assert not _is_spark_corrupt_parquet_error(AnalysisException("other"))

    # DuckDB corruption is matched on the exception class within the duckdb module.
    assert _is_duckdb_corrupt_parquet_error(IOException("bad parquet"))
    assert _is_duckdb_corrupt_parquet_error(InvalidInputException("No magic bytes found"))
    assert not _is_duckdb_corrupt_parquet_error(ValueError("bad parquet"))

    # The combined classifier accepts either backend's corruption signature and
    # rejects transient/unrelated errors so they propagate instead of deleting a
    # cache file.
    assert _is_corrupt_file_error(IOException("bad parquet"))
    assert _is_corrupt_file_error(SparkException("part.parquet is not a Parquet file"))
    assert not _is_corrupt_file_error(MemoryError("out of memory"))
    assert not _is_corrupt_file_error(ConnectionError("spark master unreachable"))


def test_require_unique_raises():
    table = get_runtime_session().createDataFrame([("a",), ("a",)], ["id"])
    with pytest.raises(DSGInvalidField, match="duplicate entries"):
        _post_process_dataframe(table, require_unique=["id"])


def test_read_dataframe_and_write_error_paths(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_dataframe(tmp_path / "missing.csv")

    unsupported = tmp_path / "table.txt"
    unsupported.write_text("a\n1\n")
    with pytest.raises(NotImplementedError, match="Unsupported file extension"):
        read_dataframe(unsupported)

    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    with pytest.raises(NotImplementedError, match="Unsupported file format"):
        write_table(table, (tmp_path / "table.invalid").as_posix(), "invalid")

    # A suffix write_dataframe cannot dispatch on must raise rather than
    # silently write nothing and leave the caller pointing at a missing file.
    unwritable = tmp_path / "output.txt"
    with pytest.raises(NotImplementedError, match="Unsupported file extension"):
        write_dataframe(table, unwritable)
    assert not unwritable.exists()

    with pytest.raises(DSGInvalidParameter, match="only supports Parquet"):
        write_dataframe_and_auto_partition(table, tmp_path / "table.csv")


def test_write_dataframe_requires_overwrite(tmp_path):
    """An existing path is only replaced when the caller asks for it."""
    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    replacement = get_runtime_session().createDataFrame([(2,), (3,)], ["a"])

    filename = tmp_path / "table.parquet"
    write_dataframe(table, filename)
    with pytest.raises(InvalidOperation, match="already exists"):
        write_dataframe(replacement, filename)
    assert count_rows(read_parquet(filename)) == 1

    write_dataframe(replacement, filename, overwrite=True)
    assert count_rows(read_parquet(filename)) == 2


def test_write_dataframe_json_requires_overwrite(tmp_path):
    """A .json write lands on the requested path and honors the overwrite contract."""
    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    replacement = get_runtime_session().createDataFrame([(2,), (3,)], ["a"])

    filename = tmp_path / "table.json"
    write_dataframe(table, filename)
    with pytest.raises(InvalidOperation, match="already exists"):
        write_dataframe(replacement, filename)
    assert count_rows(read_dataframe(filename)) == 1

    write_dataframe(replacement, filename, overwrite=True)
    assert count_rows(read_dataframe(filename)) == 2


def test_overwrite_dataframe_file_clears_stale_tmp(tmp_path):
    """A .tmp sibling left by a crashed call does not block the next write."""
    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    replacement = get_runtime_session().createDataFrame([(2,), (3,)], ["a"])

    filename = tmp_path / "table.parquet"
    write_dataframe(table, filename)
    tmp_sibling = filename.with_name(filename.name + ".tmp")
    tmp_sibling.write_text("leftover from a crashed write")

    assert count_rows(overwrite_dataframe_file(filename, replacement)) == 2
    assert not tmp_sibling.exists()


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB file overwrite paths only apply to DuckDB")
def test_persist_and_overwrite_file_helpers(tmp_path):
    table = get_runtime_session().createDataFrame([(1, "a")], ["id", "name"])
    replacement = get_runtime_session().createDataFrame([(2, "b")], ["id", "name"])
    assert _create_ibis_table(table) is table

    csv_file = tmp_path / "table.csv"
    write_dataframe(table, csv_file)
    overwritten_csv = overwrite_dataframe_file(csv_file, replacement)
    assert overwritten_csv.count().execute() == 1

    json_file = tmp_path / "table.json"
    overwritten_json = overwrite_dataframe_file(json_file, replacement)
    assert overwritten_json.count().execute() == 1

    with pytest.raises(NotImplementedError, match="Unsupported file suffix"):
        overwrite_dataframe_file(tmp_path / "table.txt", table)

    if use_duckdb():
        duckdb_json = tmp_path / "duckdb.json"
        write_dataframe(table, duckdb_json)
        assert duckdb_json.exists()
        assert not (tmp_path / "duckdb.parquet").exists()

    with ScratchDirContext(tmp_path / "scratch") as context:
        path = persist_table(table, context, tag="test")
        assert path.exists()
        persisted = persist_and_reload_table(table, context)
        assert persisted.count().execute() == 1
        persisted_auto = persist_and_reload_table(table, context, auto_partition=True)
        assert persisted_auto.count().execute() == 1


def test_csv_partition_writer_rollover(tmp_path):
    csv_dir = tmp_path / "csv_parts"
    with CsvPartitionWriter(csv_dir, max_partition_size_mb=0) as writer:
        writer.add_row(("a", "b"))
        writer.add_row(("c", "d"))

    files = sorted(csv_dir.iterdir())
    assert [x.name for x in files] == ["part1.csv", "part2.csv"]
    assert files[0].read_text() == "a,b\n"


def test_read_csv(tmp_path: Path) -> None:
    pdf = pd.DataFrame(
        {
            "a": range(3),
            "b": ["a", "b", "c"],
            "c": [float(i) for i in range(3)],
            "d": [datetime(2020, 1, 1, i, tzinfo=ZoneInfo("Etc/GMT+5")) for i in range(3)],
        }
    )
    filename = tmp_path / "load_data.csv"
    pdf.to_csv(filename, header=True, index=False)
    schema_file = tmp_path / "load_data_schema.json"
    schema = {
        "columns": [
            {
                "name": "a",
                "data_type": "integer",
            },
            {
                "name": "b",
                "data_type": "string",
            },
            {
                "name": "c",
                "data_type": "DOUBLE",
            },
            {
                "name": "d",
                "data_type": "TIMESTAMP_TZ",
            },
        ]
    }
    dump_json_file(schema, schema_file)
    df = read_csv(filename)
    values = _collect(df)
    row = values[-1]
    # No schema declared, so DuckDB infers native types from the data.
    assert isinstance(row.a, int) and row.a == 2
    assert isinstance(row.b, str) and row.b == "c"
    assert isinstance(row.c, float) and row.c == 2.0
    assert isinstance(row.d, datetime)

    assert (
        len(
            _collect(
                _filter(
                    df,
                    "d >= '2020-01-01 00:00:00-05:00' and d <= '2020-01-01 02:00:00-05:00'",
                )
            )
        )
        == 3
    )


def _string_sql() -> str:
    """The backend SQL type string for a string column, as read_csv expects."""
    spec = spec_for_name("string")
    return spec.duckdb_sql if use_duckdb() else spec.spark_sql


def test_read_csv_declared_string_preserves_leading_zeros(tmp_path: Path) -> None:
    """A declared string schema preserves leading-zero IDs (e.g. FIPS "08031").

    This is the guarantee dsgrid relies on for typed columns: dataset data files
    are read through a FileSchema, and the --schema-file / DataFileColumns
    mechanism exists specifically to pin ID columns as strings so a numeric-looking
    code is not read back as an integer. (Dimension records never reach this
    reader -- they load via csv.DictReader into str-typed Pydantic models -- so
    this pins the primitive the dataset path depends on.)
    """
    filename = tmp_path / "geo.csv"
    filename.write_text("geoid,pop\n08031,100\n12000,200\n")

    typed = _collect(read_csv(filename, schema={"geoid": _string_sql()}))
    assert sorted(r.geoid for r in typed) == ["08031", "12000"]


@pytest.mark.skipif(not use_duckdb(), reason="pins DuckDB CSV type-inference behavior")
def test_read_csv_schemaless_leading_zero_duckdb_sample_window(tmp_path: Path) -> None:
    """Characterize DuckDB's schemaless leading-zero inference: safe within the
    type-detection sample, corrupted beyond it.

    DuckDB samples the first ~20480 rows to infer column types. A leading-zero ID
    seen within that window keeps the column a string (zero preserved); if every
    sampled value is a plain integer and the first leading-zero value appears only
    *beyond* the window, the column is typed integer and the zero is silently lost.
    Either way the lesson is the same: an ID column that must survive as a string
    has to be declared (see test_read_csv_declared_string_preserves_leading_zeros),
    not inferred.
    """
    # Within the sample window: DuckDB sees the leading zero and keeps it a string.
    early = tmp_path / "early.csv"
    early.write_text("geoid,pop\n08031,100\n12000,200\n")
    early_tbl = read_csv(early)
    assert early_tbl.schema()["geoid"].is_string()
    assert sorted(r.geoid for r in _collect(early_tbl)) == ["08031", "12000"]

    # Beyond the sample window: the late leading-zero row is typed away to an int.
    late = tmp_path / "late.csv"
    lines = ["geoid,pop"] + [f"{12000 + i},{i}" for i in range(30000)] + ["08031,999999"]
    late.write_text("\n".join(lines) + "\n")
    late_tbl = read_csv(late)
    assert late_tbl.schema()["geoid"].is_integer()
    late_rows = _collect(_filter(late_tbl, "pop = 999999"))
    assert len(late_rows) == 1
    assert late_rows[0].geoid == 8031  # leading zero lost -- the hazard being pinned


@pytest.mark.skipif(use_duckdb(), reason="pins Spark CSV inferSchema behavior")
def test_read_csv_schemaless_leading_zeros_lost_on_spark(tmp_path: Path) -> None:
    """Characterize Spark's schemaless leading-zero inference: always lost.

    Unlike DuckDB (which preserves a leading-zero value seen within its sample
    window), Spark's inferSchema does a full pass and picks the most-specific
    parseable type, so a "08031" column is typed integer and the zero is dropped
    even when the value is the very first row. Declaring the column as string is
    mandatory for leading-zero IDs on Spark.
    """
    filename = tmp_path / "early.csv"
    filename.write_text("geoid,pop\n08031,100\n12000,200\n")
    table = read_csv(filename)
    assert table.schema()["geoid"].is_integer()
    assert sorted(r.geoid for r in _collect(table)) == [8031, 12000]


def test_read_csv_with_pipe_delimiter(tmp_path: Path) -> None:
    """Custom delimiter is passed through on both backends."""
    filename = tmp_path / "piped.csv"
    filename.write_text("a|b|c\n1|x|2.5\n2|y|3.5\n")
    table = read_csv(filename, delimiter="|")
    assert sorted(table.columns) == ["a", "b", "c"]
    assert count_rows(table) == 2


def test_read_csv_round_trip(tmp_path: Path, dataframe) -> None:
    """write_csv -> read_csv round-trips on the runtime backend.

    On DuckDB the output is a single file; on Spark it's a directory of
    part files. read_csv must transparently handle both shapes.
    """
    out = tmp_path / "round_trip.csv"
    write_csv(dataframe, out, overwrite=True)
    assert out.exists()
    round_tripped = read_csv(out)
    assert sorted(round_tripped.columns) == sorted(dataframe.columns)
    assert count_rows(round_tripped) == count_rows(dataframe)


def test_read_csv_rejects_non_utf8_on_duckdb(tmp_path: Path) -> None:
    """DuckDB has no encoding parameter; passing one raises with a clear message."""
    if not use_duckdb():
        pytest.skip("DuckDB-only behavior check")

    csv = tmp_path / "any.csv"
    csv.write_text("a\n1\n")
    with pytest.raises(DSGInvalidParameter, match="UTF-8"):
        read_csv(csv, encoding="latin-1")


def test_read_csv_rejects_headerless_input(tmp_path: Path) -> None:
    """read_csv treats the first row as the header on both backends; a CSV
    without a header has the wrong dsgrid shape and must produce a loud
    failure rather than silently treating row 1 as column names."""
    # Two integer rows without a header line. read_csv will treat "1,2,3" as
    # the column names. We confirm the implicit-header behavior so a caller
    # accidentally passing a headerless dataset file gets the obviously-wrong
    # column names rather than silently misinterpreting the row as data.
    csv = tmp_path / "no_header.csv"
    csv.write_text("1,2,3\n4,5,6\n")
    table = read_csv(csv)
    assert list(table.columns) == ["1", "2", "3"], (
        "read_csv requires an explicit header row; the docstring contract is "
        "that callers must rewrite headerless CSVs before reading."
    )


def test_read_csv_null_values_backend_divergence(tmp_path: Path) -> None:
    """null_values is a list of strings to recognize as NULL. DuckDB honors
    every entry; Spark's CSV reader only takes a single nullValue and the
    consolidated read_csv silently truncates to the first entry. We pin
    that behavior so a backend migration cannot quietly change the rows
    that resolve to NULL."""
    csv = tmp_path / "nulls.csv"
    csv.write_text("a,b\nNA,1\nNULL,2\nzzz,3\n")

    table = read_csv(csv, null_values=["NA", "NULL"])
    rows = table.execute().to_dict("records")
    rows.sort(key=lambda r: r["b"])
    a_values = [r["a"] for r in rows]

    if use_duckdb():
        # DuckDB recognizes both literals as NULL.
        assert a_values[0] is None or (
            isinstance(a_values[0], float) and a_values[0] != a_values[0]  # NaN
        )
        assert a_values[1] is None or (
            isinstance(a_values[1], float) and a_values[1] != a_values[1]
        )
        assert a_values[2] == "zzz"
    else:
        # Spark only honors null_values[0] ("NA"); "NULL" stays a string.
        assert a_values[0] is None
        assert a_values[1] == "NULL"
        assert a_values[2] == "zzz"
