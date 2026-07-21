from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import ibis
import pandas as pd
import pytest

from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter
from dsgrid.ibis.functions import write_csv
from dsgrid.ibis.io import (
    CsvPartitionWriter,
    overwrite_dataframe_file,
    persist_and_reload_table,
    persist_table,
    read_csv,
    read_dataframe,
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
from dsgrid.ibis.types import use_duckdb
from dsgrid.utils.files import dump_json_file
from dsgrid.utils.scratch_dir_context import ScratchDirContext

from tests._helpers import collect as _collect


def _filter(df, predicate):
    return filter_sql(df, predicate)


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

    with pytest.raises(DSGInvalidParameter, match="only supports Parquet"):
        write_dataframe_and_auto_partition(table, tmp_path / "table.csv")


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
        assert not duckdb_json.exists()
        assert (tmp_path / "duckdb.parquet").exists()

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
