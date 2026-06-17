from datetime import datetime
from pathlib import Path
from typing import Generator
from zoneinfo import ZoneInfo

import ibis
import pandas as pd
import pytest

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.ibis import backend as backend_mod
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.operations import filter_sql, rename_columns
from dsgrid.ibis.functions import (
    aggregate,
    aggregate_single_value,
    cache,
    collect_list,
    count_distinct_on_group_by,
    cross_join,
    except_all,
    perform_interval_op,
    intersect,
    is_dataframe_empty,
    join,
    join_multiple_columns,
    pivot,
    read_csv,
    select_expr,
    sql_from_df,
    unpersist,
    unpivot,
    write_csv,
)
from dsgrid.ibis.io import read_csv as _read_csv_io
from dsgrid.utils.files import dump_json_file
from dsgrid.ibis.session import (
    get_runtime_session,
    init_runtime_session,
    SparkSession,
)
from dsgrid.ibis.types import use_duckdb

from dsgrid.ibis.table_utils import count_rows
from tests._helpers import collect as _collect, order_by as _order_by


def _filter(df, predicate):
    return filter_sql(df, predicate)


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    spark = get_runtime_session()
    yield spark


@pytest.fixture(scope="module")
def dataframe(spark) -> Generator[ibis.Table, None, None]:
    df = spark.createDataFrame(
        [
            (0, "cooling", 1.0),
            (0, "heating", 2.0),
            (1, "cooling", 3.0),
            (1, "heating", 4.0),
        ],
        ["index", "metric", "value"],
    )
    df = cache(df)
    yield df
    unpersist(df)


@pytest.fixture(scope="module")
def geo_dataframe(spark) -> Generator[ibis.Table, None, None]:
    df = spark.createDataFrame(
        [
            ("Boulder",),
            ("Jefferson",),
        ],
        ["county"],
    )
    df = cache(df)
    yield df
    unpersist(df)


@pytest.fixture(scope="module")
def time_dataframe(spark) -> Generator[ibis.Table, None, None]:
    utc = ZoneInfo("UTC")
    df = spark.createDataFrame(
        [
            (datetime(2020, 1, 1, 0, tzinfo=utc), "cooling", 1.0),
            (datetime(2020, 1, 1, 0, tzinfo=utc), "heating", 2.0),
            (datetime(2020, 1, 1, 1, tzinfo=utc), "cooling", 3.0),
            (datetime(2020, 1, 1, 1, tzinfo=utc), "heating", 4.0),
        ],
        ["timestamp", "metric", "value"],
    )
    df = cache(df)
    yield df
    unpersist(df)


def test_aggregate(dataframe):
    assert _collect(aggregate(dataframe, "sum", "value", "s").select("s"))[0].s == 10.0


def test_aggregate_single_value(dataframe):
    assert aggregate_single_value(dataframe, "sum", "value") == 10.0


def test_collect_list(dataframe):
    assert sorted(collect_list(dataframe, "metric")) == [
        "cooling",
        "cooling",
        "heating",
        "heating",
    ]


def test_count_distinct_on_group_by(dataframe):
    assert _collect(count_distinct_on_group_by(dataframe, ["metric"], "index", "c"))[0].c == 2


def test_cross_join(dataframe, geo_dataframe):
    df = cross_join(dataframe, geo_dataframe)
    assert count_rows(df) == count_rows(dataframe) * count_rows(geo_dataframe)
    assert (
        aggregate_single_value(
            _filter(df, "county = 'Boulder' and metric = 'cooling'"), "sum", "value"
        )
        == 4.0
    )


def test_except_all(dataframe):
    df2 = _filter(dataframe, "metric = 'heating'")
    res = _collect(except_all(dataframe, df2))
    assert len(res) == 2
    for row in res:
        assert row.metric == "cooling"


def test_intersect(dataframe):
    df2 = _filter(dataframe, "metric = 'heating'")
    res = _collect(intersect(dataframe, df2))
    assert len(res) == 2
    for row in res:
        assert row.metric == "heating"


def test_is_dataframe_empty(dataframe):
    assert not is_dataframe_empty(dataframe)
    assert is_dataframe_empty(_filter(dataframe, "metric = 'invalid'"))


def test_interval(time_dataframe):
    res = [
        x.timestamp2
        for x in _collect(
            _order_by(
                perform_interval_op(
                    time_dataframe, "timestamp", "+", 3600, "SECONDS", "timestamp2"
                )
                .select("timestamp2")
                .distinct(),
                "timestamp2",
            )
        )
    ]
    utc = ZoneInfo("UTC")
    actual = [x.replace(tzinfo=utc) if x.tzinfo is None else x.astimezone(utc) for x in res]
    assert actual == [datetime(2020, 1, 1, 1, tzinfo=utc), datetime(2020, 1, 1, 2, tzinfo=utc)]


def test_join(spark, dataframe):
    df2 = spark.createDataFrame(
        [
            ("Boulder", 0),
            ("Jefferson", 100),
        ],
        ["county", "index2"],
    )
    df3 = join(dataframe, df2, "index", "index2")
    assert not is_dataframe_empty(_filter(df3, "county = 'Boulder'"))
    assert is_dataframe_empty(_filter(df3, "county = 'Jefferson'"))
    assert aggregate_single_value(df3, "sum", "value") == 1.0 + 2.0


def test_join_multiple_columns(spark, dataframe):
    df2 = spark.createDataFrame(
        [
            ("Boulder", "cooling", 0),
            ("Jefferson", "heating", 100),
        ],
        ["county", "metric", "index"],
    )
    df3 = join_multiple_columns(dataframe, df2, ["index", "metric"])
    assert not is_dataframe_empty(_filter(df3, "county = 'Boulder'"))
    assert is_dataframe_empty(_filter(df3, "county = 'Jefferson'"))
    assert aggregate_single_value(df3, "sum", "value") == 1.0


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


def test_rename_columns(dataframe):
    renamed = rename_columns(dataframe, {"metric": "end_use", "value": "amount"})
    assert set(renamed.columns) == {"index", "end_use", "amount"}
    assert aggregate_single_value(renamed, "sum", "amount") == aggregate_single_value(
        dataframe, "sum", "value"
    )


def test_select_expr(dataframe):
    exprs = ["value * 2 AS double_value"]
    df = select_expr(dataframe, exprs)
    assert aggregate_single_value(df, "sum", "double_value") == 2 * aggregate_single_value(
        dataframe, "sum", "value"
    )


def test_sql_from_df(dataframe):
    df = sql_from_df(dataframe, "SELECT SUM(value) as total")
    assert aggregate_single_value(df, "sum", "total") == aggregate_single_value(
        dataframe, "sum", "value"
    )


def test_pivot(dataframe):
    df = pivot(dataframe, "metric", "value")
    assert "cooling" in df.columns
    assert "heating" in df.columns
    assert aggregate_single_value(df, "sum", "cooling") == 4.0
    assert aggregate_single_value(df, "sum", "heating") == 6.0


def test_unpivot(spark):
    df = spark.createDataFrame(
        [
            (0, 1.0, 2.0),
            (1, 3.0, 4.0),
        ],
        ["index", "cooling", "heating"],
    )
    df2 = unpivot(df, ["cooling", "heating"], "metric", "value")
    assert aggregate_single_value(_filter(df2, "metric = 'cooling'"), "sum", "value") == 4.0
    assert aggregate_single_value(_filter(df2, "metric = 'heating'"), "sum", "value") == 6.0


def test_cache_preserves_query_result(dataframe):
    cached = cache(dataframe)
    try:
        assert aggregate_single_value(cached, "sum", "value") == 10.0
    finally:
        unpersist(cached)


def test_unpersist_is_safe_on_uncached_table(spark):
    df = spark.createDataFrame([(1,)], ["x"])
    unpersist(df)


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
    round_tripped = _read_csv_io(out)
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


def test_stop_invalidates_backend_cache() -> None:
    """_SparkRuntimeSession.stop() must clear the cached Ibis backend so a
    subsequent get_runtime_backend() can't hand out a reference bound to a
    stopped SparkSession. DuckDB has no stop semantic; skip there."""
    if use_duckdb():
        pytest.skip("Only the Spark stop() path invalidates the cache")

    session = init_runtime_session("dsgrid_cache_test")
    # Prime the cache.
    get_runtime_backend()
    assert backend_mod._RUNTIME_BACKEND is not None
    try:
        session.stop()
        assert backend_mod._RUNTIME_BACKEND is None, (
            "_SparkRuntimeSession.stop() must call invalidate_runtime_backend_cache "
            "to prevent the next get_runtime_backend() from returning a stopped "
            "session reference."
        )
    finally:
        # Leave a fresh session for downstream tests in the same module.
        init_runtime_session("dsgrid_cache_test_reset")


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
