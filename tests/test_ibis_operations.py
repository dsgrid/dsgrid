import ibis
import pandas as pd
import pytest

import dsgrid.ibis.temp as ibis_temp
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.functions import is_dataframe_empty
from dsgrid.ibis.operations import (
    _ensure_same_backend,
    _sole_backend,
    aggregate_single_value,
    count_distinct_on_group_by,
    create_temp_view,
    cross_join,
    cross_join_dfs,
    except_all,
    filter_sql,
    intersect,
    join,
    join_multiple_columns,
    max_by_group,
    pivot,
    rename_columns,
    sql_from_df,
    union_all,
    unpivot,
)
from dsgrid.ibis.temp import drop_temp_tables_and_views
from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.table_utils import count_rows
from dsgrid.ibis.types import use_duckdb

from tests._helpers import collect as _collect


def _filter(df, predicate):
    return filter_sql(df, predicate)


def test_aggregate_single_value(dataframe):
    assert aggregate_single_value(dataframe, "sum", "value") == 10.0


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


def test_join_rejects_overlapping_columns(spark, dataframe):
    """df2 columns that collide with df1 must be rejected, not silently dropped."""
    df2 = spark.createDataFrame([("cooling", 0)], ["metric", "index2"])
    with pytest.raises(DSGInvalidOperation, match="metric"):
        join(dataframe, df2, "index", "index2")


def test_join_rejects_overlapping_join_key(spark, dataframe):
    """column2 is retained for the caller to drop, so it may not collide either."""
    df2 = spark.createDataFrame([(0, "Boulder")], ["index", "county"])
    with pytest.raises(DSGInvalidOperation, match="index"):
        join(dataframe, df2, "index", "index")


def test_join_semi_anti_ignores_overlap(spark, dataframe):
    """Semi/anti joins project only df1's columns, so collisions are harmless."""
    df2 = spark.createDataFrame([(0, "cooling")], ["index", "metric"])
    semi = join(dataframe, df2, "index", "index", how="semi")
    assert semi.columns == dataframe.columns
    assert aggregate_single_value(semi, "sum", "value") == 1.0 + 2.0
    anti = join(dataframe, df2, "index", "index", how="anti")
    assert anti.columns == dataframe.columns
    assert aggregate_single_value(anti, "sum", "value") == 3.0 + 4.0


def test_join_multiple_columns_rejects_non_key_overlap(spark, dataframe):
    """Non-key collisions hold independent data and cannot be reconciled by a join."""
    df2 = spark.createDataFrame([(0, "cooling", 9.0)], ["index", "metric", "value"])
    with pytest.raises(DSGInvalidOperation, match="value"):
        join_multiple_columns(dataframe, df2, ["index", "metric"])


def test_join_multiple_columns_deduplicates_join_keys(spark, dataframe):
    """The equi-join guarantees the keys match, so one copy is kept."""
    df2 = spark.createDataFrame([(0, "cooling", "Boulder")], ["index", "metric", "county"])
    df3 = join_multiple_columns(dataframe, df2, ["index", "metric"])
    assert df3.columns == ("index", "metric", "value", "county")


def test_max_by_group(dataframe):
    """cooling is max(1.0, 3.0); heating is max(2.0, 4.0)."""
    df = max_by_group(dataframe, ["metric"], ["value"])
    assert set(df.columns) == {"metric", "value"}
    assert {(row.metric, row.value) for row in _collect(df)} == {
        ("cooling", 3.0),
        ("heating", 4.0),
    }


def test_max_by_group_multiple_value_columns(spark):
    df = spark.createDataFrame(
        [
            ("cooling", 1.0, 10.0),
            ("cooling", 3.0, 5.0),
        ],
        ["metric", "value", "other"],
    )
    res = _collect(max_by_group(df, ["metric"], ["value", "other"]))
    assert len(res) == 1
    assert (res[0].value, res[0].other) == (3.0, 10.0)


# create_temp_view's parquet fallback issues DuckDB-only SQL, so relocating a foreign
# table cannot work under a Spark runtime. dsgrid never hits that combination:
# DuckDbDataStore refuses to construct when the backend engine is Spark.
_needs_duckdb = pytest.mark.skipif(
    not use_duckdb(), reason="relocating a foreign table requires a DuckDB runtime"
)


@pytest.fixture
def foreign_table():
    """A table in a separate DuckDB connection, i.e. a store that never ATTACHed."""
    connection = ibis.duckdb.connect()
    return connection.create_table("foreign", pd.DataFrame({"index2": [0, 1], "y": [1.0, 2.0]}))


def test_sole_backend_reports_runtime_and_unbound(dataframe):
    assert _sole_backend(dataframe) is get_runtime_backend().connection
    assert _sole_backend(ibis.memtable({"index": [0]})) is None


def test_sole_backend_rejects_table_spanning_backends(foreign_table):
    """An operand already spanning two backends cannot be repaired by relocating it."""
    other = ibis.sqlite.connect()
    sqlite_table = other.create_table("s", pd.DataFrame({"index2": [0], "z": [5.0]}))
    spanning = foreign_table.join(sqlite_table, "index2")
    with pytest.raises(DSGInvalidOperation, match="spans multiple backends"):
        _sole_backend(spanning)


def test_ensure_same_backend_leaves_runtime_bound_tables_alone(dataframe):
    df1, df2 = _ensure_same_backend(dataframe, dataframe)
    assert df1 is dataframe
    assert df2 is dataframe


def test_ensure_same_backend_leaves_unbound_tables_alone():
    """Neither operand carries a backend, so there is nothing to relocate."""
    memtable = ibis.memtable({"index": [0]})
    df1, df2 = _ensure_same_backend(memtable, memtable)
    assert df1 is memtable
    assert df2 is memtable


@_needs_duckdb
def test_ensure_same_backend_relocates_foreign_table(dataframe, foreign_table):
    df1, df2 = _ensure_same_backend(dataframe, foreign_table)
    assert df1 is dataframe
    assert _sole_backend(df2) is get_runtime_backend().connection


@_needs_duckdb
def test_ensure_same_backend_relocates_foreign_table_beside_unbound_table(foreign_table):
    """An unbound df1 must not suppress the check on df2.

    Both operands used to be resolved inside one try block, so a memtable on the
    left returned the foreign table on the right untouched, and the query then ran
    against the foreign connection instead of the runtime.
    """
    memtable = ibis.memtable({"index2": [0, 1], "z": [7.0, 6.0]})
    df1, df2 = _ensure_same_backend(memtable, foreign_table)
    assert df1 is memtable
    assert _sole_backend(df2) is get_runtime_backend().connection


@_needs_duckdb
def test_ensure_same_backend_relocates_foreign_table_on_the_left(dataframe, foreign_table):
    df1, df2 = _ensure_same_backend(foreign_table, dataframe)
    assert _sole_backend(df1) is get_runtime_backend().connection
    assert df2 is dataframe


@_needs_duckdb
def test_join_across_backends_executes_on_the_runtime(dataframe, foreign_table):
    """End to end: a join against a non-ATTACHed store still produces correct rows."""
    joined = join(dataframe, foreign_table, "index", "index2")
    assert _sole_backend(joined) is get_runtime_backend().connection
    assert aggregate_single_value(joined, "sum", "y") == 1.0 + 1.0 + 2.0 + 2.0


@_needs_duckdb
def test_create_temp_view_parquet_fallback_materializes_and_cleans_up(foreign_table):
    """The parquet round-trip (strategy 3) must round-trip data correctly and its
    materialized file must be tracked and removed by drop_temp_tables_and_views.

    A table from a separate DuckDB connection cannot be reached by create_view or
    a cross-backend create_table, so create_temp_view falls through to the parquet
    round-trip. This forces that branch directly (rather than transitively via a
    join) and pins both halves of the reviewer's ask: correctness + cleanup.
    """
    before = set(ibis_temp._tracked_temp_files)
    view = create_temp_view(foreign_table)

    new_tracked = set(ibis_temp._tracked_temp_files) - before
    assert len(new_tracked) == 1, "parquet fallback should track exactly one temp file"
    tracked_file = next(iter(new_tracked))
    assert tracked_file.exists(), "the fallback must have materialized the table to disk"

    # The view round-trips the foreign data with no rows lost or altered.
    result = get_runtime_backend().table(view).execute().sort_values(by="index2")
    assert list(result["y"]) == [1.0, 2.0]

    # Cleanup removes the materialized parquet file and forgets it.
    drop_temp_tables_and_views()
    assert not tracked_file.exists(), "cleanup must delete the tracked parquet file"
    assert tracked_file not in ibis_temp._tracked_temp_files


def test_rename_columns(dataframe):
    renamed = rename_columns(dataframe, {"metric": "end_use", "value": "amount"})
    assert set(renamed.columns) == {"index", "end_use", "amount"}
    assert aggregate_single_value(renamed, "sum", "amount") == aggregate_single_value(
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
    # names_sort=True gives a stable, alphabetical pivot-column order (cooling before
    # heating) rather than DuckDB's unspecified scan order, so user-facing CSV/parquet
    # output is deterministic run-to-run.
    metric_columns = [c for c in df.columns if c in {"cooling", "heating"}]
    assert metric_columns == ["cooling", "heating"]
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


def test_unpivot_preserves_null_rows(spark):
    # Regression guard: the raw unpivot primitive must keep rows whose value is NULL
    # (Spark's ``df.unpivot`` and the old ``UNPIVOT INCLUDE NULLS`` SQL both did). If a
    # future Ibis default drops them, sparse pivoted load data would silently lose rows
    # and every downstream aggregation would change. ``unpivot_dataframe`` also relies on
    # these NULL rows surviving so it can collapse them into per-id null-time rows.
    df = spark.createDataFrame(
        [
            (0, 1.0, None),
            (1, 3.0, 4.0),
        ],
        ["index", "cooling", "heating"],
    )
    df2 = unpivot(df, ["cooling", "heating"], "metric", "value")
    # 2 rows * 2 pivoted columns; none dropped despite the NULL heating value.
    assert count_rows(df2) == 4
    null_rows = _collect(_filter(df2, "value IS NULL"))
    assert len(null_rows) == 1
    assert null_rows[0].metric == "heating"
    assert null_rows[0].index == 0


def test_cross_join_dfs_and_union_all():
    table = get_runtime_session().createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert cross_join_dfs([table]).columns == table.columns
    other = get_runtime_session().createDataFrame([("x",), ("y",)], ["letter"])
    assert cross_join_dfs([table, other]).count().execute() == 4
    # union_all preserves duplicates (matches Spark UNION ALL); Ibis's default
    # .union() would dedupe.
    assert union_all(table, table).count().execute() == 4
