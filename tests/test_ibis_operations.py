import pytest

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.functions import is_dataframe_empty
from dsgrid.ibis.operations import (
    aggregate_single_value,
    count_distinct_on_group_by,
    cross_join,
    cross_join_dfs,
    except_all,
    filter_sql,
    intersect,
    join,
    join_multiple_columns,
    pivot,
    rename_columns,
    sql_from_df,
    union_all,
    unpivot,
)
from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.table_utils import count_rows

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


def test_cross_join_dfs_and_union_all():
    table = get_runtime_session().createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert cross_join_dfs([table]).columns == table.columns
    other = get_runtime_session().createDataFrame([("x",), ("y",)], ["letter"])
    assert cross_join_dfs([table, other]).count().execute() == 4
    # union_all preserves duplicates (matches Spark UNION ALL); Ibis's default
    # .union() would dedupe.
    assert union_all(table, table).count().execute() == 4
