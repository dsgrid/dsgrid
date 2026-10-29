from dsgrid.ibis.functions import (
    aggregate,
    cache,
    collect_list,
    is_dataframe_empty,
    select_expr,
    unpersist,
)
from dsgrid.ibis.operations import aggregate_single_value, filter_sql

from tests._helpers import collect as _collect


def _filter(df, predicate):
    return filter_sql(df, predicate)


def test_aggregate(dataframe):
    assert _collect(aggregate(dataframe, "sum", "value", "s").select("s"))[0].s == 10.0


def test_collect_list(dataframe):
    assert sorted(collect_list(dataframe, "metric")) == [
        "cooling",
        "cooling",
        "heating",
        "heating",
    ]


def test_is_dataframe_empty(dataframe):
    assert not is_dataframe_empty(dataframe)
    assert is_dataframe_empty(_filter(dataframe, "metric = 'invalid'"))


def test_select_expr(dataframe):
    exprs = ["value * 2 AS double_value"]
    df = select_expr(dataframe, exprs)
    assert aggregate_single_value(df, "sum", "double_value") == 2 * aggregate_single_value(
        dataframe, "sum", "value"
    )


def test_cache_preserves_query_result(dataframe):
    cached = cache(dataframe)
    try:
        assert aggregate_single_value(cached, "sum", "value") == 10.0
    finally:
        unpersist(cached)


def test_unpersist_is_safe_on_uncached_table(spark):
    df = spark.createDataFrame([(1,)], ["x"])
    unpersist(df)
