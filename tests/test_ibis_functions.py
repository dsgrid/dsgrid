from datetime import datetime
from pathlib import Path
from typing import Generator
from zoneinfo import ZoneInfo

import ibis
import pandas as pd
import pytest

from dsgrid.ibis.operations import filter_sql, rename_columns
from dsgrid.ibis.table_utils import table_to_pandas
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
)
from dsgrid.utils.files import dump_json_file
from dsgrid.ibis.session import (
    get_runtime_session,
    SparkSession,
)


def _collect(df):
    if hasattr(df, "execute"):
        return list(table_to_pandas(df).itertuples(index=False, name="Row"))
    return df.collect()


def _count(df):
    count = df.count()
    if hasattr(count, "execute"):
        return count.execute()
    return count


def _filter(df, predicate):
    return filter_sql(df, predicate)


def _order_by(df, *columns):
    if hasattr(df, "order_by"):
        return df.order_by(*columns)
    return df.sort(*columns)


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
    cache(df)
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
    cache(df)
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
    cache(df)
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
    assert _count(df) == _count(dataframe) * _count(geo_dataframe)
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
    assert int(row.a) == 2
    assert isinstance(row.b, str) and row.b == "c"
    assert float(row.c) == 2.0
    assert datetime.fromisoformat(row.d)

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
