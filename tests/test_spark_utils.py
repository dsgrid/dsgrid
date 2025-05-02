from functools import reduce
from typing import Optional

import pytest

from dsgrid.spark.types import (
    DataFrame,
    use_duckdb,
)
from dsgrid.time.types import DayType
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    get_spark_session,
    get_type_from_union,
    try_read_dataframe,
    restart_spark,
    restart_spark_with_custom_conf,
    create_dataframe_from_product,
    custom_spark_conf,
)


def test_try_read_dataframe_invalid(tmp_path):
    invalid = tmp_path / "table.parquet"
    invalid.mkdir()
    assert try_read_dataframe(invalid) is None
    assert not invalid.exists()


def test_try_read_dataframe_valid(tmp_path):
    spark = get_spark_session()
    df = spark.createDataFrame([(1,)], ["a"])
    filename = tmp_path / "table.parquet"
    df.write.parquet(str(filename))
    df = try_read_dataframe(filename)
    assert isinstance(df, DataFrame)
    assert df.collect()[0].a == 1


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_restart_spark():
    spark = get_spark_session()
    cur_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    new_partitions = str(int(cur_partitions) + 1)
    cur_compress = spark.conf.get("spark.rdd.compress")
    if cur_compress.lower() == "true":
        new_compress = "false"
    elif cur_compress.lower() == "false":
        new_compress = "true"
    else:
        assert False, cur_compress

    conf = {
        "spark.sql.shuffle.partitions": new_partitions,
        "spark.rdd.compress": new_compress,
    }
    with restart_spark_with_custom_conf(conf=conf) as new_spark:
        assert new_spark.conf.get("spark.sql.shuffle.partitions") == new_partitions
        assert new_spark.conf.get("spark.rdd.compress") == new_compress


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_custom_spark_conf():
    orig_session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    assert orig_session_tz != "UTC"
    conf = {"spark.sql.session.timeZone": "UTC"}
    with custom_spark_conf(conf):
        assert get_spark_session().conf.get("spark.sql.session.timeZone") == "UTC"
        restart_spark(force=True)
        assert get_spark_session().conf.get("spark.sql.session.timeZone") == "UTC"
    assert get_spark_session().conf.get("spark.sql.session.timeZone") == orig_session_tz


def test_create_dataframe_from_product(tmp_path):
    with ScratchDirContext(tmp_path / "scratch") as context:
        data = {
            "geography": [f"county_{i}" for i in range(200)],
            "scenario": [f"scenario_{i}" for i in range(10)],
            "model_year": [str(x) for x in range(2020, 2030)],
            "sector": ["com", "ind", "res", "trans"],
        }
        df = create_dataframe_from_product(data, context, max_partition_size_mb=1)
        assert df.count() == reduce(lambda x, y: x * y, [len(x) for x in data.values()])


def test_get_type_from_union():
    assert get_type_from_union(Optional[str]) is str
    assert get_type_from_union(Optional[DayType]) is str
