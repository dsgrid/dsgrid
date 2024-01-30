from pyspark.sql import DataFrame

from dsgrid.utils.spark import (
    get_spark_session,
    try_read_dataframe,
    restart_spark_with_custom_conf,
)


def test_try_read_dataframe_invalid(tmp_path):
    invalid = tmp_path / "table.parquet"
    invalid.mkdir()
    assert try_read_dataframe(invalid) is None
    assert not invalid.exists()


def test_try_read_dataframe_valid(tmp_path):
    spark = get_spark_session()
    df = spark.createDataFrame([{"a": 1}])
    filename = tmp_path / "table.parquet"
    df.write.parquet(str(filename))
    df = try_read_dataframe(filename)
    assert isinstance(df, DataFrame)
    assert df.collect()[0].a == 1


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
