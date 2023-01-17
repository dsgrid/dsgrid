from pyspark.sql import DataFrame

from dsgrid.utils.spark import get_spark_session, try_read_dataframe


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
