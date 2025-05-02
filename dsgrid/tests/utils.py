from pathlib import Path

from dsgrid.spark.types import use_duckdb
from dsgrid.utils.spark import get_spark_session


def read_parquet(filename: Path):
    """Read a Parquet file and load it into cache. This helps debugging with pytest --pdb.
    If you don't use this, the parquet file will get deleted on a failure and you won't be able
    to inspect the dataframe.
    """
    spark = get_spark_session()
    df = spark.read.parquet(str(filename))
    if not use_duckdb():
        df.cache()
        df.count()
    return df


def read_parquet_two_table_format(path: Path):
    spark = get_spark_session()
    load_data = spark.read.parquet(str(path / "load_data.parquet"))
    lookup = spark.read.parquet(str(path / "load_data_lookup.parquet"))
    table = load_data.join(lookup, on="id").drop("id")
    return table
