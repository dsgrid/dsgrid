from pathlib import Path

from pyspark.sql import SparkSession


def read_parquet(filename: Path):
    """Read a Parquet file and load it into cache. This helps debugging with pytest --pdb.
    If you don't use this, the parquet file will get deleted on a failure and you won't be able
    to inspect the dataframe.
    """
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    df = spark.read.parquet(str(filename)).cache()
    df.count()
    return df


def read_csv_single_table_format(path: Path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    return spark.read.csv(str(path), header=True, inferSchema=True)


def read_parquet_two_table_format(path: Path):
    spark = SparkSession.builder.appName("dgrid").getOrCreate()
    load_data = spark.read.parquet(str(path / "load_data.parquet"))
    lookup = spark.read.parquet(str(path / "load_data_lookup.parquet"))
    table = load_data.join(lookup, on="id").drop("id")
    return table
