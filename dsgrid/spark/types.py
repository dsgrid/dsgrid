# flake8: noqa

import os


def use_duckdb() -> bool:
    """Return True if the environment is set to use DuckDB instead of Spark."""
    return os.getenv("USE_DUCKDB", "false").lower() == "true"


if use_duckdb():
    import duckdb.experimental.spark.sql.functions as F
    from duckdb.experimental.spark.conf import SparkConf
    from duckdb.experimental.spark.sql import DataFrame, SparkSession
    from duckdb.experimental.spark.sql.types import (
        ByteType,
        StructField,
        StructType,
        StringType,
        BooleanType,
        IntegerType,
        DoubleType,
        FloatType,
        TimestampType,
        TimestampNTZType,
        Row,
    )
    from duckdb.experimental.spark.errors import AnalysisException
else:
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrame, Row, SparkSession
    from pyspark.sql.types import (
        ByteType,
        FloatType,
        StructType,
        StructField,
        StringType,
        DoubleType,
        IntegerType,
        BooleanType,
        TimestampType,
        TimestampNTZType,
    )
    from pyspark.errors import AnalysisException
    from pyspark import SparkConf
