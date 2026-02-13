# flake8: noqa

import dsgrid
from dsgrid.common import BackendEngine


def use_duckdb() -> bool:
    """Return True if the environment is set to use DuckDB instead of Spark."""
    return dsgrid.runtime_config.backend_engine == BackendEngine.DUCKDB


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
        ShortType,
        LongType,
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
        LongType,
        ShortType,
        BooleanType,
        TimestampType,
        TimestampNTZType,
    )
    from pyspark.errors import AnalysisException
    from pyspark import SparkConf


SUPPORTED_TYPES = set(
    (
        "BOOLEAN",
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "TIMESTAMP_TZ",
        "TIMESTAMP_NTZ",
        "STRING",
        "TEXT",
        "VARCHAR",
    )
)

DUCKDB_COLUMN_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "TINYINT": "TINYINT",
    "SMALLINT": "INTEGER",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "VARCHAR": "VARCHAR",
}

SPARK_COLUMN_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "INT": "INT",
    "INTEGER": "INT",
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "STRING": "STRING",
    "TEXT": "STRING",
    "VARCHAR": "STRING",
    "TIMESTAMP_TZ": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
}

assert sorted(DUCKDB_COLUMN_TYPES.keys()) == sorted(SPARK_COLUMN_TYPES.keys())
assert not SUPPORTED_TYPES.difference(DUCKDB_COLUMN_TYPES.keys())


def get_str_type() -> str:
    """Return the string type used by the current database system."""
    types = DUCKDB_COLUMN_TYPES if use_duckdb() else SPARK_COLUMN_TYPES
    return types["STRING"]
