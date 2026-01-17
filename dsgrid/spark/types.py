# flake8: noqa

import dsgrid
from dsgrid.common import BackendEngine
from typing import Any


# Import PySpark components if available, primarily for legacy support and
# specific Spark optimizations (like salting).
try:
    import pyspark.sql.functions as F
    from pyspark.sql import Row, SparkSession
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
except ImportError:
    # Mock for environments without Spark (e.g. pure DuckDB environment?)
    F = Any
    Row = Any
    SparkSession = Any
    SparkConf = Any
    AnalysisException = Exception

    # Define dummy types if needed, or rely on them being Any
    ByteType = Any
    FloatType = Any
    StructType = Any
    StructField = Any
    StringType = Any
    DoubleType = Any
    IntegerType = Any
    LongType = Any
    ShortType = Any
    BooleanType = Any
    TimestampType = Any
    TimestampNTZType = Any

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
    if dsgrid.runtime_config.backend_engine == BackendEngine.DUCKDB:
        return DUCKDB_COLUMN_TYPES["STRING"]
    return SPARK_COLUMN_TYPES["STRING"]
