from typing import Any

import dsgrid
from dsgrid.common import BackendEngine


SUPPORTED_TYPES = {
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
}

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


def use_duckdb() -> bool:
    """Return True if the configured execution backend is DuckDB."""
    return dsgrid.runtime_config.backend_engine == BackendEngine.DUCKDB


def get_str_type() -> str:
    """Return the string type used by the current database system."""
    types = DUCKDB_COLUMN_TYPES if use_duckdb() else SPARK_COLUMN_TYPES
    return types["STRING"]


def is_string_type(data_type: Any) -> bool:
    """Return True if a Spark-style schema data type represents a string."""
    if hasattr(data_type, "is_string"):
        return data_type.is_string()
    return data_type.__class__.__name__ == "StringType"


def is_string_column(table: Any, column: str) -> bool:
    """Return True if the table column has a string data type."""
    schema = table.schema
    if callable(schema):
        schema = schema()
    data_type = schema[column]
    if hasattr(data_type, "dataType"):
        data_type = data_type.dataType
    return is_string_type(data_type)


def is_table_empty(table: Any) -> bool:
    """Return True if a table-like object has no rows."""
    return table.limit(1).count().execute() == 0
