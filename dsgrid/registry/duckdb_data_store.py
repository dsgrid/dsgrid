import logging
from pathlib import Path
from typing import Literal, Self

import duckdb
from duckdb import DuckDBPyConnection

from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.spark.functions import get_spark_session
from dsgrid.spark.types import DataFrame


DATABASE_FILENAME = "data.duckdb"
SCHEMA_DATA = "dsgrid_data"
SCHEMA_LOOKUP_DATA = "dsgrid_lookup"
SCHEMA_MISSING_DIMENSION_ASSOCIATIONS = "dsgrid_missing_dimension_associations"
TABLE_TYPE_TO_SCHEMA = {
    "data": SCHEMA_DATA,
    "lookup": SCHEMA_LOOKUP_DATA,
    "missing_dimension_associations": SCHEMA_MISSING_DIMENSION_ASSOCIATIONS,
}

logger = logging.getLogger(__name__)


class DuckDbDataStore(DataStoreInterface):
    """Data store that stores tables in Parquet files on the local or remote filesystem."""

    def __init__(self, base_path: Path):
        super().__init__(base_path)

    @classmethod
    def create(cls, base_path: Path) -> Self:
        base_path.mkdir(exist_ok=True)
        store = cls(base_path)
        db_file = base_path / DATABASE_FILENAME
        if db_file.exists():
            msg = f"Database file {db_file} already exists. Cannot initialize DuckDB data store."
            raise FileExistsError(msg)
        con = duckdb.connect(db_file)
        con.sql(f"CREATE SCHEMA {SCHEMA_DATA}")
        con.sql(f"CREATE SCHEMA {SCHEMA_LOOKUP_DATA}")
        con.sql(f"CREATE SCHEMA {SCHEMA_MISSING_DIMENSION_ASSOCIATIONS}")
        return store

    @classmethod
    def load(cls, base_path: Path) -> Self:
        """Load an existing DuckDB data store from the given base path."""
        db_file = base_path / DATABASE_FILENAME
        if not db_file.exists():
            msg = f"Database file {db_file} does not exist."
            raise FileNotFoundError(msg)

        return cls(base_path)

    def read_table(self, dataset_id: str, version: str) -> DataFrame:
        con = self._get_connection()
        table_name = _make_table_full_name("data", dataset_id, version)
        df = con.sql(f"SELECT * FROM {table_name}").to_df()
        return get_spark_session().createDataFrame(df)

    def replace_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["data"]
        short_name = _make_table_short_name(dataset_id, version)
        self._replace_table(df, schema, short_name)

    def read_lookup_table(self, dataset_id: str, version: str) -> DataFrame:
        con = self._get_connection()
        table_name = _make_table_full_name("lookup", dataset_id, version)
        df = con.sql(f"SELECT * FROM {table_name}").to_df()
        return get_spark_session().createDataFrame(df)

    def replace_lookup_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["lookup"]
        short_name = _make_table_short_name(dataset_id, version)
        self._replace_table(df, schema, short_name)

    def read_missing_associations_table(self, dataset_id: str, version: str) -> DataFrame | None:
        con = self._get_connection()
        full_name = _make_table_full_name("missing_dimension_associations", dataset_id, version)
        short_name = _make_table_short_name(dataset_id, version)
        if not self._has_table(con, SCHEMA_MISSING_DIMENSION_ASSOCIATIONS, short_name):
            return None
        df = con.sql(f"SELECT * FROM {full_name}").to_df()
        return get_spark_session().createDataFrame(df)

    def write_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        con = self._get_connection()
        table_name = _make_table_full_name("data", dataset_id, version)
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table_name}")
        _create_table_from_dataframe(con, df, table_name)

    def write_lookup_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        con = self._get_connection()
        table_name = _make_table_full_name("lookup", dataset_id, version)
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table_name}")
        _create_table_from_dataframe(con, df, table_name)

    def write_missing_associations_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        con = self._get_connection()
        table_name = _make_table_full_name("missing_dimension_associations", dataset_id, version)
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table_name}")
        _create_table_from_dataframe(con, df, table_name)

    def remove_tables(self, dataset_id: str, version: str) -> None:
        con = self._get_connection()
        for table_type in ("data", "lookup", "missing_dimension_associations"):
            table_name = _make_table_full_name(table_type, dataset_id, version)
            con.sql(f"DROP TABLE IF EXISTS {table_name}")

    @property
    def _data_dir(self) -> Path:
        return self.base_path / "data"

    @property
    def _db_file(self) -> Path:
        return self.base_path / DATABASE_FILENAME

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self._db_file)

    def _has_table(self, con: DuckDBPyConnection, schema: str, table_name: str) -> bool:
        return (
            con.sql(
                f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        """
            ).fetchone()[0]
            > 0
        )

    def _replace_table(self, df: DataFrame, schema: str, table_name: str) -> None:
        con = self._get_connection()
        if not self._has_table(con, schema, table_name):
            _create_table_from_dataframe(con, df, table_name)
            return

        tmp_name = f"{schema}.{table_name}_tmp"
        _create_table_from_dataframe(con, df, tmp_name)
        con.sql(f"DROP TABLE {table_name}")
        con.sql(f"ALTER TABLE {tmp_name} RENAME TO {table_name}")


def _create_table_from_dataframe(
    con: DuckDBPyConnection, df: DataFrame, full_table_name: str
) -> None:
    pdf = df.toPandas()  # noqa: F841
    con.sql(f"CREATE TABLE {full_table_name} AS SELECT * from pdf")


def _make_table_full_name(
    base_name: Literal["data", "lookup", "missing_dimension_associations"],
    dataset_id: str,
    version: str,
) -> str:
    schema = TABLE_TYPE_TO_SCHEMA[base_name]
    short_name = _make_table_short_name(dataset_id, version)
    return f"{schema}.{short_name}"


def _make_table_short_name(dataset_id: str, version: str) -> str:
    # Replace dots so that manual SQL queries don't have to escape them.
    ver = version.replace(".", "_")
    return f"{dataset_id}__{ver}"
