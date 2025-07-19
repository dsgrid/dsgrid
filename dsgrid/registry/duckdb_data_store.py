import logging
from pathlib import Path
from typing import Literal, Self

import duckdb
from duckdb import DuckDBPyConnection

from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.spark.functions import get_spark_session
from dsgrid.spark.types import DataFrame


DATABASE_FILENAME = "data.duckdb"
SCHEMA_DATA = "dsgrid_data_tables"
SCHEMA_LOOKUP_DATA = "dsgrid_lookup_tables"
TABLE_TYPE_TO_SCHEMA = {"data": SCHEMA_DATA, "lookup": SCHEMA_LOOKUP_DATA}

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
        table_name = self._make_table_name("data", dataset_id, version)
        df = con.sql(f"SELECT * FROM {table_name}").to_df()
        return get_spark_session().createDataFrame(df)

    def read_lookup_table(self, dataset_id: str, version: str) -> DataFrame:
        con = self._get_connection()
        table_name = self._make_table_name("lookup", dataset_id, version)
        df = con.sql(f"SELECT * FROM {table_name}").to_df()
        return get_spark_session().createDataFrame(df)

    def write_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        con = self._get_connection()
        table_name = self._make_table_name("data", dataset_id, version)
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table_name}")
        _create_table_from_dataframe(con, df, table_name)

    def write_lookup_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        con = self._get_connection()
        table_name = self._make_table_name("lookup", dataset_id, version)
        if overwrite:
            con.sql(f"DROP TABLE IF EXISTS {table_name}")
        _create_table_from_dataframe(con, df, table_name)

    def remove_tables(self, dataset_id: str, version: str) -> None:
        con = self._get_connection()
        table_name = self._make_table_name("data", dataset_id, version)
        con.sql(f"DROP TABLE {table_name}")
        lookup_table_name = self._make_table_name("lookup", dataset_id, version)
        con.sql(f"DROP TABLE IF EXISTS {lookup_table_name}")

    @property
    def _data_dir(self) -> Path:
        return self.base_path / "data"

    @property
    def _db_file(self) -> Path:
        return self.base_path / DATABASE_FILENAME

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self._db_file)

    @staticmethod
    def _make_table_name(
        base_name: Literal["data", "lookup"], dataset_id: str, version: str
    ) -> str:
        schema = TABLE_TYPE_TO_SCHEMA[base_name]
        # Replace dots so that manual SQL queries don't have to escape them.
        ver = version.replace(".", "_")
        return f"{schema}.{dataset_id}__{ver}"


def _create_table_from_dataframe(con: DuckDBPyConnection, df: DataFrame, table_name: str) -> None:
    pdf = df.toPandas()  # noqa: F841
    con.sql(f"CREATE TABLE {table_name} AS SELECT * from pdf")
