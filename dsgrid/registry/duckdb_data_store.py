import logging
from pathlib import Path
from typing import Any, Self, cast

import ibis
import pandas as pd
from chronify.ibis import IbisBackend, make_backend

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.registry.data_store_interface import DataStoreInterface


DATABASE_FILENAME = "data.duckdb"
SCHEMA_DATA = "dsgrid_data"
SCHEMA_LOOKUP_DATA = "dsgrid_lookup"
SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS = "dsgrid_expected_dimension_associations"
SCHEMA_MISSING_DIMENSION_ASSOCIATIONS = "dsgrid_missing_dimension_associations"
TABLE_TYPE_TO_SCHEMA = {
    "data": SCHEMA_DATA,
    "lookup": SCHEMA_LOOKUP_DATA,
    "expected_dimension_associations": SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS,
    "missing_dimension_associations": SCHEMA_MISSING_DIMENSION_ASSOCIATIONS,
}

logger = logging.getLogger(__name__)


class DuckDbDataStore(DataStoreInterface):
    """Data store that stores tables in a DuckDB database."""

    def __init__(self, base_path: Path):
        super().__init__(base_path)
        if dsgrid.runtime_config.backend_engine == BackendEngine.SPARK:
            # This store uses a DuckDB backend, so it is incompatible with the Spark backend.
            msg = "Spark backend engine is not supported with DuckDbDataStore."
            raise DSGInvalidOperation(msg)
        self._backend = make_backend("duckdb", database=str(self._db_file))

    @classmethod
    def create(cls, base_path: Path) -> Self:
        base_path.mkdir(exist_ok=True)
        db_file = base_path / DATABASE_FILENAME
        if db_file.exists():
            msg = f"Database file {db_file} already exists. Cannot initialize DuckDB data store."
            raise FileExistsError(msg)
        store = cls(base_path)
        store._backend.execute_sql(f"CREATE SCHEMA {SCHEMA_DATA}")
        store._backend.execute_sql(f"CREATE SCHEMA {SCHEMA_LOOKUP_DATA}")
        store._backend.execute_sql(f"CREATE SCHEMA {SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS}")
        store._backend.execute_sql(f"CREATE SCHEMA {SCHEMA_MISSING_DIMENSION_ASSOCIATIONS}")
        return store

    @classmethod
    def load(cls, base_path: Path) -> Self:
        """Load an existing DuckDB data store from the given base path."""
        db_file = base_path / DATABASE_FILENAME
        if not db_file.exists():
            msg = f"Database file {db_file} does not exist."
            raise FileNotFoundError(msg)

        return cls(base_path)

    def read_table(self, dataset_id: str, version: str) -> ibis.Table:
        schema = TABLE_TYPE_TO_SCHEMA["data"]
        table_name = _make_table_short_name(dataset_id, version)
        return self._read_table(schema, table_name)

    def replace_table(self, df: ibis.Table, dataset_id: str, version: str) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["data"]
        short_name = _make_table_short_name(dataset_id, version)
        self._replace_table(df, schema, short_name)

    def read_lookup_table(self, dataset_id: str, version: str) -> ibis.Table:
        schema = TABLE_TYPE_TO_SCHEMA["lookup"]
        table_name = _make_table_short_name(dataset_id, version)
        return self._read_table(schema, table_name)

    def replace_lookup_table(self, df: ibis.Table, dataset_id: str, version: str) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["lookup"]
        short_name = _make_table_short_name(dataset_id, version)
        self._replace_table(df, schema, short_name)

    def read_expected_associations_tables(
        self, dataset_id: str, version: str
    ) -> dict[str, ibis.Table]:
        dfs: dict[str, ibis.Table] = {}
        names = self._list_expected_associations_table_names(dataset_id, version)
        if not names:
            return dfs
        for name in names:
            dfs[name] = self._read_table(SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS, name)
        return dfs

    def read_missing_associations_tables(
        self, dataset_id: str, version: str
    ) -> dict[str, ibis.Table]:
        dfs: dict[str, ibis.Table] = {}
        names = self._list_missing_associations_table_names(dataset_id, version)
        if not names:
            return dfs
        for name in names:
            dfs[name] = self._read_table(SCHEMA_MISSING_DIMENSION_ASSOCIATIONS, name)
        return dfs

    def write_table(
        self, df: ibis.Table, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["data"]
        table_name = _make_table_short_name(dataset_id, version)
        if overwrite:
            self._drop_table(schema, table_name, if_exists=True)
        _create_table_from_dataframe(self._backend, df, schema, table_name)

    def write_lookup_table(
        self, df: ibis.Table, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["lookup"]
        table_name = _make_table_short_name(dataset_id, version)
        if overwrite:
            self._drop_table(schema, table_name, if_exists=True)
        _create_table_from_dataframe(self._backend, df, schema, table_name)

    def write_expected_associations_tables(
        self, dfs: dict[str, ibis.Table], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["expected_dimension_associations"]
        base_name = _make_table_short_name(dataset_id, version)
        for tag, df in dfs.items():
            table_name = f"{base_name}__{tag}"
            if overwrite:
                self._drop_table(schema, table_name, if_exists=True)
            _create_table_from_dataframe(self._backend, df, schema, table_name)

    def write_missing_associations_tables(
        self, dfs: dict[str, ibis.Table], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["missing_dimension_associations"]
        base_name = _make_table_short_name(dataset_id, version)
        for tag, df in dfs.items():
            table_name = f"{base_name}__{tag}"
            if overwrite:
                self._drop_table(schema, table_name, if_exists=True)
            _create_table_from_dataframe(self._backend, df, schema, table_name)

    def remove_tables(self, dataset_id: str, version: str) -> None:
        for table_type in ("data", "lookup"):
            schema = TABLE_TYPE_TO_SCHEMA[table_type]
            table_name = _make_table_short_name(dataset_id, version)
            self._drop_table(schema, table_name, if_exists=True)
        for name in self._list_expected_associations_table_names(dataset_id, version):
            self._drop_table(SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS, name, if_exists=True)
        for name in self._list_missing_associations_table_names(dataset_id, version):
            self._drop_table(SCHEMA_MISSING_DIMENSION_ASSOCIATIONS, name, if_exists=True)

    @property
    def _data_dir(self) -> Path:
        return self.base_path / "data"

    @property
    def _db_file(self) -> Path:
        return self.base_path / DATABASE_FILENAME

    def _read_table(self, schema: str, table_name: str) -> ibis.Table:
        return self._backend.connection.table(table_name, database=schema)

    def _drop_table(self, schema: str, table_name: str, if_exists: bool = False) -> None:
        if_clause = "IF EXISTS " if if_exists else ""
        self._backend.execute_sql(
            f"DROP TABLE {if_clause}{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
        )

    def _has_table(self, schema: str, table_name: str) -> bool:
        count = cast(
            Any,
            self._backend.execute_sql_to_df(
                f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        """
            ).iloc[0, 0],
        )
        return count > 0

    def _replace_table(self, df: ibis.Table, schema: str, table_name: str) -> None:
        if not self._has_table(schema, table_name):
            _create_table_from_dataframe(self._backend, df, schema, table_name)
            return

        tmp_name = f"{table_name}_tmp"
        _create_table_from_dataframe(self._backend, df, schema, tmp_name)
        self._drop_table(schema, table_name)
        self._backend.execute_sql(
            f"ALTER TABLE {_quote_identifier(schema)}.{_quote_identifier(tmp_name)} "
            f"RENAME TO {_quote_identifier(table_name)}"
        )

    def _list_expected_associations_table_names(self, dataset_id: str, version: str) -> list[str]:
        short_name = _make_table_short_name(dataset_id, version)
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{TABLE_TYPE_TO_SCHEMA["expected_dimension_associations"]}' AND table_name LIKE '%{short_name}%'
        """
        return self._backend.execute_sql_to_df(query)["table_name"].to_list()

    def _list_missing_associations_table_names(self, dataset_id: str, version: str) -> list[str]:
        short_name = _make_table_short_name(dataset_id, version)
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{TABLE_TYPE_TO_SCHEMA["missing_dimension_associations"]}' AND table_name LIKE '%{short_name}%'
        """
        return self._backend.execute_sql_to_df(query)["table_name"].to_list()

    def close(self) -> None:
        self._backend.dispose()


def _create_table_from_dataframe(
    backend: IbisBackend, df: ibis.Table, schema: str, table_name: str
) -> None:
    """Materialize a table into the on-disk DuckDB store.

    For ibis.Table inputs that live in a *different* backend (e.g. the
    runtime in-memory backend), we materialize via PyArrow rather than
    via a temp Parquet file. PyArrow avoids the parquet write + read
    round-trip on disk for the typical small/medium-sized payloads
    (dimension records, lookup tables, association tables) at the cost
    of buffering the whole table in driver memory. Very large
    cross-backend transfers should pre-write Parquet themselves and
    use a different store-write path.
    """
    if isinstance(df, ibis.Table):
        arrow_table = df.to_pyarrow()
        backend.connection.create_table(
            table_name,
            obj=arrow_table,
            database=schema,
            overwrite=False,
        )
        return

    backend.connection.create_table(
        table_name,
        obj=_as_ibis_table(df),
        database=schema,
        overwrite=False,
    )


def _as_ibis_table(df: Any) -> ibis.Table:
    if isinstance(df, ibis.Table):
        return df
    if isinstance(df, pd.DataFrame):
        return ibis.memtable(df)
    msg = f"Unsupported table type: {type(df)}"
    raise TypeError(msg)


def _make_table_short_name(dataset_id: str, version: str) -> str:
    # Replace dots so that manual SQL queries don't have to escape them.
    ver = version.replace(".", "_")
    return f"{dataset_id}__{ver}"


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
