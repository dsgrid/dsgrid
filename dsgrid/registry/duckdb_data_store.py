import logging
from pathlib import Path
from typing import Any, Self, cast

import ibis
import pandas as pd

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.backend import (
    attach_duckdb_file_to_runtime,
    get_attached_alias,
    get_runtime_backend,
)
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
    """Data store backed by a DuckDB file ATTACHed to the runtime backend.

    The store does not own a separate DuckDB connection — it ATTACHes its
    file (read-write) to the shared runtime DuckDB backend on init, so all
    its tables live in the same Ibis backend that the rest of dsgrid uses.
    Tables read from this store can therefore be joined directly against
    runtime tables without tripping the cross-backend fallback in
    :func:`~dsgrid.ibis.operations._ensure_same_backend`.

    All SQL statements emitted by this class qualify table references with
    the attach alias (``"alias"."schema"."table"``) so they target the
    attached database and not whatever schema happens to be on the runtime
    connection's search path.
    """

    def __init__(self, base_path: Path):
        super().__init__(base_path)
        if dsgrid.runtime_config.backend_engine == BackendEngine.SPARK:
            # The DuckDB ATTACH path requires a DuckDB runtime; with a Spark
            # runtime there is no DuckDB connection to attach to.
            msg = "Spark backend engine is not supported with DuckDbDataStore."
            raise DSGInvalidOperation(msg)
        # Pre-resolve so the file is attached by the time __init__ returns
        # and ``DuckDbDataStore.create``'s subsequent CREATE SCHEMA calls
        # can use the alias. The actual alias is looked up via the helper
        # cache on every operation, so we don't store it on self.
        self._resolve_alias()

    def _resolve_alias(self) -> str:
        """Return the alias under which this store's file is attached to the
        current runtime DuckDB connection. ATTACHes if needed.

        The alias is looked up rather than stored on the instance so two
        sources of drift self-heal:

        1. **Multiple stores share an attach.** DuckDB rejects a second
           ``ATTACH '<file>'`` for the same path on the same connection
           with a "Unique file handle conflict". When several stores wrap
           the same registry file (test fixtures, lifecycle quirks),
           ``attach_duckdb_file_to_runtime``'s cache returns the existing
           alias and we adopt it transparently.
        2. **Runtime backend rotation.** If the runtime backend cache is
           invalidated (e.g. via session restart), the helper's cache is
           cleared too — the next call re-ATTACHes against the new
           connection and we pick up the fresh alias.
        """
        alias = get_attached_alias(self._db_file)
        if alias is not None:
            return alias
        return attach_duckdb_file_to_runtime(
            self._db_file, alias=f"dsgrid_store_{id(self):x}"
        )

    @property
    def _runtime_alias(self) -> str:
        # Public-ish accessor for the alias; resolves on each call so the
        # store remains usable across runtime-backend rotations.
        return self._resolve_alias()

    @property
    def _runtime_backend(self) -> Any:
        # Resolved each call so a session restart that invalidates the cache
        # picks up the new backend, and the alias is re-resolved on the next
        # ``_resolve_alias`` access.
        return get_runtime_backend()

    @classmethod
    def create(cls, base_path: Path) -> Self:
        base_path.mkdir(exist_ok=True)
        db_file = base_path / DATABASE_FILENAME
        if db_file.exists():
            msg = f"Database file {db_file} already exists. Cannot initialize DuckDB data store."
            raise FileExistsError(msg)
        store = cls(base_path)
        alias = store._qualify_alias()
        store._runtime_backend.execute_sql(f"CREATE SCHEMA {alias}.{_quote_identifier(SCHEMA_DATA)}")
        store._runtime_backend.execute_sql(
            f"CREATE SCHEMA {alias}.{_quote_identifier(SCHEMA_LOOKUP_DATA)}"
        )
        store._runtime_backend.execute_sql(
            f"CREATE SCHEMA {alias}.{_quote_identifier(SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS)}"
        )
        store._runtime_backend.execute_sql(
            f"CREATE SCHEMA {alias}.{_quote_identifier(SCHEMA_MISSING_DIMENSION_ASSOCIATIONS)}"
        )
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
        self._create_table_from_dataframe(df, schema, table_name)

    def write_lookup_table(
        self, df: ibis.Table, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["lookup"]
        table_name = _make_table_short_name(dataset_id, version)
        if overwrite:
            self._drop_table(schema, table_name, if_exists=True)
        self._create_table_from_dataframe(df, schema, table_name)

    def write_expected_associations_tables(
        self, dfs: dict[str, ibis.Table], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["expected_dimension_associations"]
        base_name = _make_table_short_name(dataset_id, version)
        for tag, df in dfs.items():
            table_name = f"{base_name}__{tag}"
            if overwrite:
                self._drop_table(schema, table_name, if_exists=True)
            self._create_table_from_dataframe(df, schema, table_name)

    def write_missing_associations_tables(
        self, dfs: dict[str, ibis.Table], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        schema = TABLE_TYPE_TO_SCHEMA["missing_dimension_associations"]
        base_name = _make_table_short_name(dataset_id, version)
        for tag, df in dfs.items():
            table_name = f"{base_name}__{tag}"
            if overwrite:
                self._drop_table(schema, table_name, if_exists=True)
            self._create_table_from_dataframe(df, schema, table_name)

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

    def _qualify_alias(self) -> str:
        return _quote_identifier(self._runtime_alias)

    def _qualify_table(self, schema: str, table_name: str) -> str:
        return f"{self._qualify_alias()}.{_quote_identifier(schema)}.{_quote_identifier(table_name)}"

    def _read_table(self, schema: str, table_name: str) -> ibis.Table:
        # Tables come back bound to the runtime backend because we read
        # through the runtime connection — joins with other runtime tables
        # therefore skip the cross-backend fallback entirely.
        return self._runtime_backend.connection.table(
            table_name, database=(self._resolve_alias(), schema)
        )

    def _drop_table(self, schema: str, table_name: str, if_exists: bool = False) -> None:
        if_clause = "IF EXISTS " if if_exists else ""
        self._runtime_backend.execute_sql(
            f"DROP TABLE {if_clause}{self._qualify_table(schema, table_name)}"
        )

    def _has_table(self, schema: str, table_name: str) -> bool:
        # information_schema in the runtime connection covers attached
        # databases; filter on table_catalog to scope the query to this
        # store's attached file.
        catalog = self._runtime_alias.replace("'", "''")
        escaped_schema = schema.replace("'", "''")
        escaped_table = table_name.replace("'", "''")
        count = cast(
            Any,
            self._runtime_backend.execute_sql_to_df(
                f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_catalog = '{catalog}'
                  AND table_schema = '{escaped_schema}'
                  AND table_name = '{escaped_table}'
                """
            ).iloc[0, 0],
        )
        return count > 0

    def _replace_table(self, df: ibis.Table, schema: str, table_name: str) -> None:
        if not self._has_table(schema, table_name):
            self._create_table_from_dataframe(df, schema, table_name)
            return

        tmp_name = f"{table_name}_tmp"
        self._create_table_from_dataframe(df, schema, tmp_name)
        self._drop_table(schema, table_name)
        self._runtime_backend.execute_sql(
            f"ALTER TABLE {self._qualify_table(schema, tmp_name)} "
            f"RENAME TO {_quote_identifier(table_name)}"
        )

    def _list_table_names_like(self, schema: str, dataset_id: str, version: str) -> list[str]:
        short_name = _make_table_short_name(dataset_id, version)
        catalog = self._runtime_alias.replace("'", "''")
        escaped_schema = schema.replace("'", "''")
        escaped_short = short_name.replace("'", "''")
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{escaped_schema}'
              AND table_name LIKE '%{escaped_short}%'
        """
        return self._runtime_backend.execute_sql_to_df(query)["table_name"].to_list()

    def _list_expected_associations_table_names(self, dataset_id: str, version: str) -> list[str]:
        return self._list_table_names_like(
            SCHEMA_EXPECTED_DIMENSION_ASSOCIATIONS, dataset_id, version
        )

    def _list_missing_associations_table_names(self, dataset_id: str, version: str) -> list[str]:
        return self._list_table_names_like(
            SCHEMA_MISSING_DIMENSION_ASSOCIATIONS, dataset_id, version
        )

    def close(self) -> None:
        # Intentionally no-op. The store's file is attached to the shared
        # runtime DuckDB connection, possibly under an alias that other
        # store instances (or future ones) are using too. Issuing DETACH
        # here would break those siblings. The attach is cleaned up
        # implicitly when the runtime backend is disposed (process exit
        # or :func:`~dsgrid.ibis.backend.invalidate_runtime_backend_cache`).
        # Callers that genuinely need explicit detach can call
        # :func:`~dsgrid.ibis.backend.detach_duckdb_file_from_runtime`.
        pass

    def _create_table_from_dataframe(
        self, df: ibis.Table, schema: str, table_name: str
    ) -> None:
        """Materialize ``df`` as a new table in the attached database.

        For ibis.Table inputs that live in a *different* backend, we
        materialize via PyArrow rather than via a temp Parquet file.
        PyArrow avoids the parquet write + read round-trip on disk for
        the typical small/medium-sized payloads (dimension records,
        lookup tables, association tables) at the cost of buffering the
        whole table in driver memory. Very large cross-backend transfers
        should pre-write Parquet and use a different write path.
        """
        conn = self._runtime_backend.connection
        if isinstance(df, ibis.Table):
            obj: Any = df.to_pyarrow()
        elif isinstance(df, pd.DataFrame):
            obj = df
        else:
            msg = f"Unsupported table type: {type(df)}"
            raise TypeError(msg)
        conn.create_table(
            table_name,
            obj=obj,
            database=(self._resolve_alias(), schema),
            overwrite=False,
        )


def _make_table_short_name(dataset_id: str, version: str) -> str:
    # Replace dots so that manual SQL queries don't have to escape them.
    ver = version.replace(".", "_")
    return f"{dataset_id}__{ver}"


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'
