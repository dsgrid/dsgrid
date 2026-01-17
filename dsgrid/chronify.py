from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import chronify
from chronify.ibis.duckdb_backend import DuckDBBackend

import dsgrid
from dsgrid.common import BackendEngine


class DsgridDuckDBBackend(DuckDBBackend):
    """DuckDB backend that uses dsgrid's existing ibis connection."""

    def __init__(self, connection) -> None:
        """Create a DuckDB backend from an existing ibis connection.

        Parameters
        ----------
        connection
            Existing ibis DuckDB connection from dsgrid
        """
        # Call the parent's parent init to set the connection without creating a new one
        from chronify.ibis.base import IbisBackend

        IbisBackend.__init__(self, connection)
        self._file_path = ":memory:"


@contextmanager
def create_store(store_file: Path) -> Generator[chronify.Store, None, None]:
    """Create a chronify Store based on the dsgrid runtime configuration.

    Uses dsgrid's existing ibis connection so that ibis tables can be
    registered as views without needing to materialize data.
    """
    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        from dsgrid.ibis_api import get_spark_session

        store = chronify.Store.create_new_spark_store(session=get_spark_session())
    else:
        from dsgrid.ibis_api import get_ibis_connection

        # Use dsgrid's existing ibis connection so views can reference memtables
        backend = DsgridDuckDBBackend(get_ibis_connection())
        store = chronify.Store(backend=backend)
    try:
        yield store
    finally:
        # Don't dispose the backend since it uses dsgrid's shared connection
        pass


@contextmanager
def create_in_memory_store() -> Generator[chronify.Store, None, None]:
    """Create an in-memory chronify Store."""
    store = chronify.Store.create_in_memory_db()
    try:
        yield store
    finally:
        store.dispose()
