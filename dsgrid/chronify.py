from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import chronify

import dsgrid
from dsgrid.common import BackendEngine


@contextmanager
def create_store(store_file: Path) -> Generator[chronify.Store, None, None]:
    """Create a chronify Store based on the dsgrid runtime configuration."""
    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        store = chronify.Store.create_new_hive_store(config.thrift_server_url)
    else:
        store = chronify.Store.create_file_db(store_file)
    try:
        yield store
    finally:
        store.dispose()


@contextmanager
def create_in_memory_store() -> Generator[chronify.Store, None, None]:
    """Create an in-memory chronify Store."""
    store = chronify.Store.create_in_memory_db()
    try:
        yield store
    finally:
        store.dispose()
