from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import chronify

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.ibis.backend import create_chronify_store


@contextmanager
def create_store(store_file: Path) -> Generator[chronify.Store, None, None]:
    """Create a chronify Store based on the dsgrid runtime configuration."""
    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        store = create_chronify_store()
    else:
        store = create_chronify_store(database=str(store_file))
    yield store


@contextmanager
def create_in_memory_store() -> Generator[chronify.Store, None, None]:
    """Create an in-memory chronify Store."""
    store = create_chronify_store()
    yield store
