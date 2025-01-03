from pathlib import Path

import chronify

import dsgrid
from dsgrid.common import BackendEngine


def create_store(store_file: Path) -> chronify.Store:
    """Create a chronify Store based on the dsgrid runtime configuration."""
    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        store = chronify.Store.create_new_hive_store(config.thrift_server_url)
    else:
        store = chronify.Store.create_file_db(store_file)
    return store
