from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import chronify

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.utils.run_command import check_run_command


@contextmanager
def create_store(store_file: Path) -> Generator[chronify.Store, None, None]:
    """Create a chronify Store based on the dsgrid runtime configuration."""
    config = dsgrid.runtime_config
    try:
        if config.chronify_backend_engine == BackendEngine.SPARK:
            check_run_command(config.get_thrift_server_start_command())
            store = chronify.Store.create_new_hive_store(config.thrift_server_url)
        else:
            store = chronify.Store.create_file_db(store_file)
        yield store
    finally:
        if config.chronify_backend_engine == BackendEngine.SPARK:
            check_run_command(config.get_thrift_server_stop_command())
