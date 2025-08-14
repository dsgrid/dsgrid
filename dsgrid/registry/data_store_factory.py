from pathlib import Path

from dsgrid.registry.common import DataStoreType
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.duckdb_data_store import DuckDbDataStore
from dsgrid.registry.filesystem_data_store import FilesystemDataStore


def make_data_store(
    base_path: Path, data_store_type: DataStoreType, initialize: bool = False
) -> DataStoreInterface:
    """Factory function to create a data store.

    Parameters
    ----------
    base_path : Path
        The base path for the data store.
    data_store_type : DataStoreType
        The type of data store to create.
    initialize : bool
        Whether to initialize the data store.
    """
    match data_store_type:
        case DataStoreType.FILESYSTEM:
            cls = FilesystemDataStore
        case DataStoreType.DUCKDB:
            cls = DuckDbDataStore
        case _:
            msg = f"Unsupported data store type: {data_store_type}"
            raise NotImplementedError(msg)

    if initialize:
        return cls.create(base_path)
    return cls.load(base_path)
