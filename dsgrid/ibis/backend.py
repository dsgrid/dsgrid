from pathlib import Path
from typing import Any

import chronify
import ibis
import pandas as pd
from chronify.ibis import IbisBackend, make_backend

import dsgrid
from dsgrid.common import BackendEngine

_RUNTIME_BACKEND: IbisBackend | None = None
_RUNTIME_BACKEND_KEY: tuple[Any, ...] | None = None


def make_runtime_backend(**kwargs: Any) -> IbisBackend:
    """Create an Ibis backend from the dsgrid runtime configuration."""
    global _RUNTIME_BACKEND, _RUNTIME_BACKEND_KEY

    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        session = kwargs.pop("session", None)
        if session is None:
            from dsgrid.ibis.session import get_spark_session

            session = get_spark_session()
        key = _make_backend_cache_key(config.backend_engine, {**kwargs, "session": session})
        if _RUNTIME_BACKEND is not None and _RUNTIME_BACKEND_KEY == key:
            return _RUNTIME_BACKEND
        backend = make_backend("spark", session=session, **kwargs)
    else:
        key = _make_backend_cache_key(config.backend_engine, kwargs)
        if _RUNTIME_BACKEND is not None and _RUNTIME_BACKEND_KEY == key:
            return _RUNTIME_BACKEND
        backend = make_backend("duckdb", **kwargs)

    _RUNTIME_BACKEND = backend
    _RUNTIME_BACKEND_KEY = key
    return backend


def create_chronify_store(**kwargs: Any) -> chronify.Store:
    """Create a chronify Store backed by the configured Ibis backend."""
    return chronify.Store(backend=make_runtime_backend(**kwargs))


def read_csv_expr(path: Path | str, schema: dict[str, str] | None = None) -> ibis.Table:
    """Read CSV data into an Ibis table expression."""
    path_str = Path(path).as_posix()
    if dsgrid.runtime_config.backend_engine == BackendEngine.SPARK:
        from dsgrid.ibis.session import get_runtime_session

        kwargs: dict[str, Any] = {"header": True}
        if schema:
            kwargs["schema"] = schema
        return get_runtime_session().read.csv(path_str, **kwargs)
    if schema:
        return make_runtime_backend().connection.read_csv(path_str, header=True, types=schema)
    return make_runtime_backend().connection.read_csv(path_str, header=True, all_varchar=True)


def read_parquet_expr(path: Path | str) -> ibis.Table:
    """Read Parquet data into an Ibis table expression."""
    path_str = Path(path).as_posix()
    return make_runtime_backend().connection.read_parquet(path_str)


def read_json_expr(path: Path | str) -> ibis.Table:
    """Read JSON data into an Ibis table expression."""
    path = Path(path)
    return make_runtime_backend().connection.read_json(path.as_posix())


def read_csv_pandas(path: Path | str, schema: dict[str, str] | None = None) -> pd.DataFrame:
    """Read CSV data through Ibis and return a pandas DataFrame."""
    return read_csv_expr(path, schema=schema).execute()


def _make_backend_cache_key(
    backend_engine: BackendEngine, kwargs: dict[str, Any]
) -> tuple[Any, ...]:
    items = tuple(sorted((key, _cacheable_value(value)) for key, value in kwargs.items()))
    return (backend_engine, items)


def _cacheable_value(value: Any) -> Any:
    try:
        hash(value)
    except TypeError:
        return id(value)
    return value
