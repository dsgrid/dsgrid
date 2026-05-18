import logging
from pathlib import Path
from typing import Any, cast

import chronify
from chronify.ibis import IbisBackend, make_backend

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.exceptions import DSGInvalidOperation

# NOTE: ``dsgrid.ibis.session`` imports this module at module top, so importing
# session here at module top creates an unbreakable circular import. The two
# call sites below (in ``get_runtime_backend`` and ``build_independent_backend``)
# resolve ``get_spark_session`` lazily for that reason; do NOT hoist them.

logger = logging.getLogger(__name__)

_RUNTIME_BACKEND: IbisBackend | None = None
_RUNTIME_BACKEND_KEY: tuple[Any, ...] | None = None
# Tracks files attached to the runtime DuckDB connection so we don't issue
# duplicate ATTACH statements. Keyed by ``(id(connection), absolute path)``.
_ATTACHED_FILES: dict[tuple[int, str], str] = {}
# Reference count per attached file. Incremented by attach, decremented by
# detach. ``DETACH`` only fires when the count returns to zero so multiple
# callers (e.g. two DuckDbDataStore instances sharing an alias via the
# cache, or test fixtures that connect/disconnect) don't yank the file
# out from under each other.
_ATTACH_REFCOUNT: dict[tuple[int, str], int] = {}


def get_runtime_backend(**kwargs: Any) -> IbisBackend:
    """Return the cached runtime Ibis backend, creating one if needed.

    The runtime backend is a process-wide singleton. The first call with no
    kwargs (the typical case from runtime helpers) builds the default
    backend and caches it; subsequent calls return the cached instance.

    ``kwargs`` are honored only on the first call that bootstraps the cache
    — they're meant for tests or one-off bootstrap callers that need to
    influence backend construction. Once the singleton exists, ``kwargs``
    on later calls are ignored to avoid silently rotating the backend
    underneath callers (notably :class:`DuckDbDataStore`) that have already
    ATTACHed files to it.

    Callers that need a SEPARATE backend (e.g. a chronify Store backed by
    a different DuckDB file) must use :func:`build_independent_backend`,
    which does not touch the runtime cache.
    """
    global _RUNTIME_BACKEND, _RUNTIME_BACKEND_KEY

    if _RUNTIME_BACKEND is not None:
        return _RUNTIME_BACKEND

    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        session = kwargs.pop("session", None)
        if session is None:
            from dsgrid.ibis.session import get_spark_session

            session = get_spark_session()
        key = _make_backend_cache_key(config.backend_engine, {**kwargs, "session": session})
        backend = make_backend("spark", session=session, **kwargs)
    else:
        key = _make_backend_cache_key(config.backend_engine, kwargs)
        backend = make_backend("duckdb", **kwargs)

    _RUNTIME_BACKEND = backend
    _RUNTIME_BACKEND_KEY = key
    return backend


def build_independent_backend(**kwargs: Any) -> IbisBackend:
    """Build a fresh Ibis backend that is NOT cached as the runtime backend.

    Use this when a caller needs a backend bound to a specific resource
    (e.g. a chronify Store backed by a temporary DuckDB file) without
    disturbing the process-wide runtime backend that other components
    (notably :class:`DuckDbDataStore`'s ATTACH state) depend on.

    The returned backend is the caller's responsibility to dispose.
    """
    config = dsgrid.runtime_config
    if config.backend_engine == BackendEngine.SPARK:
        session = kwargs.pop("session", None)
        if session is None:
            # Lazy: session.py imports this module at module top.
            from dsgrid.ibis.session import get_spark_session

            session = get_spark_session()
        return make_backend("spark", session=session, **kwargs)
    return make_backend("duckdb", **kwargs)


def create_chronify_store(**kwargs: Any) -> chronify.Store:
    """Create a chronify Store backed by an Ibis backend.

    If ``kwargs`` are provided, the store gets its OWN backend via
    :func:`build_independent_backend` so the runtime backend (and any
    files attached to it by :class:`DuckDbDataStore`) is not displaced.
    With no ``kwargs`` the store reuses the runtime backend — the typical
    "in-memory chronify operations against the active runtime" case.
    """
    if kwargs:
        backend = build_independent_backend(**kwargs)
    else:
        backend = get_runtime_backend()
    return chronify.Store(backend=backend)


def invalidate_runtime_backend_cache() -> None:
    """Drop the cached Ibis runtime backend.

    Call this after restarting the underlying Spark session so the next
    ``get_runtime_backend()`` call (which may not receive an explicit
    ``session=`` kwarg) builds a fresh backend bound to the new session,
    instead of returning a backend still wired to the stopped session.

    Also drops the ATTACH bookkeeping in :data:`_ATTACHED_FILES` because a
    fresh backend has a fresh connection and any earlier ATTACH state on
    the old connection is gone.
    """
    global _RUNTIME_BACKEND, _RUNTIME_BACKEND_KEY
    _RUNTIME_BACKEND = None
    _RUNTIME_BACKEND_KEY = None
    _ATTACHED_FILES.clear()
    _ATTACH_REFCOUNT.clear()


def attach_duckdb_file_to_runtime(
    file_path: Path | str, alias: str, *, read_only: bool = False
) -> str:
    """ATTACH ``file_path`` to the runtime DuckDB backend.

    Tables in the attached file become addressable from the runtime
    connection via ``runtime.connection.table(name, database=(alias,
    schema))`` or ``"{alias}"."{schema}"."{table}"`` in raw SQL. This is
    the canonical way for an on-disk DuckDB store to expose its tables to
    the runtime backend without opening a second independent connection.

    Idempotent: a second call with the same resolved ``file_path`` returns
    the alias that was used the first time, regardless of the ``alias``
    argument on the second call.

    Reference-counted with :func:`detach_duckdb_file_from_runtime`: each
    call increments a per-(connection, file) refcount; ``detach`` only
    runs the SQL ``DETACH`` when the count returns to zero. This lets
    multiple callers (sibling DuckDbDataStore instances on the same file,
    test fixtures that load-then-connect, etc.) share an attach without
    one of them yanking it out from under the others.

    Parameters
    ----------
    file_path : Path or str
        Path to a DuckDB database file. Created if it does not exist
        (matches the DuckDB ``ATTACH`` default), unless ``read_only=True``.
    alias : str
        The alias to use in the ATTACH statement on first attach. Must be
        unique per file across the process.
    read_only : bool, optional
        If True, attach in read-only mode. Defaults to False because the
        established usage pattern (DuckDbDataStore) needs to both read and
        write through the runtime connection.

    Returns
    -------
    str
        The alias under which the file is attached.

    Raises
    ------
    DSGInvalidOperation
        If the runtime backend is not DuckDB (the ATTACH is a DuckDB
        feature and is meaningless for the Spark runtime; callers must
        gate on backend engine before calling this helper).
    """
    if dsgrid.runtime_config.backend_engine != BackendEngine.DUCKDB:
        msg = (
            "attach_duckdb_file_to_runtime is only valid when the runtime "
            "backend is DuckDB."
        )
        raise DSGInvalidOperation(msg)

    runtime = get_runtime_backend()
    # cast: chronify's IbisBackend.connection is typed as ibis.BaseBackend,
    # but the runtime backend is always DuckDB here and we need the raw_sql
    # method that ibis.backends.duckdb.Backend exposes.
    conn = cast(Any, runtime.connection)
    abs_path = str(Path(file_path).resolve())
    key = (id(conn), abs_path)
    cached_alias = _ATTACHED_FILES.get(key)
    if cached_alias is not None:
        _ATTACH_REFCOUNT[key] = _ATTACH_REFCOUNT.get(key, 0) + 1
        return cached_alias

    escaped_path = abs_path.replace("'", "''")
    quoted_alias = _quote_duckdb_identifier(alias)
    mode = " (READ_ONLY)" if read_only else ""
    conn.raw_sql(f"ATTACH '{escaped_path}' AS {quoted_alias}{mode}")
    _ATTACHED_FILES[key] = alias
    _ATTACH_REFCOUNT[key] = 1
    return alias


def get_attached_alias(file_path: Path | str) -> str | None:
    """Return the alias under which ``file_path`` is currently attached to
    the runtime DuckDB connection, or ``None`` if not attached.

    Cheaper than re-issuing ATTACH and useful for callers that need to
    know the alias without modifying state (the SQL string-builders in
    :class:`~dsgrid.registry.duckdb_data_store.DuckDbDataStore`, for
    instance).
    """
    if dsgrid.runtime_config.backend_engine != BackendEngine.DUCKDB:
        return None
    if _RUNTIME_BACKEND is None:
        return None
    abs_path = str(Path(file_path).resolve())
    key = (id(_RUNTIME_BACKEND.connection), abs_path)
    return _ATTACHED_FILES.get(key)


def detach_duckdb_file_from_runtime(file_path: Path | str) -> None:
    """Drop a reference to ``file_path``'s ATTACH on the runtime backend.

    Decrements the per-(connection, file) refcount maintained by
    :func:`attach_duckdb_file_to_runtime`. Only when the count returns
    to zero does the actual SQL ``DETACH`` run; earlier calls just
    decrement so sibling holders of the same attach keep working.

    No-op when the file was never attached, the runtime is not DuckDB,
    or the runtime backend has already been invalidated/replaced.
    """
    if dsgrid.runtime_config.backend_engine != BackendEngine.DUCKDB:
        return
    if _RUNTIME_BACKEND is None:
        return
    # cast: see attach_duckdb_file_to_runtime above.
    conn = cast(Any, _RUNTIME_BACKEND.connection)
    abs_path = str(Path(file_path).resolve())
    key = (id(conn), abs_path)
    alias = _ATTACHED_FILES.get(key)
    if alias is None:
        return

    refcount = _ATTACH_REFCOUNT.get(key, 0) - 1
    if refcount > 0:
        _ATTACH_REFCOUNT[key] = refcount
        return

    # Last reference — release everything.
    _ATTACHED_FILES.pop(key, None)
    _ATTACH_REFCOUNT.pop(key, None)
    quoted_alias = _quote_duckdb_identifier(alias)
    try:
        conn.raw_sql(f"DETACH {quoted_alias}")
    except Exception as exc:
        # DETACH can fail if the alias is already gone (e.g. the runtime
        # connection was reset out from under us). Surface for diagnostics
        # but don't propagate — close() callers are typically in cleanup
        # paths where raising would mask the original error.
        logger.debug("DETACH of %s (%s) failed: %s", abs_path, alias, exc)


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


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
