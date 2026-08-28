import pytest

from dsgrid.ibis import backend as backend_mod
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.session import init_runtime_session
from dsgrid.ibis.types import use_duckdb


def test_stop_invalidates_backend_cache() -> None:
    """_SparkRuntimeSession.stop() must clear the cached Ibis backend so a
    subsequent get_runtime_backend() can't hand out a reference bound to a
    stopped SparkSession. DuckDB has no stop semantic; skip there."""
    if use_duckdb():
        pytest.skip("Only the Spark stop() path invalidates the cache")

    session = init_runtime_session("dsgrid_cache_test")
    # Prime the cache.
    get_runtime_backend()
    assert backend_mod._RUNTIME_BACKEND is not None
    try:
        session.stop()
        assert backend_mod._RUNTIME_BACKEND is None, (
            "_SparkRuntimeSession.stop() must call invalidate_runtime_backend_cache "
            "to prevent the next get_runtime_backend() from returning a stopped "
            "session reference."
        )
    finally:
        # Leave a fresh session for downstream tests in the same module.
        init_runtime_session("dsgrid_cache_test_reset")
