"""Tests for DuckDbDataStore."""

import pytest
import pandas as pd

from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.operations import join_multiple_columns
from dsgrid.ibis.types import use_duckdb
from dsgrid.registry.duckdb_data_store import DuckDbDataStore

pytestmark = pytest.mark.skipif(not use_duckdb(), reason="DuckDbDataStore requires DuckDB backend")


def _make_df():
    return pd.DataFrame([{"id": "a", "value": 1}])


def _count_rows(table):
    return table.count().execute()


def test_remove_tables(tmp_path):
    store = DuckDbDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_table(df, "ds1", "1.0.0")
    store.write_lookup_table(df, "ds1", "1.0.0")
    store.write_expected_associations_tables({"geo": df}, "ds1", "1.0.0")
    store.write_missing_associations_tables({"geo": df}, "ds1", "1.0.0")

    # Verify tables exist by reading them.
    assert _count_rows(store.read_table("ds1", "1.0.0")) == 1
    assert _count_rows(store.read_lookup_table("ds1", "1.0.0")) == 1
    assert len(store.read_expected_associations_tables("ds1", "1.0.0")) == 1
    assert len(store.read_missing_associations_tables("ds1", "1.0.0")) == 1

    store.remove_tables("ds1", "1.0.0")

    # After removal, reads should fail or return empty.
    with pytest.raises(Exception):
        store.read_table("ds1", "1.0.0").execute()
    with pytest.raises(Exception):
        store.read_lookup_table("ds1", "1.0.0").execute()
    assert store.read_expected_associations_tables("ds1", "1.0.0") == {}
    assert store.read_missing_associations_tables("ds1", "1.0.0") == {}


def test_read_table_returns_runtime_bound_table(tmp_path):
    """The store ATTACHes its DuckDB file to the runtime backend on init
    and returns runtime-bound Ibis tables from read_table. That keeps
    downstream joins on the runtime backend so they don't trip the
    create_temp_view cross-backend fallback (which in the worst case
    materializes the table to a tmp Parquet).

    We verify both that the ATTACH happened (``_runtime_alias`` is set)
    and that the returned table is bound to the runtime backend.
    """
    store = DuckDbDataStore.create(tmp_path / "store")
    store.write_table(_make_df(), "ds1", "1.0.0")
    assert store._runtime_alias is not None, (
        "DuckDbDataStore.__init__ should ATTACH its file to the runtime "
        "DuckDB connection so read_table returns runtime-bound tables."
    )

    store_table = store.read_table("ds1", "1.0.0")
    runtime = get_runtime_backend()
    # chronify's IbisBackend wraps an ibis.backends.duckdb.Backend in
    # ``.connection``. The store table reports that inner backend via
    # ``_find_backend``; they must be the *same* object so joins stay
    # inside one Ibis backend and skip the create_temp_view fallback.
    assert store_table._find_backend(use_default=False) is runtime.connection, (
        "read_table must return an ibis.Table whose backend is the runtime "
        "Ibis backend (chronify's IbisBackend.connection); otherwise joins "
        "against runtime tables fall back to create_temp_view."
    )

    # Join with a freshly-created runtime table — must NOT trip the
    # _ensure_same_backend warning path. We verify by running a join and
    # asserting the result is correct.
    runtime_table = runtime.connection.create_table(
        "runtime_side", pd.DataFrame([{"id": "a", "extra": "ok"}]), temp=True
    )
    joined = join_multiple_columns(store_table, runtime_table, ["id"])
    assert joined.execute().to_dict("records") == [
        {"id": "a", "value": 1, "extra": "ok"}
    ]
