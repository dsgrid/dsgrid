"""Tests for DuckDbDataStore."""

import pytest
import pandas as pd

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
