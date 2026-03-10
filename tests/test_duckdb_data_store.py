"""Tests for DuckDbDataStore."""

import pytest

from dsgrid.registry.duckdb_data_store import DuckDbDataStore
from dsgrid.utils.spark import create_dataframe_from_dicts


def _make_df():
    return create_dataframe_from_dicts([{"id": "a", "value": 1}])


def test_remove_tables(tmp_path):
    store = DuckDbDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_table(df, "ds1", "1.0.0")
    store.write_lookup_table(df, "ds1", "1.0.0")
    store.write_expected_associations_tables({"geo": df}, "ds1", "1.0.0")
    store.write_missing_associations_tables({"geo": df}, "ds1", "1.0.0")

    # Verify tables exist by reading them.
    assert store.read_table("ds1", "1.0.0").count() == 1
    assert store.read_lookup_table("ds1", "1.0.0").count() == 1
    assert len(store.read_expected_associations_tables("ds1", "1.0.0")) == 1
    assert len(store.read_missing_associations_tables("ds1", "1.0.0")) == 1

    store.remove_tables("ds1", "1.0.0")

    # After removal, reads should fail or return empty.
    with pytest.raises(Exception):
        store.read_table("ds1", "1.0.0")
    with pytest.raises(Exception):
        store.read_lookup_table("ds1", "1.0.0")
    assert store.read_expected_associations_tables("ds1", "1.0.0") == {}
    assert store.read_missing_associations_tables("ds1", "1.0.0") == {}
