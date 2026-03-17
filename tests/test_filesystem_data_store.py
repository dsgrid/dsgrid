"""Tests for FilesystemDataStore."""

import pytest

from dsgrid.registry.filesystem_data_store import FilesystemDataStore
from dsgrid.utils.spark import create_dataframe_from_dicts


def _make_df():
    return create_dataframe_from_dicts([{"id": "a", "value": 1}])


def test_write_table_raises_if_exists(tmp_path):
    store = FilesystemDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_table(df, "ds1", "1.0.0")
    with pytest.raises(FileExistsError, match="Table already exists"):
        store.write_table(df, "ds1", "1.0.0")


def test_write_expected_associations_tables_raises_if_exists(tmp_path):
    store = FilesystemDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_expected_associations_tables({"geo": df}, "ds1", "1.0.0")
    with pytest.raises(FileExistsError, match="Expected associations table already exists"):
        store.write_expected_associations_tables({"geo": df}, "ds1", "1.0.0")


def test_write_missing_associations_tables_raises_if_exists(tmp_path):
    store = FilesystemDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_missing_associations_tables({"geo": df}, "ds1", "1.0.0")
    with pytest.raises(FileExistsError, match="Missing associations table already exists"):
        store.write_missing_associations_tables({"geo": df}, "ds1", "1.0.0")


def test_remove_tables(tmp_path):
    store = FilesystemDataStore.create(tmp_path / "store")
    df = _make_df()
    store.write_table(df, "ds1", "1.0.0")
    store.write_lookup_table(df, "ds1", "1.0.0")
    store.write_expected_associations_tables({"geo": df}, "ds1", "1.0.0")
    store.write_missing_associations_tables({"geo": df}, "ds1", "1.0.0")

    base_dir = store._base_dir("ds1", "1.0.0")
    assert base_dir.exists()

    store.remove_tables("ds1", "1.0.0")
    assert not base_dir.exists()
