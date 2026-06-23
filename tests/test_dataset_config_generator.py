"""Unit tests for dsgrid.registry.dataset_config_generator helpers."""

import pytest

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.dataset_config_generator import (
    DataFileColumns,
    load_data_file_columns,
)
from dsgrid.utils.files import dump_data


# DataFileColumns model


def test_data_file_columns_defaults_are_empty():
    columns = DataFileColumns()
    assert columns.load_data == []
    assert columns.load_data_lookup == []


def test_data_file_columns_parses_both_roles():
    columns = DataFileColumns.model_validate(
        {
            "load_data": [{"name": "id", "data_type": "BIGINT"}],
            "load_data_lookup": [{"name": "id", "data_type": "BIGINT"}],
        }
    )
    assert [c.name for c in columns.load_data] == ["id"]
    assert columns.load_data[0].data_type == "BIGINT"
    assert [c.name for c in columns.load_data_lookup] == ["id"]


def test_data_file_columns_one_table_only_load_data():
    """one_table layouts populate only load_data; load_data_lookup stays empty."""
    columns = DataFileColumns.model_validate(
        {"load_data": [{"name": "geography", "data_type": "VARCHAR"}]}
    )
    assert len(columns.load_data) == 1
    assert columns.load_data_lookup == []


# load_data_file_columns loader


def test_load_data_file_columns_round_trip(tmp_path):
    payload = {
        "load_data": [{"name": "id", "data_type": "BIGINT"}],
        "load_data_lookup": [{"name": "id", "data_type": "BIGINT"}],
    }
    path = tmp_path / "schema.json5"
    dump_data(payload, path)

    columns = load_data_file_columns(path)
    assert columns.load_data[0].name == "id"
    assert columns.load_data[0].data_type == "BIGINT"
    assert columns.load_data_lookup[0].data_type == "BIGINT"


def test_load_data_file_columns_empty_document(tmp_path):
    path = tmp_path / "empty.json5"
    dump_data({}, path)
    columns = load_data_file_columns(path)
    assert columns.load_data == []
    assert columns.load_data_lookup == []


def test_load_data_file_columns_raises_on_invalid_payload(tmp_path):
    """An unknown data_type fails Column validation; the loader wraps it."""
    path = tmp_path / "bad.json5"
    dump_data({"load_data": [{"name": "id", "data_type": "BOGUS"}]}, path)
    with pytest.raises(DSGInvalidParameter, match="Failed to parse schema file"):
        load_data_file_columns(path)


def test_load_data_file_columns_raises_on_missing_file(tmp_path):
    """The loader wraps any parse error, including file-not-found."""
    with pytest.raises(DSGInvalidParameter, match="Failed to parse schema file"):
        load_data_file_columns(tmp_path / "missing.json5")
