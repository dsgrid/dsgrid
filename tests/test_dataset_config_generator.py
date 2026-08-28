"""Unit tests for dsgrid.registry.dataset_config_generator helpers."""

import json

import ibis
import pytest

from dsgrid.config.dataset_config import _read_and_apply_types
from dsgrid.config.file_schema import Column
from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter
from dsgrid.ibis.io import write_dataframe
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


# _read_and_apply_types per-format contract


def test_read_and_apply_types_json_casts_declared_types(tmp_path):
    """JSON declarations are authoritative in generate-config, matching
    registration's read_data_file contract."""
    json_file = tmp_path / "load_data.json"
    json_file.write_text(json.dumps({"model_year": 2030, "value": 1.5}))

    columns = [Column(name="model_year", data_type="STRING")]
    df = _read_and_apply_types(json_file, columns)
    assert str(df.schema()["model_year"]) == "string"


def test_read_and_apply_types_parquet_validates_without_casting(tmp_path):
    """Parquet declarations are validated but never cast, so generated
    dimension records reflect the file's own schema."""
    parquet_file = tmp_path / "load_data.parquet"
    write_dataframe(ibis.memtable({"geography": ["06037"], "value": [1.5]}), parquet_file)

    columns = [Column(name="geography", data_type="STRING")]
    df = _read_and_apply_types(parquet_file, columns)
    assert str(df.schema()["geography"]) == "string"


def test_read_and_apply_types_parquet_mismatched_declaration_raises(tmp_path):
    """A declaration a Parquet file disagrees with fails here rather than
    generating records that registration would reject."""
    parquet_file = tmp_path / "load_data.parquet"
    write_dataframe(ibis.memtable({"geography": [6037], "value": [1.5]}), parquet_file)

    columns = [Column(name="geography", data_type="STRING")]
    with pytest.raises(DSGInvalidField, match="conflicts with the Parquet"):
        _read_and_apply_types(parquet_file, columns)
