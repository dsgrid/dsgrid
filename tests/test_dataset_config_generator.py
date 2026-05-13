"""Unit tests for dsgrid.registry.dataset_config_generator helpers."""

import pytest

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.dataset_config_generator import (
    GenerateConfigSchemas,
    load_generate_config_schemas,
)
from dsgrid.utils.files import dump_data


# GenerateConfigSchemas model


def test_generate_config_schemas_defaults_are_empty():
    schemas = GenerateConfigSchemas()
    assert schemas.load_data == []
    assert schemas.load_data_lookup == []


def test_generate_config_schemas_parses_both_roles():
    schemas = GenerateConfigSchemas.model_validate(
        {
            "load_data": [{"name": "id", "data_type": "BIGINT"}],
            "load_data_lookup": [{"name": "id", "data_type": "BIGINT"}],
        }
    )
    assert [c.name for c in schemas.load_data] == ["id"]
    assert schemas.load_data[0].data_type == "BIGINT"
    assert [c.name for c in schemas.load_data_lookup] == ["id"]


def test_generate_config_schemas_one_table_only_load_data():
    """one_table layouts populate only load_data; load_data_lookup stays empty."""
    schemas = GenerateConfigSchemas.model_validate(
        {"load_data": [{"name": "geography", "data_type": "VARCHAR"}]}
    )
    assert len(schemas.load_data) == 1
    assert schemas.load_data_lookup == []


# load_generate_config_schemas loader


def test_load_generate_config_schemas_round_trip(tmp_path):
    payload = {
        "load_data": [{"name": "id", "data_type": "BIGINT"}],
        "load_data_lookup": [{"name": "id", "data_type": "BIGINT"}],
    }
    path = tmp_path / "schema.json5"
    dump_data(payload, path)

    schemas = load_generate_config_schemas(path)
    assert schemas.load_data[0].name == "id"
    assert schemas.load_data[0].data_type == "BIGINT"
    assert schemas.load_data_lookup[0].data_type == "BIGINT"


def test_load_generate_config_schemas_empty_document(tmp_path):
    path = tmp_path / "empty.json5"
    dump_data({}, path)
    schemas = load_generate_config_schemas(path)
    assert schemas.load_data == []
    assert schemas.load_data_lookup == []


def test_load_generate_config_schemas_raises_on_invalid_payload(tmp_path):
    """An unknown data_type fails Column validation; the loader wraps it."""
    path = tmp_path / "bad.json5"
    dump_data({"load_data": [{"name": "id", "data_type": "BOGUS"}]}, path)
    with pytest.raises(DSGInvalidParameter, match="Failed to parse schema file"):
        load_generate_config_schemas(path)


def test_load_generate_config_schemas_raises_on_missing_file(tmp_path):
    """The loader wraps any parse error, including file-not-found."""
    with pytest.raises(DSGInvalidParameter, match="Failed to parse schema file"):
        load_generate_config_schemas(tmp_path / "missing.json5")
