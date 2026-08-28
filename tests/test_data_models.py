"""Tests for dsgrid.data_models module."""

import pytest
from pydantic import ValidationError

from dsgrid.config.mapping_tables import MappingTableRecordModel
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.files import dump_data


def test_from_file_reports_filename_on_validation_error(tmp_path):
    filename = tmp_path / "model.json"
    dump_data({"from_id": "a", "from_fraction": "not-a-float"}, filename)
    with pytest.raises(DSGInvalidParameter, match="model.json") as exc:
        MappingTableRecordModel.from_file(filename)
    assert isinstance(exc.value.__cause__, ValidationError)
