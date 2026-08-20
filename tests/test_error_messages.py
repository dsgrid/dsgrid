"""Tests for validation error messages that include file and record context."""

import pytest

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.mapping_tables import (
    MappingTableByNameModel,
    MappingTableModel,
    MappingTableRecordModel,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping, DSGInvalidParameter
from dsgrid.utils.files import dump_data
from dsgrid.utils.utilities import convert_record_dicts_to_classes


def test_from_file_reports_filename_on_validation_error(tmp_path):
    filename = tmp_path / "model.json"
    dump_data({"from_id": "a", "from_fraction": "not-a-float"}, filename)
    with pytest.raises(DSGInvalidParameter, match="model.json"):
        MappingTableRecordModel.from_file(filename)


def test_convert_record_dicts_to_classes_reports_record_number():
    rows = [
        {"from_id": "a", "from_fraction": 1.0},
        {"from_id": "b", "from_fraction": "not-a-float"},
    ]
    with pytest.raises(ValueError, match="record 2"):
        convert_record_dicts_to_classes(rows, MappingTableRecordModel)


def test_from_pre_registered_model_reports_mapping_file(tmp_path):
    records_file = tmp_path / "records.csv"
    records_file.write_text("from_id,to_id,from_fraction\na,x,1.0\nb,y,not-a-float\n")
    model = MappingTableByNameModel(file=str(records_file))
    ref = DimensionReferenceModel(
        dimension_type=DimensionType.GEOGRAPHY, dimension_id="d1", version="1.0.0"
    )
    with pytest.raises(DSGInvalidDimensionMapping, match="records.csv"):
        MappingTableModel.from_pre_registered_model(model, ref, ref)
