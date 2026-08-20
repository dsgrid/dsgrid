"""Tests for validation error messages that include file and row context."""

import pytest

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.mapping_tables import MappingTableByNameModel, MappingTableModel
from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping, DSGInvalidParameter
from dsgrid.utils.files import dump_data
from dsgrid.utils.utilities import convert_record_dicts_to_classes


class _RecordModel(DSGBaseModel):
    id: str
    value: float


def test_from_file_reports_filename_on_validation_error(tmp_path):
    filename = tmp_path / "model.json"
    dump_data({"id": "x", "value": "not-a-float"}, filename)
    with pytest.raises(DSGInvalidParameter, match="model.json"):
        _RecordModel.from_file(filename)


def test_convert_record_dicts_to_classes_reports_row_number():
    rows = [
        {"id": "a", "value": 1.0},
        {"id": "b", "value": "not-a-float"},
    ]
    with pytest.raises(ValueError, match="row 2"):
        convert_record_dicts_to_classes(rows, _RecordModel)


def test_from_pre_registered_model_reports_mapping_file(tmp_path):
    records_file = tmp_path / "records.csv"
    records_file.write_text("from_id,to_id,from_fraction\na,x,1.0\nb,y,not-a-float\n")
    model = MappingTableByNameModel(file=str(records_file))
    ref = DimensionReferenceModel(
        dimension_type=DimensionType.GEOGRAPHY, dimension_id="d1", version="1.0.0"
    )
    with pytest.raises(DSGInvalidDimensionMapping, match="records.csv"):
        MappingTableModel.from_pre_registered_model(model, ref, ref)
