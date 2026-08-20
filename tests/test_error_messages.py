"""Tests for validation error messages that include file context.

Record-level context for ``convert_record_dicts_to_classes`` is tested in
``tests/test_utilities.py`` alongside that module's other tests.
"""

import pytest
from pydantic import ValidationError

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.mapping_tables import (
    MappingTableByNameModel,
    MappingTableModel,
    MappingTableRecordModel,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping, DSGInvalidParameter
from dsgrid.utils.files import dump_data


def test_from_file_reports_filename_on_validation_error(tmp_path):
    filename = tmp_path / "model.json"
    dump_data({"from_id": "a", "from_fraction": "not-a-float"}, filename)
    with pytest.raises(DSGInvalidParameter, match="model.json") as exc:
        MappingTableRecordModel.from_file(filename)
    assert isinstance(exc.value.__cause__, ValidationError)


def test_from_pre_registered_model_reports_mapping_file(tmp_path):
    records_file = tmp_path / "records.csv"
    records_file.write_text("from_id,to_id,from_fraction\na,x,1.0\nb,y,not-a-float\n")
    model = MappingTableByNameModel(file=str(records_file))
    ref = DimensionReferenceModel(
        dimension_type=DimensionType.GEOGRAPHY, dimension_id="d1", version="1.0.0"
    )
    with pytest.raises(DSGInvalidDimensionMapping, match="records.csv") as exc:
        MappingTableModel.from_pre_registered_model(model, ref, ref)
    # The record-level context from convert_record_dicts_to_classes is preserved.
    assert "record 2" in str(exc.value)
    assert isinstance(exc.value.__cause__, ValidationError)
