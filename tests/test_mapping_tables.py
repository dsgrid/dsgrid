"""Tests for dsgrid.config.mapping_tables module."""

import pytest
from pydantic import ValidationError

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.mapping_tables import MappingTableByNameModel, MappingTableModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping


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
