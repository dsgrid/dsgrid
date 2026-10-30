"""Tests for dsgrid.config.dimensions module."""

import logging

import pytest

from dsgrid.config.dimensions import (
    AnnualRangeModel,
    AnnualTimeDimensionModel,
    DateTimeDimensionModel,
)
from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.dimension.time import MeasurementType


def _datetime_model_dict(column_format: dict) -> dict:
    return {
        "name": "time",
        "type": "time",
        "module": "dsgrid.dimension.standard",
        "class_name": "Time",
        "column_format": column_format,
        "time_zone_format": {
            "format_type": "aligned_in_absolute_time",
            "time_zone": "Etc/GMT+7",
        },
        "measurement_type": "total",
        "ranges": [
            {
                "start": "2018-01-01 00:00:00",
                "end": "2018-01-01 01:00:00",
                "frequency": "P0DT1H",
            }
        ],
        "time_interval_type": "period_beginning",
    }


@pytest.mark.parametrize(
    "column_format, expected_dtype",
    [
        ({"dtype": "TIMESTAMP_TZ"}, "timestamp_tz"),
        ({"dtype": "TIMESTAMP_NTZ"}, "timestamp_ntz"),
        (
            {
                "dtype": "TIME_FORMAT_IN_PARTS",
                "year_column": "year",
                "month_column": "month",
                "day_column": "day",
            },
            "time_format_in_parts",
        ),
    ],
)
def test_column_format_dtype_is_case_insensitive(column_format, expected_dtype, caplog):
    with caplog.at_level(logging.WARNING):
        model = DateTimeDimensionModel.model_validate(_datetime_model_dict(column_format))
    assert model.column_format.dtype == expected_dtype
    assert any("Renaming legacy dtype" in x.message for x in caplog.records)


def test_column_format_lowercase_dtype_no_warning(caplog):
    with caplog.at_level(logging.WARNING):
        model = DateTimeDimensionModel.model_validate(
            _datetime_model_dict({"dtype": "timestamp_tz"})
        )
    assert model.column_format.dtype == "timestamp_tz"
    assert not any("Renaming legacy dtype" in x.message for x in caplog.records)


def test_dimension_union_accepts_model_instances():
    # Pydantic's smart union feeds already-constructed sibling instances into other
    # members' before-validators; the non-dict guards must pass them through.
    datetime_model = DateTimeDimensionModel.model_validate(
        _datetime_model_dict({"dtype": "timestamp_tz"})
    )
    annual_model = AnnualTimeDimensionModel(
        name="annual",
        dimension_type="time",
        module="dsgrid.dimension.standard",
        class_name="AnnualTime",
        ranges=[AnnualRangeModel(start="2018", end="2020")],
        measurement_type=MeasurementType.TOTAL,
    )
    config = DimensionsConfigModel(dimensions=[datetime_model, annual_model])
    assert len(config.dimensions) == 2
