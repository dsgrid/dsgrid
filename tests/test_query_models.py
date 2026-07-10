"""Tests for the open-ended ``FunctionReference`` set in dsgrid.query.models."""

import logging

import pytest

from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    DimensionNamesModel,
    FunctionReference,
)


def _aggregation_model(name: str) -> AggregationModel:
    return AggregationModel(
        aggregation_function=name,
        dimensions=DimensionNamesModel(
            geography=[],
            metric=["end_use"],
            model_year=[],
            scenario=[],
            sector=[],
            subsector=[],
            time=[],
            weather_year=[],
        ),
    )


def test_function_reference_accepts_arbitrary_name():
    """FunctionReference no longer rejects names outside a fixed allowlist."""
    ref = FunctionReference("median")
    assert ref.name == "median"
    assert ref.__name__ == "median"


@pytest.mark.parametrize(
    "name",
    ["sum", "mean", "max", "min", "count", "avg", "stddev", "first", "last", "median"],
)
def test_aggregation_model_accepts_common_aggregations(name: str):
    """Aggregations that DuckDB/Spark know about should pass model validation."""
    model = _aggregation_model(name)
    assert model.aggregation_function.name == name


@pytest.mark.parametrize("name", ["hour", "year", "day", "month", "quarter"])
def test_column_model_accepts_scalar_extractions(name: str):
    """Scalar functions (date extractions) on ColumnModel are also unrestricted."""
    col = ColumnModel(dimension_name="timestamp", function=name)
    assert col.function.name == name
    # Auto-generated alias preserves the function name.
    assert col.alias == f"{name}__timestamp"


def test_aggregation_model_still_rejects_none():
    """None remains explicitly disallowed."""
    with pytest.raises(ValueError, match="aggregation_function cannot be None"):
        AggregationModel(
            aggregation_function=None,
            dimensions=DimensionNamesModel(
                geography=[],
                metric=["end_use"],
                model_year=[],
                scenario=[],
                sector=[],
                subsector=[],
                time=[],
                weather_year=[],
            ),
        )


def test_unregistered_aggregation_warns(caplog):
    """Unregistered names are still accepted but flagged for discoverability."""
    with caplog.at_level(logging.WARNING, logger="dsgrid.query.models"):
        model = _aggregation_model("stddev")
    assert model.aggregation_function.name == "stddev"
    messages = [r.message for r in caplog.records if "stddev" in r.message]
    assert messages, "expected a warning naming the unregistered aggregation"
    assert "mean" in messages[0]  # the warning lists the registered names


def test_registered_aggregation_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="dsgrid.query.models"):
        _aggregation_model("mean")
    assert not caplog.records


def test_column_model_scalar_function_does_not_warn(caplog):
    """ColumnModel functions are scalar extractions, not aggregations; names
    outside the aggregation registry are the norm there and must not warn."""
    with caplog.at_level(logging.WARNING, logger="dsgrid.query.models"):
        ColumnModel(dimension_name="timestamp", function="hour")
    assert not caplog.records


def test_aggregation_model_serialization_round_trip():
    """Serializing the model emits the bare function name (string), not the object."""
    model = _aggregation_model("count")
    dumped = model.model_dump()
    assert dumped["aggregation_function"] == "count"
    # Round-trip back through validation.
    re_parsed = AggregationModel(**dumped)
    assert re_parsed.aggregation_function.name == "count"
