"""Tests for the open-ended ``FunctionReference`` set in dsgrid.query.models.

Phase 10 of the Ibis migration cleanup dropped the 5-element
``SUPPORTED_QUERY_FUNCTIONS`` allowlist and lets callers pass any SQL
function name. Validation moves from parse-time to backend execution
time. These tests pin the new contract.
"""

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    DimensionNamesModel,
    FunctionReference,
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
    model = AggregationModel(
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


def test_aggregation_model_serialization_round_trip():
    """Serializing the model emits the bare function name (string), not the object."""
    model = AggregationModel(
        aggregation_function="count",
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
    dumped = model.model_dump()
    assert dumped["aggregation_function"] == "count"
    # Round-trip back through validation.
    re_parsed = AggregationModel(**dumped)
    assert re_parsed.aggregation_function.name == "count"
