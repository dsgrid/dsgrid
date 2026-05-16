"""Direct tests for :mod:`dsgrid.dataset.growth_rates`.

Coverage prior to Phase 9 was integration-only via test_derived_datasets,
which doesn't exercise the error paths or the small-table math directly.
These tests build minimal Ibis tables and check the value math + the
``_check_model_years`` DSGInvalidQuery branch.
"""

import math

import pytest

from dsgrid.dataset.growth_rates import (
    _check_model_years,
    _process_exponential_growth_rate,
    apply_annual_multiplier,
    apply_exponential_growth_rate,
)
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.ibis.session import create_dataframe_from_dicts
from dsgrid.query.models import ProjectionDatasetModel

from tests._helpers import collect as _collect, order_by as _order_by


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def projection_dataset_no_base_year():
    return ProjectionDatasetModel(
        dataset_id="result",
        initial_value_dataset_id="initial",
        growth_rate_dataset_id="growth",
        base_year=None,
    )


@pytest.fixture
def projection_dataset_2020():
    return ProjectionDatasetModel(
        dataset_id="result",
        initial_value_dataset_id="initial",
        growth_rate_dataset_id="growth",
        base_year=2020,
    )


# --- _check_model_years -----------------------------------------------------


def test_check_model_years_base_year_none_uses_earliest():
    """When base_year is None, the function picks the earliest model_year."""
    dataset = ProjectionDatasetModel(
        dataset_id="result",
        initial_value_dataset_id="initial",
        growth_rate_dataset_id="growth",
        base_year=None,
    )
    initial = create_dataframe_from_dicts(
        [{"geo": "CA", "model_year": "2020", "value": 100.0}]
    )
    growth = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 0.0},
            {"geo": "CA", "model_year": "2021", "value": 0.05},
        ]
    )
    _, base_year = _check_model_years(dataset, initial, growth, "model_year")
    assert base_year == 2020


def test_check_model_years_explicit_base_year_present():
    dataset = ProjectionDatasetModel(
        dataset_id="result",
        initial_value_dataset_id="initial",
        growth_rate_dataset_id="growth",
        base_year=2020,
    )
    initial = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 100.0},
            {"geo": "CA", "model_year": "2019", "value": 90.0},
        ]
    )
    growth = create_dataframe_from_dicts(
        [{"geo": "CA", "model_year": "2020", "value": 0.0}]
    )
    _, base_year = _check_model_years(dataset, initial, growth, "model_year")
    assert base_year == 2020


def test_check_model_years_explicit_base_year_missing_raises():
    """base_year specified but not present in initial_value_df raises DSGInvalidQuery."""
    dataset = ProjectionDatasetModel(
        dataset_id="result",
        initial_value_dataset_id="initial",
        growth_rate_dataset_id="growth",
        base_year=2030,
    )
    initial = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 100.0},
            {"geo": "CA", "model_year": "2021", "value": 110.0},
        ]
    )
    growth = create_dataframe_from_dicts(
        [{"geo": "CA", "model_year": "2020", "value": 0.0}]
    )
    with pytest.raises(DSGInvalidQuery, match="base_year=2030 is not in"):
        _check_model_years(dataset, initial, growth, "model_year")


def test_check_model_years_filters_initial_to_base_when_multiple_iv_years(
    projection_dataset_2020,
):
    """With multiple iv_years and a chosen base_year, only the base_year rows survive."""
    initial = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 100.0},
            {"geo": "CA", "model_year": "2021", "value": 110.0},
            {"geo": "CA", "model_year": "2022", "value": 120.0},
        ]
    )
    growth = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 0.0},
            {"geo": "CA", "model_year": "2021", "value": 0.05},
        ]
    )
    filtered, base_year = _check_model_years(
        projection_dataset_2020, initial, growth, "model_year"
    )
    assert base_year == 2020
    # _check_model_years cross-joins growth's model_year set onto the
    # base-year-filtered initial table. So filtered has one row per
    # growth model_year (2 rows), each carrying initial value=100.
    rows = _collect(_order_by(filtered, "model_year"))
    assert len(rows) == 2
    for row in rows:
        assert row.value == 100.0


# --- apply_annual_multiplier ----------------------------------------------


def test_apply_annual_multiplier_simple():
    """Each value column multiplies element-wise by the matching ``<col>_gr`` column
    in the growth-rate table after joining on dimension columns.

    Note: ``apply_annual_multiplier`` expects the growth-rate columns to already be
    suffixed ``_gr``. ``_process_exponential_growth_rate`` produces that shape;
    ``apply_annual_multiplier`` is only a generic multiplier when callers feed it
    pre-renamed input.
    """
    initial = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 100.0},
            {"geo": "TX", "model_year": "2020", "value": 200.0},
        ]
    )
    growth = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value_gr": 1.10},
            {"geo": "TX", "model_year": "2020", "value_gr": 1.20},
        ]
    )
    result = apply_annual_multiplier(
        initial,
        growth,
        time_columns=set(),
        value_columns={"value"},
    )
    rows = _collect(_order_by(result, "geo"))
    assert len(rows) == 2
    by_geo = {row.geo: row for row in rows}
    assert math.isclose(by_geo["CA"].value, 100.0 * 1.10)
    assert math.isclose(by_geo["TX"].value, 200.0 * 1.20)


# --- apply_exponential_growth_rate end-to-end ----------------------------


def test_apply_exponential_growth_rate_one_dim_three_years(projection_dataset_2020):
    """P(t) = P0 * (1+r)^(t-t0) where t0=2020.

    With base_year=2020 the function filters initial down to 2020, then
    cross-joins the growth_rate model_year set (2020, 2021, 2022). The
    growth-rate column is exponentiated by (t - 2020) before being
    multiplied into the initial value.
    """
    initial = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 100.0},
        ]
    )
    growth = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 0.05},
            {"geo": "CA", "model_year": "2021", "value": 0.05},
            {"geo": "CA", "model_year": "2022", "value": 0.05},
        ]
    )
    result = apply_exponential_growth_rate(
        projection_dataset_2020,
        initial,
        growth,
        time_columns=set(),
        model_year_column="model_year",
        value_columns={"value"},
    )
    rows = _collect(_order_by(result, "model_year"))
    assert len(rows) == 3
    by_year = {row.model_year: row.value for row in rows}
    assert math.isclose(by_year["2020"], 100.0 * (1.05) ** 0)  # 100.0
    assert math.isclose(by_year["2021"], 100.0 * (1.05) ** 1)
    assert math.isclose(by_year["2022"], 100.0 * (1.05) ** 2)


# --- _process_exponential_growth_rate -------------------------------------


def test_process_exponential_growth_rate_uses_renamed_columns(projection_dataset_2020):
    """The returned growth_rate table has value columns renamed with _gr suffix and
    contains (1 + r)^(t - base_year) instead of r."""
    initial = create_dataframe_from_dicts(
        [{"geo": "CA", "model_year": "2020", "value": 100.0}]
    )
    growth = create_dataframe_from_dicts(
        [
            {"geo": "CA", "model_year": "2020", "value": 0.10},
            {"geo": "CA", "model_year": "2022", "value": 0.10},
        ]
    )
    _, gr_df = _process_exponential_growth_rate(
        projection_dataset_2020,
        initial,
        growth,
        "model_year",
        {"value"},
    )
    # Renamed column present; original gone.
    assert "value_gr" in gr_df.columns
    assert "value" not in gr_df.columns
    rows = _collect(_order_by(gr_df, "model_year"))
    by_year = {row.model_year: row.value_gr for row in rows}
    assert math.isclose(by_year["2020"], (1.10) ** 0)
    assert math.isclose(by_year["2022"], (1.10) ** 2)
