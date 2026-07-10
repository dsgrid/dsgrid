import pytest
from pydantic import ValidationError

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterBetweenColumnOperatorModel,
    DimensionFilterColumnOperatorModel,
    SupplementalDimensionFilterColumnOperatorModel,
    _make_sql_value,
)
from dsgrid.ibis.filter_operators import FILTER_OPERATOR_NAMES, apply_filter_operator
from dsgrid.exceptions import DSGInvalidField
from dsgrid.ibis.session import create_dataframe_from_dicts


def _filter_ids(model, table):
    return [row.id for row in model.apply_filter(table).execute().itertuples()]


@pytest.fixture
def name_table():
    return create_dataframe_from_dicts(
        [
            {"id": "a", "name": "Alpha"},
            {"id": "b", "name": "Beta"},
            {"id": "c", "name": "Delta"},
            {"id": "d", "name": None},
        ]
    )


def _model(operator, value, negate=False, column="name"):
    return DimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column=column,
        operator=operator,
        value=value,
        negate=negate,
    )


def test_contains(name_table):
    assert _filter_ids(_model("contains", "lph"), name_table) == ["a"]


def test_contains_negate(name_table):
    assert _filter_ids(_model("contains", "ph", negate=True), name_table) == ["b", "c"]


def test_startswith(name_table):
    assert _filter_ids(_model("startswith", "Be"), name_table) == ["b"]


def test_endswith(name_table):
    assert _filter_ids(_model("endswith", "ta"), name_table) == ["b", "c"]


def test_isnull(name_table):
    assert _filter_ids(_model("isNull", None), name_table) == ["d"]


def test_isnotnull(name_table):
    assert _filter_ids(_model("isNotNull", None), name_table) == ["a", "b", "c"]


def test_isin(name_table):
    assert _filter_ids(_model("isin", ["Alpha", "Delta"]), name_table) == ["a", "c"]


def test_isin_invalid(name_table):
    with pytest.raises(DSGInvalidField, match="value must be"):
        _filter_ids(_model("isin", "abc"), name_table)


def test_like(name_table):
    assert _filter_ids(_model("like", "A%"), name_table) == ["a"]


def test_rlike(name_table):
    assert _filter_ids(_model("rlike", "^B"), name_table) == ["b"]


def test_contains_literal_wildcards():
    table = create_dataframe_from_dicts(
        [
            {"id": "a", "name": "50% off"},
            {"id": "b", "name": "50 off"},
            {"id": "c", "name": "a_b"},
            {"id": "d", "name": "axb"},
        ]
    )
    assert _filter_ids(_model("contains", "%"), table) == ["a"]
    assert _filter_ids(_model("contains", "_"), table) == ["c"]


def test_between():
    table = create_dataframe_from_dicts(
        [
            {"id": "a", "year": 2019},
            {"id": "b", "year": 2020},
            {"id": "c", "year": 2021},
        ]
    )
    model = DimensionFilterBetweenColumnOperatorModel(
        dimension_type=DimensionType.MODEL_YEAR,
        dimension_name="model_year",
        column="year",
        lower_bound=2020,
        upper_bound=2021,
    )
    assert _filter_ids(model, table) == ["b", "c"]


def test_between_negate():
    table = create_dataframe_from_dicts(
        [
            {"id": "a", "year": 2019},
            {"id": "b", "year": 2020},
            {"id": "c", "year": 2021},
        ]
    )
    model = DimensionFilterBetweenColumnOperatorModel(
        dimension_type=DimensionType.MODEL_YEAR,
        dimension_name="model_year",
        column="year",
        lower_bound=2020,
        upper_bound=2021,
        negate=True,
    )
    assert _filter_ids(model, table) == ["a"]


def test_supplemental_column_operator_apply_filter(name_table):
    model = SupplementalDimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column="name",
        operator="startswith",
        value="B",
    )
    assert _filter_ids(model, name_table) == ["b"]


def test_supplemental_column_operator_defaults(name_table):
    """The default filter (``like "%"``) matches every non-null record."""
    model = SupplementalDimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column="name",
    )
    assert _filter_ids(model, name_table) == ["a", "b", "c"]


def test_unknown_operator_rejected():
    with pytest.raises(ValidationError, match="is not supported"):
        _model("cont", "lph")


@pytest.fixture
def year_table():
    return create_dataframe_from_dicts(
        [
            {"id": "a", "year": 2019},
            {"id": "b", "year": 2020},
            {"id": "c", "year": 2021},
        ]
    )


def test_between_via_column_operator(year_table):
    """The column-operator model accepts the two-element form its ``value``
    field description has always promised."""
    model = _model("between", [2020, 2021], column="year")
    assert _filter_ids(model, year_table) == ["b", "c"]


def test_between_via_column_operator_negate(year_table):
    model = _model("between", [2020, 2021], negate=True, column="year")
    assert _filter_ids(model, year_table) == ["a"]


@pytest.mark.parametrize("value", [2020, [2020], [2019, 2020, 2021]])
def test_between_value_arity_error(year_table, value):
    with pytest.raises(DSGInvalidField, match="two-element"):
        _filter_ids(_model("between", value, column="year"), year_table)


# One (value, expected ids) sample per registered operator, evaluated against
# name_table. Paired with test_every_operator_has_a_dispatch_case, this makes
# it impossible to register an operator without a working applier.
DISPATCH_CASES = [
    ("contains", "lph", ["a"]),
    ("startswith", "Be", ["b"]),
    ("endswith", "ta", ["b", "c"]),
    ("like", "A%", ["a"]),
    ("rlike", "^B", ["b"]),
    ("isNull", None, ["d"]),
    ("isNotNull", None, ["a", "b", "c"]),
    ("isin", ["Alpha", "Delta"], ["a", "c"]),
    ("between", ["A", "C"], ["a", "b"]),
]


def test_every_operator_has_a_dispatch_case():
    assert {case[0] for case in DISPATCH_CASES} == set(FILTER_OPERATOR_NAMES)


@pytest.mark.parametrize("operator,value,expected", DISPATCH_CASES)
def test_registry_drives_dispatch(name_table, operator, value, expected):
    df = apply_filter_operator(name_table, "name", operator, value, negate=False)
    assert [row.id for row in df.execute().itertuples()] == expected


def test_make_sql_value():
    assert _make_sql_value("O'Brien") == "'O''Brien'"
    assert _make_sql_value(1) == "1"
    assert _make_sql_value(1.5) == "1.5"
    with pytest.raises(DSGInvalidField, match="Unsupported type"):
        _make_sql_value(None)
