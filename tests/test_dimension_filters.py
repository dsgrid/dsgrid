import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterBetweenColumnOperatorModel,
    DimensionFilterColumnOperatorModel,
    _escape_like_value,
    _make_between_where_clause,
    _make_column_operator_where_clause,
    _make_sql_value,
)
from dsgrid.exceptions import DSGInvalidField
from dsgrid.ibis.session import create_dataframe_from_dicts


@pytest.mark.parametrize(
    ("operator", "value", "expected"),
    (
        ("contains", "abc", "name LIKE '%abc%'"),
        ("contains", "O'Brien", "name LIKE '%O''Brien%'"),
        ("endswith", "xyz", "name LIKE '%xyz'"),
        ("isNotNull", None, "name IS NOT NULL"),
        ("isNull", None, "name IS NULL"),
        ("isin", ["a", "b"], "name IN ('a', 'b')"),
        ("isin", ("a", 2), "name IN ('a', 2)"),
        ("like", "a%", "name LIKE 'a%'"),
        ("rlike", "^a", "name RLIKE '^a'"),
        ("startswith", "abc", "name LIKE 'abc%'"),
    ),
)
def test_make_column_operator_where_clause(operator, value, expected):
    assert _make_column_operator_where_clause("name", operator, value) == expected


def test_make_column_operator_where_clause_negated():
    assert (
        _make_column_operator_where_clause("name", "startswith", "abc", negate=True)
        == "NOT (name LIKE 'abc%')"
    )


def test_make_column_operator_where_clause_invalid():
    with pytest.raises(DSGInvalidField, match="value must be"):
        _make_column_operator_where_clause("name", "isin", "abc")
    with pytest.raises(DSGInvalidField, match="operator ='unknown'"):
        _make_column_operator_where_clause("name", "unknown", "abc")
    with pytest.raises(DSGInvalidField, match="Unsupported type for LIKE value"):
        _make_column_operator_where_clause("name", "contains", 1)


def test_make_between_where_clause():
    assert _make_between_where_clause("year", 2020, 2030) == "year BETWEEN 2020 AND 2030"
    assert (
        _make_between_where_clause("year", 2020, 2030, negate=True)
        == "NOT (year BETWEEN 2020 AND 2030)"
    )


def test_make_sql_value():
    assert _make_sql_value("O'Brien") == "'O''Brien'"
    assert _make_sql_value(1) == "1"
    assert _make_sql_value(1.5) == "1.5"
    with pytest.raises(DSGInvalidField, match="Unsupported type"):
        _make_sql_value(None)


def test_escape_like_value():
    assert _escape_like_value("O'Brien") == "O''Brien"
    with pytest.raises(DSGInvalidField, match="Unsupported type for LIKE value"):
        _escape_like_value(1)


def test_column_operator_model_apply_filter():
    table = create_dataframe_from_dicts(
        [
            {"id": "a", "name": "Alpha"},
            {"id": "b", "name": "Beta"},
            {"id": "d", "name": "Delta"},
        ]
    )
    model = DimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column="name",
        operator="contains",
        value="ph",
        negate=True,
    )
    assert [row.id for row in model.apply_filter(table).execute().itertuples()] == ["b", "d"]


def test_between_column_operator_model_apply_filter():
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
    assert [row.id for row in model.apply_filter(table).execute().itertuples()] == ["b", "c"]
