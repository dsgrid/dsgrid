"""Characterization tests for :func:`dsgrid.dataset.unpivoted_table._aggregate_value`.

``_aggregate_value`` has two code paths that must produce identical results: a
native-Ibis fast path (bare group-by columns and a known reduction) and a
raw-SQL fallback (anything else). These tests pin the behavior of both paths
over a small hand-verifiable table, so that a refactor of the dispatch logic
must reproduce today's results exactly.

The input table is small enough to check by eye:

    geography  values           sum   mean  min  max  count  median
    CO         1.0, 2.0, 6.0    9.0   3.0   1.0  6.0  3      2.0
    NM         5.0, 7.0         12.0  6.0   5.0  7.0  2      6.0

The fallback is forced with an aliased group-by (``"geography AS geography"``):
``_looks_like_bare_column`` rejects anything containing ``" AS "``, so the
semantics are identical to the bare column and the two paths can be compared
directly.
"""

import math
from typing import Any

import pytest

from dsgrid.common import VALUE_COLUMN
from dsgrid.dataset.unpivoted_table import _aggregate_value
from dsgrid.ibis import aggregations
from dsgrid.ibis.aggregations import AggregationSpec
from dsgrid.ibis.session import create_dataframe_from_dicts

from tests._helpers import collect as _collect

LOAD_ROWS: list[dict[str, Any]] = [
    {"id": "co1", "geography": "CO", "value": 1.0},
    {"id": "co2", "geography": "CO", "value": 2.0},
    {"id": "co3", "geography": "CO", "value": 6.0},
    {"id": "nm1", "geography": "NM", "value": 5.0},
    {"id": "nm2", "geography": "NM", "value": 7.0},
]

GROUP_VALUES: dict[str, set[float]] = {"CO": {1.0, 2.0, 6.0}, "NM": {5.0, 7.0}}

# Expected results for the ops whose value does not depend on row order.
EXPECTED: dict[str, dict[str, float]] = {
    "sum": {"CO": 9.0, "NM": 12.0},
    "mean": {"CO": 3.0, "NM": 6.0},
    "min": {"CO": 1.0, "NM": 5.0},
    "max": {"CO": 6.0, "NM": 7.0},
    "count": {"CO": 3, "NM": 2},
    "median": {"CO": 2.0, "NM": 6.0},
}

# first/last are order-dependent on both backends; only membership is stable.
ORDER_DEPENDENT_OPS = ("first", "last")

# Group-by entries that force the raw-SQL fallback with unchanged semantics.
FORCED_SQL_GROUP_BY = ["geography AS geography"]


# One group with a NULL among real values, one group of all NULLs. SUM/AVG and the
# native reductions both skip NULLs; an all-NULL group yields NULL. The fully populated
# LOAD_ROWS above cannot exercise either, so NULL parity gets its own table.
#
#     geography  values        sum   mean
#     CO         1.0, ·, 3.0   4.0   2.0    (NULL skipped)
#     NM         ·, ·          None  None   (all NULL)
NULL_ROWS: list[dict[str, Any]] = [
    {"id": "co1", "geography": "CO", "value": 1.0},
    {"id": "co2", "geography": "CO", "value": None},
    {"id": "co3", "geography": "CO", "value": 3.0},
    {"id": "nm1", "geography": "NM", "value": None},
    {"id": "nm2", "geography": "NM", "value": None},
]

EXPECTED_WITH_NULLS: dict[str, dict[str, float | None]] = {
    "sum": {"CO": 4.0, "NM": None},
    "mean": {"CO": 2.0, "NM": None},
}


@pytest.fixture
def load_table():
    return create_dataframe_from_dicts(LOAD_ROWS)


@pytest.fixture
def null_table():
    return create_dataframe_from_dicts(NULL_ROWS)


def _close_or_none(actual: Any, expected: float | None) -> bool:
    if expected is None:
        return actual is None
    return actual is not None and math.isclose(actual, expected)


def _by_group(df) -> dict[str, Any]:
    return {row.geography: getattr(row, VALUE_COLUMN) for row in _collect(df)}


@pytest.mark.parametrize("op", sorted(EXPECTED))
def test_fast_path_pinned_results(load_table, op):
    result = _by_group(_aggregate_value(load_table, ["geography"], op))
    for geography, expected in EXPECTED[op].items():
        assert math.isclose(result[geography], expected), (op, geography)


@pytest.mark.parametrize("op", sorted(EXPECTED))
def test_sql_fallback_pinned_results(load_table, op):
    result = _by_group(_aggregate_value(load_table, FORCED_SQL_GROUP_BY, op))
    for geography, expected in EXPECTED[op].items():
        assert math.isclose(result[geography], expected), (op, geography)


@pytest.mark.parametrize("op", [*sorted(EXPECTED), "approx_median"])
def test_paths_agree(load_table, op):
    """The same logical query must not change its answer with the engine.

    A group-by column that carries an alias or function takes the raw-SQL
    fallback instead of the native-Ibis fast path; the results must match.
    approx_median qualifies because both paths now use the same approximate
    function, and the approximation is exact on a table this small.
    """
    fast = _by_group(_aggregate_value(load_table, ["geography"], op))
    fallback = _by_group(_aggregate_value(load_table, FORCED_SQL_GROUP_BY, op))
    assert fast.keys() == fallback.keys()
    for geography in fast:
        assert math.isclose(fast[geography], fallback[geography]), (op, geography)


@pytest.mark.parametrize("op", ["sum", "mean"])
def test_paths_agree_with_null_values(null_table, op):
    """NULLs in a group must be handled identically by both engines.

    This is the case the reviewer flagged that the fully populated table cannot
    exercise: a group with one NULL (skipped) and a group of all NULLs (yields
    NULL). Both paths must match each other and the hand-computed expectation.
    """
    fast = _by_group(_aggregate_value(null_table, ["geography"], op))
    fallback = _by_group(_aggregate_value(null_table, FORCED_SQL_GROUP_BY, op))
    expected = EXPECTED_WITH_NULLS[op]
    assert fast.keys() == fallback.keys() == expected.keys()
    for geography, exp in expected.items():
        assert _close_or_none(fast[geography], exp), ("fast", op, geography, fast[geography])
        assert _close_or_none(fallback[geography], exp), (
            "fallback",
            op,
            geography,
            fallback[geography],
        )


@pytest.mark.parametrize("op", ORDER_DEPENDENT_OPS)
def test_fast_path_order_dependent_ops(load_table, op):
    result = _by_group(_aggregate_value(load_table, ["geography"], op))
    for geography, values in GROUP_VALUES.items():
        assert result[geography] in values, (op, geography)


@pytest.mark.parametrize("op", ORDER_DEPENDENT_OPS)
def test_sql_fallback_order_dependent_ops(load_table, op):
    result = _by_group(_aggregate_value(load_table, FORCED_SQL_GROUP_BY, op))
    for geography, values in GROUP_VALUES.items():
        assert result[geography] in values, (op, geography)


def test_approx_median_fast_path(load_table):
    """Approximate medians vary by backend; pin only the possible range."""
    result = _by_group(_aggregate_value(load_table, ["geography"], "approx_median"))
    assert 1.0 <= result["CO"] <= 6.0
    assert 5.0 <= result["NM"] <= 7.0


def test_approx_median_sql_fallback(load_table):
    """The registry supplies backend-correct spellings for approx_median.

    Before the registry, this path uppercased the name into
    ``APPROX_MEDIAN(value)``, a function that exists on neither backend
    (DuckDB spells it ``approx_quantile(x, 0.5)``; Spark SQL
    ``percentile_approx(x, 0.5)``), so this query always raised.
    """
    result = _by_group(_aggregate_value(load_table, FORCED_SQL_GROUP_BY, "approx_median"))
    assert 1.0 <= result["CO"] <= 6.0
    assert 5.0 <= result["NM"] <= 7.0


def test_group_by_expression_fallback(load_table):
    """A function group-by takes the SQL path and aliases the derived column."""
    result = _aggregate_value(load_table, ["substr(id, 1, 1) AS g1", "geography"], "sum")
    by_group = {row.g1: getattr(row, VALUE_COLUMN) for row in _collect(result)}
    assert by_group == {"c": 9.0, "n": 12.0}


def test_unregistered_op_passes_through(load_table):
    """A name outside the fast-path allowlist is forwarded verbatim to the
    backend, even when every group-by column is bare."""
    result = _by_group(_aggregate_value(load_table, ["geography"], "stddev"))
    assert math.isclose(result["CO"], math.sqrt(7.0))  # sample stddev of {1, 2, 6}
    assert math.isclose(result["NM"], math.sqrt(2.0))  # sample stddev of {5, 7}


def test_registry_drives_both_paths(load_table, monkeypatch):
    """Both execution paths must read the registry, not private tables.

    Redefine ``sum`` to compute a max; if either path stops consulting the
    registry (say, a hard-coded allowlist or ``.upper()`` passthrough comes
    back), it keeps summing and this test fails.
    """
    monkeypatch.setitem(
        aggregations._BY_NAME,
        "sum",
        AggregationSpec("sum", "max", "MAX({column})", "MAX({column})"),
    )
    for group_by in (["geography"], FORCED_SQL_GROUP_BY):
        result = _by_group(_aggregate_value(load_table, group_by, "sum"))
        assert result == EXPECTED["max"], group_by
