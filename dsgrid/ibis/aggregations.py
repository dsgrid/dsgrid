"""Aggregation-function definitions for the dsgrid Ibis abstraction layer.

This module is the single source of truth for the aggregation-function
vocabulary that may appear in a query's
:class:`~dsgrid.query.models.AggregationModel`. Every per-function
representation (Ibis reduction method for the native fast path, DuckDB and
Spark SQL expressions for the raw-SQL fallback) lives on an
:class:`AggregationSpec`.

dsgrid deliberately does not reject unregistered names: any plain SQL
identifier accepted by :class:`~dsgrid.query.models.FunctionReference` is
uppercased and forwarded to the backend, which may reject it at execution
time. (SQL function names are case-insensitive on both backends, so the
uppercasing does not change which function runs.) Registered names get the
native fast path and backend-correct SQL spellings;
:func:`sql_aggregate_expression` implements both rules in one place.
"""

from dataclasses import dataclass

from dsgrid.ibis.types import use_duckdb


@dataclass(frozen=True)
class AggregationSpec:
    """An aggregation function that dsgrid users may name in a query.

    Parameters
    ----------
    name
        The user-facing token written in ``aggregation_function``
        (e.g. ``"mean"``).
    ibis_method
        The Ibis ``Column`` reduction method used on the native fast path.
        It must be a reduction; scalar methods (e.g. ``round``) are not
        valid inside ``aggregate()``.
    duckdb_sql
        DuckDB SQL expression template for the raw-SQL fallback, with a
        ``{column}`` placeholder for the (already quoted) value column.
    spark_sql
        The same, for Spark SQL.
    """

    name: str
    ibis_method: str
    duckdb_sql: str
    spark_sql: str


# The first value of each AggregationSpec is the user-facing name to use in
# ``aggregation_function``. first/last are order-dependent on both backends,
# and their raw-SQL forms do not skip nulls while the Ibis reductions do.
AGGREGATION_SPECS: tuple[AggregationSpec, ...] = (
    AggregationSpec("sum", "sum", "SUM({column})", "SUM({column})"),
    AggregationSpec("mean", "mean", "AVG({column})", "AVG({column})"),
    AggregationSpec("min", "min", "MIN({column})", "MIN({column})"),
    AggregationSpec("max", "max", "MAX({column})", "MAX({column})"),
    AggregationSpec("count", "count", "COUNT({column})", "COUNT({column})"),
    AggregationSpec("median", "median", "MEDIAN({column})", "MEDIAN({column})"),
    AggregationSpec(
        "approx_median",
        "approx_median",
        "APPROX_QUANTILE({column}, 0.5)",
        "PERCENTILE_APPROX({column}, 0.5)",
    ),
    AggregationSpec("first", "first", "FIRST({column})", "FIRST({column})"),
    AggregationSpec("last", "last", "LAST({column})", "LAST({column})"),
)


_BY_NAME: dict[str, AggregationSpec] = {spec.name: spec for spec in AGGREGATION_SPECS}

SUPPORTED_AGGREGATIONS: frozenset[str] = frozenset(_BY_NAME)


def find_aggregation_spec(name: str) -> AggregationSpec | None:
    """Return the :class:`AggregationSpec` for ``name``, or None if unregistered."""
    return _BY_NAME.get(name)


def sql_aggregate_expression(op_name: str, column: str) -> str:
    """Build the SQL aggregation expression for ``op_name`` over ``column``.

    Registered names use their per-backend template. Unregistered names are
    uppercased and forwarded otherwise unchanged, preserving dsgrid's
    passthrough contract for any backend function a user cares to name; the
    backend rejects names it does not know at execution time.

    Parameters
    ----------
    op_name : str
        The user-facing aggregation-function name.
    column : str
        The value column, already quoted for the active backend.
    """
    spec = find_aggregation_spec(op_name)
    if spec is None:
        return f"{op_name.upper()}({column})"
    template = spec.duckdb_sql if use_duckdb() else spec.spark_sql
    return template.format(column=column)
