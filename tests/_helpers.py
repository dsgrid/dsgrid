"""Shared helpers for the test suite.

These wrap the small set of operations that appeared, copy-pasted, in nearly
every test module: materializing an Ibis table to a list of rows, ordering
rows for deterministic assertions, and reading a column from a row.

dsgrid produces only Ibis tables (in both the DuckDB and Spark backends), so
these helpers operate on ``ibis.Table`` exclusively. For plain row counts and
pandas conversion, call ``dsgrid.ibis.table_utils`` (``count_rows`` /
``table_to_pandas``) directly.
"""

from typing import Any

import ibis

from dsgrid.ibis.operations import create_temp_view
from dsgrid.ibis.session import get_runtime_session, get_spark_session


def make_table(columns: list[str], *rows: tuple) -> ibis.Table:
    """Build an Ibis table from a column header and one tuple per row.

    The pandas-like way to define small test tables so the data reads like a
    table (columns on top, one row per line) instead of a list of per-row dicts::

        df = make_table(
            ["county", "sector", "value"],
            ("Jefferson", "com", 2.1),
            ("Boulder", "com", 3.5),
        )

    Column types are inferred from the row values (``None`` for NULLs), matching
    ``get_runtime_session().createDataFrame`` on both backends.
    """
    return get_runtime_session().createDataFrame(list(rows), columns)


def perform_interval_op(
    df: ibis.Table, time_column: str, op: str, val: Any, unit: str, alias: str
) -> ibis.Table:
    """Shift ``time_column`` by an interval, returning ``df`` with the result as ``alias``.

    Test-only helper for building expected timestamps — e.g. converting between
    ``PERIOD_BEGINNING`` and ``PERIOD_ENDING`` by shifting one time step. When
    ``alias`` equals ``time_column`` the column is shifted in place (keeping its
    position); otherwise the shifted values are added as a new column.

    Parameters
    ----------
    df : ibis.Table
    time_column : str
        Name of the timestamp column to shift.
    op : str
        ``"+"`` or ``"-"``.
    val : Any
        Magnitude of the interval.
    unit : str
        Interval unit understood by :func:`ibis.interval` (e.g. ``"SECONDS"``).
    alias : str
        Output column name.
    """
    if op not in ("+", "-"):
        msg = f"Unsupported interval op: {op!r}"
        raise ValueError(msg)
    delta = ibis.interval(**{unit.lower(): val})
    shifted = df[time_column] + delta if op == "+" else df[time_column] - delta
    return df.mutate(**{alias: shifted})


def collect(df) -> list[Any]:
    """Materialize ``df`` into a list of named-tuple rows.

    NaN values in object-dtype columns are normalized to ``None`` so equality
    checks across rows are stable (the prior copy of this helper in
    ``test_dataset_utils`` did the same thing).
    """
    pdf = df.execute()
    for col in pdf.columns:
        mask = pdf[col].isna()
        if mask.any():
            pdf[col] = pdf[col].astype(object)
            pdf.loc[mask, col] = None
    return list(pdf.itertuples(index=False, name="Row"))


def order_by(df, *columns):
    """Order ``df`` by the given columns."""
    return df.order_by(*columns)


def row_value(row, key):
    """Read ``key`` from ``row``, supporting both NamedTuple and PySpark Row.

    ``key`` may be a column name (str) or a positional index (int).
    """
    if isinstance(key, int):
        return row[key]
    try:
        return row[key]
    except TypeError:
        return getattr(row, key)


def first_value(df, column: str):
    """Return ``column`` from the first row of ``df``."""
    return getattr(collect(df.limit(1))[0], column)


def spark_physical_plan(df: ibis.Table) -> str:
    """Return the Spark physical plan for ``df`` as a string.

    Spark-only. Use this to assert that an operation whose entire purpose is a
    shuffle (e.g. the salted repartitioning in
    ``repartition_if_needed_by_mapping``) actually survives into the plan that
    Spark executes — results alone cannot show it, because a missing shuffle
    changes performance, not values.

    Reaches through the JVM DataFrame because PySpark exposes the plan only via
    ``explain()``, which prints to stdout instead of returning it.
    """
    spark_df = get_spark_session().table(create_temp_view(df))
    return spark_df._jdf.queryExecution().executedPlan().toString()
