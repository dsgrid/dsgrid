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
