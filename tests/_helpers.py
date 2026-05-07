"""Shared helpers for the test suite.

These wrap the small set of operations that appeared, copy-pasted, in nearly
every test module: materializing an Ibis table to a list of rows, counting
rows, ordering rows for deterministic assertions, and reading a column from
a row whose type varies by backend.

The functions accept either an ``ibis.Table`` or a legacy PySpark
``DataFrame``. dsgrid produces only Ibis tables today; the PySpark fallback
is kept so tests still work if a non-Ibis intermediate ever appears.
"""

from typing import Any

import ibis

from dsgrid.ibis.table_utils import table_to_pandas


def collect(df) -> list[Any]:
    """Materialize ``df`` into a list of named-tuple rows.

    For Ibis tables, NaN values in object-dtype columns are normalized to
    ``None`` so equality checks across rows are stable (the prior copy of
    this helper in ``test_dataset_utils`` did the same thing).
    """
    if isinstance(df, ibis.Table):
        pdf = df.execute()
        for col in pdf.columns:
            mask = pdf[col].isna()
            if mask.any():
                pdf[col] = pdf[col].astype(object)
                pdf.loc[mask, col] = None
        return list(pdf.itertuples(index=False, name="Row"))
    return df.collect()


def count(df) -> int:
    """Return the number of rows in ``df``."""
    if isinstance(df, ibis.Table):
        return int(df.count().execute())
    return int(df.count())


def order_by(df, *columns):
    """Order ``df`` by the given columns. Works on either Ibis or PySpark."""
    if isinstance(df, ibis.Table):
        return df.order_by(*columns)
    # PySpark uses both .sort and .orderBy; .sort is the more idiomatic alias.
    return df.sort(*columns)


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


def to_pandas(df):
    """Materialize ``df`` to a pandas DataFrame, regardless of backend."""
    return table_to_pandas(df)
