"""Materialize-to-memory helpers for known-small Ibis tables (counts, distinct values, records)."""

from typing import Any, Sequence

import ibis
import pandas as pd


def count_rows(table: ibis.Table) -> int:
    """Return the row count of a table by executing ``table.count()``."""
    return int(table.count().execute())  # ty: ignore[invalid-argument-type]


def count_distinct(table: ibis.Table, column: str) -> int:
    """Return the number of distinct values in a single column."""
    return int(
        table.select(column).distinct().count().execute()  # ty: ignore[invalid-argument-type]
    )


def is_table_empty(table: Any) -> bool:
    """Return True if a table-like object has no rows."""
    return table.limit(1).count().execute() == 0


def table_to_pandas(table: Any) -> pd.DataFrame:
    """Execute a table-like object and return a pandas DataFrame.

    Use this only for metadata, capped diagnostics, API payloads, or other known-small
    tables. Large dataset tables should stay as lazy Ibis expressions.
    """
    return table.execute()


def table_to_records(table: Any) -> list[dict]:
    """Collect a known-small table as records without routing through pandas."""
    return table.to_pyarrow().to_pylist()


def table_column_to_list(table: Any, column: str) -> list:
    """Collect one known-small column without routing through pandas."""
    return [row[column] for row in table_to_records(table.select(column))]


def get_unique_values(table: Any, columns: Sequence[str] | str) -> set:
    """Return distinct values from a known-small table column or column set."""
    if isinstance(columns, str):
        column_names = [columns]
        single_column = True
    else:
        column_names = list(columns)
        single_column = False

    rows = table.select(column_names).distinct().to_pyarrow().to_pylist()
    if single_column:
        return {row[column_names[0]] for row in rows}
    return {tuple(row[col] for col in column_names) for row in rows}


def get_unique_values_per_column(table: Any, columns: Sequence[str]) -> dict[str, set]:
    """Return ``{column: set(distinct_values)}`` collected in a single query.

    Each column is reduced to its distinct values via ``collect(distinct=True)``
    in one aggregation. This replaces an N-column ``for col in columns: ...``
    loop where each iteration would issue a separate ``DISTINCT col`` execute,
    a hot-path pattern that dominated cost in the dimension-association
    validators (one execute per column × two sides of the comparison).

    ``collect`` drops NULL on both backends, so each column also aggregates a
    non-null count that is compared against the table's row count; a column
    containing NULL gets ``None`` added to its set, matching the semantics of
    per-column :func:`get_unique_values`.
    """
    column_names = list(columns)
    if not column_names:
        return {}
    aggs: dict[str, Any] = {"__row_count": table.count()}
    for col in column_names:
        aggs[f"{col}__values"] = table[col].collect(distinct=True)
        # count(col) excludes NULL; comparing to the row count detects NULL
        # without a bool_or aggregate, which is NULL (not False) on empty input.
        aggs[f"{col}__non_null_count"] = table[col].count()
    row = table.aggregate(**aggs).execute().iloc[0]
    result: dict[str, set] = {}
    for col in column_names:
        collected = row[f"{col}__values"]
        # Empty table: collect returns NULL instead of an empty list/array. An
        # all-NULL column may emit null entries in the array on some backends;
        # the non-null-count comparison below is the sole authority on NULL.
        values = set() if collected is None else {v for v in collected if v is not None}
        if row[f"{col}__non_null_count"] < row["__row_count"]:
            values.add(None)
        result[col] = values
    return result
