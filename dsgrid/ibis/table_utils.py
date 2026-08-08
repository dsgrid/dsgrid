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
