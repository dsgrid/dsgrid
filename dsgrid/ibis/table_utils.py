from typing import Any, Sequence

import ibis
import pandas as pd


def count_rows(table: ibis.Table) -> int:
    """Return the row count of a table by executing ``table.count()``."""
    return int(table.count().execute())  # ty: ignore[invalid-argument-type]


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

    Empty tables produce ``None`` for a ``collect`` aggregation on both Spark
    and DuckDB; in that case the corresponding entry is an empty set. NULL
    values within a column are dropped by ``collect`` (matching the
    semantics of the per-column ``.distinct()`` loop this replaces).
    """
    column_names = list(columns)
    if not column_names:
        return {}
    aggs = {col: table[col].collect(distinct=True) for col in column_names}
    row = table.aggregate(**aggs).execute().iloc[0]
    result: dict[str, set] = {}
    for col in column_names:
        value = row[col]
        if value is None:
            # Empty table: collect returns NULL.
            result[col] = set()
        else:
            # value is a list (DuckDB) or numpy array (Spark/pandas); both are
            # iterable. Filter NULL entries that an all-NULL column may emit.
            result[col] = {v for v in value if v is not None}
    return result
