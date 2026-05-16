"""NULL-value validation helpers for Ibis tables.

Separated from session.py because the NULL-finding logic issues N+1 SQL
queries on failure (one per column with at least one NULL) and is on
the Phase 11 perf-fix list. Keeping it in its own module makes the
rewrite easier to scope.
"""

from typing import Any, cast

from dsgrid.exceptions import DSGInvalidField
from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.operations import create_temp_view, handle_column_spaces
from dsgrid.ibis.types import is_table_empty
from dsgrid.utils.timing import timer_stats_collector, track_timing


@track_timing(timer_stats_collector)
def check_for_nulls(df, exclude_columns=None) -> None:
    """Check if an Ibis table has null values.

    Parameters
    ----------
    df : ibis.Table
    exclude_columns : None or Set

    Raises
    ------
    DSGInvalidField
        Raised if null exists in any column.
    """
    # Lazy import: session.py imports this module's siblings during
    # bootstrap; avoid a circular import via the runtime backend's sql().
    from dsgrid.ibis.session import get_runtime_session

    if exclude_columns is None:
        exclude_columns = set()
    cols_to_check = set(df.columns).difference(exclude_columns)
    if not cols_to_check:
        return
    view = create_temp_view(df)
    cols_str = ", ".join(handle_column_spaces(x) for x in cols_to_check)
    filter_str = " OR ".join(f"{handle_column_spaces(x)} IS NULL" for x in cols_to_check)

    try:
        # Avoid iterating with many checks unless we know there is at least one failure.
        nulls = get_runtime_session().sql(f"SELECT {cols_str} FROM {view} WHERE {filter_str}")
        if not is_table_empty(nulls):
            cols_with_null = set()
            for col in cols_to_check:
                quoted_col = handle_column_spaces(col)
                col_nulls = get_runtime_session().sql(
                    f"SELECT {quoted_col} FROM {view} WHERE {quoted_col} IS NULL LIMIT 1"
                )
                if not is_table_empty(col_nulls):
                    cols_with_null.add(col)
            assert cols_with_null, "Did not find any columns with NULL values"

            msg = f"Ibis table contains NULL value(s) for column(s): {cols_with_null}"
            raise DSGInvalidField(msg)
    finally:
        conn = cast(Any, make_runtime_backend().connection)
        conn.raw_sql(f"DROP VIEW IF EXISTS {view}")
