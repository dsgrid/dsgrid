"""NULL-value validation helpers for Ibis tables."""

from dsgrid.exceptions import DSGInvalidField
from dsgrid.utils.timing import timer_stats_collector, track_timing


@track_timing(timer_stats_collector)
def check_for_nulls(df, exclude_columns=None) -> None:
    """Check if an Ibis table has null values.

    Issues a single aggregation query that computes ``BOOL_OR(col IS NULL)``
    for every checked column.

    Parameters
    ----------
    df : ibis.Table
    exclude_columns : None or Set

    Raises
    ------
    DSGInvalidField
        Raised if null exists in any non-excluded column.
    """
    if exclude_columns is None:
        exclude_columns = set()
    cols_to_check = set(df.columns).difference(exclude_columns)
    if not cols_to_check:
        return

    aggs = {col: df[col].isnull().any() for col in cols_to_check}
    row = df.aggregate(**aggs).execute().iloc[0]
    cols_with_null = {col for col in cols_to_check if bool(row[col])}
    if cols_with_null:
        msg = f"Ibis table contains NULL value(s) for column(s): {cols_with_null}"
        raise DSGInvalidField(msg)
