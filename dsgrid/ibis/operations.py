import logging
from typing import Any, cast
from tempfile import NamedTemporaryFile

import ibis

from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.temp import make_temp_view_name, track_temp_file
from dsgrid.ibis.types import use_duckdb

logger = logging.getLogger(__name__)


def create_temp_view(df: ibis.Table) -> str:
    """Register ``df`` as a temporary view in the runtime backend.

    Falls back through three strategies because cross-backend tables (e.g. an
    ibis Table read from the on-disk DuckDB data store) cannot be referenced by
    a CREATE VIEW running on the runtime in-memory backend:

    1. ``backend.create_view`` — works for tables already in the runtime backend.
    2. ``conn.create_table(..., temp=True)`` — materializes via duckdb when the
       table can be executed cross-backend.
    3. Round-trip through parquet — definitive fallback. Drops any leaked
       table/view name from earlier failed attempts before re-creating it.
    """
    view = make_temp_view_name()
    backend = make_runtime_backend()
    try:
        backend.create_view(view, df)
        return view
    except Exception:
        pass
    conn = cast(Any, backend).connection
    try:
        conn.create_table(view, df, temp=True)
        return view
    except Exception:
        pass
    tmp_file = NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_file.close()
    track_temp_file(tmp_file.name)
    logger.warning(
        "create_temp_view fell back to parquet round-trip via %s; this materializes the "
        "full table to disk and may indicate a cross-backend reference that should be "
        "registered into the runtime backend earlier. The file is tracked for cleanup by "
        "drop_temp_tables_and_views/atexit.",
        tmp_file.name,
    )
    df.to_parquet(tmp_file.name)
    escaped_path = tmp_file.name.replace("'", "''")
    conn.raw_sql(f"DROP TABLE IF EXISTS {view}")
    conn.raw_sql(f"DROP VIEW IF EXISTS {view}")
    conn.raw_sql(f"CREATE TEMP VIEW {view} AS SELECT * FROM read_parquet('{escaped_path}')")
    return view


def cross_join(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    df1, df2 = _ensure_same_backend(df1, df2)
    return df1.cross_join(df2)


def filter_sql(df: ibis.Table, predicate: str) -> ibis.Table:
    """Filter a table with a SQL predicate without materializing rows."""
    view = create_temp_view(df)
    return _sql_on_df(df, f"SELECT * FROM {view} WHERE {predicate}")


def rename_columns(df: ibis.Table, mapping: dict[str, str]) -> ibis.Table:
    """Rename columns using a mapping of old name to new name."""
    return df.rename({new: old for old, new in mapping.items()})


def drop_columns(df: ibis.Table, *columns: str) -> ibis.Table:
    """Drop columns with an explicit projection."""
    to_drop = set(columns)
    if not to_drop:
        return df
    return df.select(*(col for col in df.columns if col not in to_drop))


def _ensure_same_backend(df1: ibis.Table, df2: ibis.Table) -> tuple[ibis.Table, ibis.Table]:
    """Bring both tables into the runtime backend if they live in different backends.

    Native Ibis set ops and joins reject expressions spanning multiple backends.
    The store backend (e.g. on-disk DuckDB) is distinct from the runtime backend,
    so cross-backend operands must be registered into the runtime first.
    """
    try:
        b1 = df1._find_backend(use_default=False)
        b2 = df2._find_backend(use_default=False)
    except Exception:
        return df1, df2
    if b1 is b2:
        return df1, df2
    runtime = make_runtime_backend()
    if b1 is not runtime:
        df1 = runtime.table(create_temp_view(df1))
    if b2 is not runtime:
        df2 = runtime.table(create_temp_view(df2))
    return df1, df2


def except_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    """SQL ``EXCEPT ALL``. Inputs must share a schema; mismatches raise."""
    df1, df2 = _ensure_same_backend(df1, df2)
    return df1.difference(df2, distinct=False)


def intersect(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    """SQL ``INTERSECT``. Inputs must share a schema; mismatches raise."""
    df1, df2 = _ensure_same_backend(df1, df2)
    return df1.intersect(df2)


def union_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    """SQL ``UNION ALL``. Inputs must share a schema; mismatches raise."""
    df1, df2 = _ensure_same_backend(df1, df2)
    return df1.union(df2, distinct=False)


def count_distinct_on_group_by(
    df: ibis.Table, group_by_columns: list[str], agg_column: str, alias: str
) -> ibis.Table:
    return df.group_by(group_by_columns).aggregate(**{alias: df[agg_column].nunique()})


def handle_column_spaces(column: str) -> str:
    if use_duckdb():
        return f'"{column}"'
    return f"`{column}`"


def join(df1: ibis.Table, df2: ibis.Table, column1: str, column2: str, how="inner") -> ibis.Table:
    """Join two tables on a single column from each side.

    Drops df2 columns whose names overlap with df1 (matching the prior SQL
    semantics) and projects df1's columns followed by df2's remaining columns.
    Join key types must match exactly; cast the inputs before calling if they
    don't (e.g. ``df = df.mutate(id=df.id.cast("int64"))``).
    """
    df1, df2 = _ensure_same_backend(df1, df2)
    overlap = [c for c in df2.columns if c in df1.columns]
    df2_pruned = df2.drop(*overlap) if overlap else df2
    joined = df1.join(df2_pruned, df1[column1] == df2[column2], how=how)
    return joined.select(*df1.columns, *df2_pruned.columns)


def join_multiple_columns(
    df1: ibis.Table, df2: ibis.Table, columns: list[str], how="inner"
) -> ibis.Table:
    """Equi-join on the named columns. Join keys are deduplicated; other
    overlapping df2 columns are dropped. Join key types must match exactly
    on both sides; cast the inputs before calling if they don't."""
    df1, df2 = _ensure_same_backend(df1, df2)
    extra_overlap = [c for c in df2.columns if c in df1.columns and c not in columns]
    df2_pruned = df2.drop(*extra_overlap) if extra_overlap else df2
    return df1.join(df2_pruned, columns, how=how)


def sql_from_df(df: ibis.Table, query: str) -> ibis.Table:
    view = create_temp_view(df)
    return _sql_on_df(df, query + f" FROM {view}")


def pivot(df: ibis.Table, name_column: str, value_column: str) -> ibis.Table:
    return df.pivot_wider(
        names_from=name_column,
        values_from=value_column,
        values_agg="sum",
    )


def unpivot(df: ibis.Table, pivoted_columns, name_column: str, value_column: str) -> ibis.Table:
    return df.pivot_longer(pivoted_columns, names_to=name_column, values_to=value_column)


def aggregate_single_value(df: ibis.Table, agg_func: str, column: str) -> Any:
    return getattr(df[column], agg_func)().execute()


def _sql_on_df(df: ibis.Table, query: str) -> ibis.Table:
    return make_runtime_backend().sql(query)


def coalesce(df: ibis.Table, num_partitions: int) -> ibis.Table:
    """Reduce the number of output partitions.

    On DuckDB this is a no-op (single-file output by default). On Spark it
    coalesces the underlying PySpark DataFrame and re-registers it as an
    Ibis table so downstream writers produce ``num_partitions`` files.
    """
    if use_duckdb():
        return df
    view = create_temp_view(df)
    backend = cast(Any, make_runtime_backend())
    spark_df = backend.connection._session.sql(f"SELECT * FROM {view}")
    coalesced = spark_df.coalesce(num_partitions)
    coalesced_view = make_temp_view_name()
    coalesced.createOrReplaceTempView(coalesced_view)
    return backend.connection.table(coalesced_view)
