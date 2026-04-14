from typing import Any, cast
from tempfile import NamedTemporaryFile

import ibis

from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.temp import make_temp_view_name
from dsgrid.ibis.types import use_duckdb


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
    df.to_parquet(tmp_file.name)
    escaped_path = tmp_file.name.replace("'", "''")
    conn.raw_sql(f"DROP TABLE IF EXISTS {view}")
    conn.raw_sql(f"DROP VIEW IF EXISTS {view}")
    conn.raw_sql(f"CREATE TEMP VIEW {view} AS SELECT * FROM read_parquet('{escaped_path}')")
    return view


def cross_join(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    df1, df2 = _ensure_same_backend(df1, df2)
    return df1.cross_join(df2)


def coalesce(df: ibis.Table, num_partitions: int) -> ibis.Table:
    """Reduce the number of output partitions.

    On DuckDB this is a no-op (single-file output by default). On Spark it
    coalesces the underlying PySpark DataFrame and re-registers it as an
    Ibis table so downstream writers produce `num_partitions` files.
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


def _promote_dtype(t1, t2):
    if t1.is_integer() and t2.is_integer():
        return "int64"
    if t1.is_numeric() and t2.is_numeric():
        return "float64"
    return "string"


def _align_set_op_schemas(df1: ibis.Table, df2: ibis.Table) -> tuple[ibis.Table, ibis.Table]:
    """Align shared columns on both sides so Ibis set ops accept mismatched
    numeric widths (int32 vs int64, etc.) the way SQL EXCEPT ALL / UNION did."""
    s1 = df1.schema()
    s2 = df2.schema()
    mutations1: dict[str, Any] = {}
    mutations2: dict[str, Any] = {}
    for name in df1.columns:
        if name in s2 and s1[name] != s2[name]:
            target = _promote_dtype(s1[name], s2[name])
            mutations1[name] = df1[name].cast(target)
            mutations2[name] = df2[name].cast(target)
    if mutations1:
        df1 = df1.mutate(**mutations1)
        df2 = df2.mutate(**mutations2)
    return df1, df2


def except_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    df1, df2 = _ensure_same_backend(df1, df2)
    df1, df2 = _align_set_op_schemas(df1, df2)
    return df1.difference(df2, distinct=False)


def intersect(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    df1, df2 = _ensure_same_backend(df1, df2)
    df1, df2 = _align_set_op_schemas(df1, df2)
    return df1.intersect(df2)


def union_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    df1, df2 = _ensure_same_backend(df1, df2)
    df1, df2 = _align_set_op_schemas(df1, df2)
    return df1.union(df2, distinct=False)


def count_distinct_on_group_by(
    df: ibis.Table, group_by_columns: list[str], agg_column: str, alias: str
) -> ibis.Table:
    return df.group_by(group_by_columns).aggregate(**{alias: df[agg_column].nunique()})


def handle_column_spaces(column: str) -> str:
    if use_duckdb():
        return f'"{column}"'
    return f"`{column}`"


def _align_join_key_types(
    df1: ibis.Table, df2: ibis.Table, col1: str, col2: str
) -> tuple[ibis.Table, ibis.Table]:
    """Cast mismatched join keys to a common type to mimic SQL EQ auto-coercion."""
    t1 = df1.schema()[col1]
    t2 = df2.schema()[col2]
    if t1 == t2:
        return df1, df2
    target = _promote_dtype(t1, t2)
    df1 = df1.mutate(**{col1: df1[col1].cast(target)})
    df2 = df2.mutate(**{col2: df2[col2].cast(target)})
    return df1, df2


def join(df1: ibis.Table, df2: ibis.Table, column1: str, column2: str, how="inner") -> ibis.Table:
    """Join two tables on a single column from each side.

    Drops df2 columns whose names overlap with df1 (matching the prior SQL
    semantics) and projects df1's columns followed by df2's remaining columns.
    """
    df1, df2 = _ensure_same_backend(df1, df2)
    df1, df2 = _align_join_key_types(df1, df2, column1, column2)
    overlap = [c for c in df2.columns if c in df1.columns]
    df2_pruned = df2.drop(*overlap) if overlap else df2
    joined = df1.join(df2_pruned, df1[column1] == df2[column2], how=how)
    return joined.select(*df1.columns, *df2_pruned.columns)


def join_multiple_columns(
    df1: ibis.Table, df2: ibis.Table, columns: list[str], how="inner"
) -> ibis.Table:
    """Equi-join on the named columns. Join keys are deduplicated; other
    overlapping df2 columns are dropped."""
    df1, df2 = _ensure_same_backend(df1, df2)
    for col in columns:
        df1, df2 = _align_join_key_types(df1, df2, col, col)
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
