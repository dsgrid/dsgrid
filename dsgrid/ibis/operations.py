"""Lazy table transforms (joins, set ops, aggregations) on Ibis expressions."""

import logging
from typing import Any, cast
from tempfile import NamedTemporaryFile

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.temp import make_temp_view_name, track_temp_file
from dsgrid.ibis.types import use_duckdb

logger = logging.getLogger(__name__)

# Semi/anti joins project only the left table's columns, so df2 name collisions are
# harmless and there is nothing to reorder.
_LEFT_ONLY_JOINS = frozenset({"semi", "anti"})


def create_temp_view(df: ibis.Table) -> str:
    """Register ``df`` as a temporary view in the runtime backend.

    Falls back through three strategies because cross-backend tables (e.g. an
    ibis Table read from the on-disk DuckDB data store) cannot be referenced by
    a CREATE VIEW running on the runtime in-memory backend:

    1. ``backend.create_view`` — works for tables already in the runtime backend.
    2. ``conn.create_table(..., temp=True)`` — materializes via duckdb when the
       table can be executed cross-backend.
    3. Round-trip through parquet — last resort. Drops any leaked table/view name
       from earlier failed attempts before re-creating it.

    Step 3 issues DuckDB-only SQL (``CREATE TEMP VIEW ... read_parquet(...)``) and
    fails under a Spark runtime. That is survivable only because the sole
    heterogeneous configuration, a DuckDB store beneath a Spark runtime, is rejected
    by :meth:`~dsgrid.registry.duckdb_data_store.DuckDbDataStore.__init__`.
    """
    view = make_temp_view_name()
    backend = get_runtime_backend()
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


def _sole_backend(df: ibis.Table) -> Any | None:
    """Return the one ibis backend ``df`` is bound to, or None if it is bound to none.

    None means the expression carries no backend of its own -- a memtable, a
    literal, or an unbound table -- so there is nothing to relocate and Ibis will
    bind it at execution time.

    Raises
    ------
    DSGInvalidOperation
        If ``df`` already spans more than one backend, which no amount of
        re-registering here can repair.
    """
    backends, _ = df._find_backends()
    if not backends:
        return None
    if len(backends) > 1:
        names = sorted(getattr(b, "name", type(b).__name__) for b in backends)
        msg = (
            f"Table already spans multiple backends {names}; it cannot be executed or "
            "registered into a single backend. Build each operand from tables in one "
            "backend before combining them."
        )
        raise DSGInvalidOperation(msg)
    return backends[0]


def _ensure_same_backend(df1: ibis.Table, df2: ibis.Table) -> tuple[ibis.Table, ibis.Table]:
    """Bring both tables into the runtime backend if they live in different backends.

    Native Ibis set ops and joins reject expressions spanning multiple backends, so
    an operand bound to some other backend must be registered into the runtime first.

    This should not fire in practice.
    :class:`~dsgrid.registry.duckdb_data_store.DuckDbDataStore` ATTACHes its file to
    the runtime DuckDB connection and returns runtime-bound tables, and it refuses to
    construct at all under a Spark runtime. Reaching the fallback therefore means a
    store was added without the ATTACH wiring. The warning says so rather than paying
    the ``create_temp_view`` round-trip silently.

    Backends are compared by identity, never by equality: two *distinct* DuckDB
    connections compare equal and hash equal, so ``b1 == b2`` would call a genuine
    cross-connection reference same-backend and let it through. For the same reason
    Ibis collapses them into a single entry, so ``_sole_backend`` only ever sees more
    than one backend when their classes differ (e.g. DuckDB and Spark).
    """
    b1 = _sole_backend(df1)
    b2 = _sole_backend(df2)
    if b1 is b2:
        # Same backend, or neither is bound to one. Nothing to do either way.
        return df1, df2
    runtime = get_runtime_backend()
    # ``_find_backends`` returns underlying ibis backends (e.g.
    # ``ibis.backends.duckdb.Backend``), while ``get_runtime_backend`` returns
    # chronify's ``IbisBackend`` wrapper. Compare against the wrapper's
    # ``.connection`` (which IS the inner backend) so runtime-bound tables
    # don't get unnecessarily round-tripped through ``create_temp_view``.
    runtime_inner = runtime.connection
    if b1 is not None and b1 is not runtime_inner:
        df1 = _register_in_runtime(df1, b1, runtime)
    if b2 is not None and b2 is not runtime_inner:
        df2 = _register_in_runtime(df2, b2, runtime)
    return df1, df2


def _register_in_runtime(df: ibis.Table, source: Any, runtime: Any) -> ibis.Table:
    logger.warning(
        "Cross-backend operand detected (source=%s runtime=%s); falling "
        "back to create_temp_view. If both backends are DuckDB this "
        "indicates the source store did not ATTACH to the runtime — see "
        "DuckDbDataStore.__init__ for the established pattern.",
        getattr(source, "name", type(source).__name__),
        getattr(runtime, "name", type(runtime).__name__),
    )
    return runtime.table(create_temp_view(df))


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


def count_groups(df: ibis.Table, columns: list[str]) -> ibis.Table:
    """Return per-group row counts in a ``count`` column (whole-table count if no columns)."""
    if not columns:
        return df.aggregate(count=df.count())
    return df.group_by(*columns).aggregate(count=df.count())


def max_by_group(
    df: ibis.Table, group_by_columns: list[str], value_columns: list[str]
) -> ibis.Table:
    """Return the per-group maximum of each value column."""
    aggs = {col: df[col].max() for col in value_columns}
    return df.group_by(*group_by_columns).aggregate(**aggs)


def handle_column_spaces(column: str) -> str:
    if use_duckdb():
        return f'"{column}"'
    return f"`{column}`"


def join(df1: ibis.Table, df2: ibis.Table, column1: str, column2: str, how="inner") -> ibis.Table:
    """Join two tables on a single column from each side.

    Projects df1's columns followed by df2's, so no df2 column name may collide
    with a df1 column name -- including ``column2`` itself, which is retained for
    the caller to drop. Rename the colliding columns before calling. Join key
    types must match exactly; cast the inputs before calling if they don't
    (e.g. ``df = df.cast({"id": "int64"})``).

    Raises
    ------
    DSGInvalidOperation
        If any df2 column name also appears in df1.
    """
    df1, df2 = _ensure_same_backend(df1, df2)
    if how in _LEFT_ONLY_JOINS:
        return df1.join(df2, df1[column1] == df2[column2], how=how)
    _check_no_overlap(df1, df2, [])
    joined = df1.join(df2, df1[column1] == df2[column2], how=how)
    return joined.select(*df1.columns, *df2.columns)


def join_multiple_columns(
    df1: ibis.Table, df2: ibis.Table, columns: list[str], how="inner"
) -> ibis.Table:
    """Equi-join on the named columns.

    Join keys are deduplicated: the equi-join guarantees df1 and df2 hold the same
    value for each key, so only one copy is kept. Any *other* df2 column whose name
    collides with df1 carries independent data that a join cannot reconcile, so it
    is rejected rather than silently dropped; rename it before calling. Join key
    types must match exactly on both sides; cast the inputs before calling if they
    don't (e.g. ``df = df.cast({"id": "int64"})``).

    Raises
    ------
    DSGInvalidOperation
        If a df2 column name that is not a join key also appears in df1.
    """
    df1, df2 = _ensure_same_backend(df1, df2)
    if how not in _LEFT_ONLY_JOINS:
        _check_no_overlap(df1, df2, columns)
    return df1.join(df2, columns, how=how)


def _check_no_overlap(df1: ibis.Table, df2: ibis.Table, deduplicated_join_keys: list[str]) -> None:
    """Reject df2 column names that collide with df1 and are not deduplicated join keys.

    ``deduplicated_join_keys`` names only the join keys the join itself collapses to a
    single column. Everything else that collides -- including a join key the join does
    *not* collapse, such as ``join``'s ``column2`` -- holds data the join cannot
    reconcile into one column.
    """
    overlap = [c for c in df2.columns if c in df1.columns and c not in deduplicated_join_keys]
    if overlap:
        msg = (
            f"Cannot join: df2 columns {overlap} collide with df1 column names. A join "
            "cannot reconcile two same-named columns holding independent data. Rename "
            "them first, as callers do with prefixes/suffixes like 'from_'/'to_' or "
            "'__other'."
        )
        raise DSGInvalidOperation(msg)


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
    return get_runtime_backend().sql(query)


def cross_join_dfs(dfs: list[ibis.Table]) -> ibis.Table:
    """Perform a cross join of all tables in dfs."""
    if len(dfs) == 1:
        return dfs[0]

    df = dfs[0]
    for other in dfs[1:]:
        df = cross_join(df, other)
    return df


def coalesce(df: ibis.Table, num_partitions: int) -> ibis.Table:
    """Reduce the number of output partitions.

    On DuckDB this is a no-op (single-file output by default). On Spark it
    coalesces the underlying PySpark DataFrame and re-registers it as an
    Ibis table so downstream writers produce ``num_partitions`` files.
    """
    if use_duckdb():
        return df
    view = create_temp_view(df)
    backend = cast(Any, get_runtime_backend())
    spark_df = backend.connection._session.sql(f"SELECT * FROM {view}")
    coalesced = spark_df.coalesce(num_partitions)
    coalesced_view = make_temp_view_name()
    coalesced.createOrReplaceTempView(coalesced_view)
    return backend.connection.table(coalesced_view)


def repartition(df: ibis.Table, num_partitions: int, *columns: str) -> ibis.Table:
    """Repartition an Ibis table.

    On DuckDB this is a no-op (single-file output by default; partitioning
    is a Spark-execution concern that has no DuckDB equivalent through this
    helper). On Spark it issues PySpark's ``DataFrame.repartition(...)``
    via the same temp-view dance as :func:`coalesce` and re-registers the
    result as an Ibis table so downstream writers produce the requested
    number of output files.

    When ``columns`` is non-empty, ``repartition(num_partitions, *columns)``
    hash-partitions on those columns; otherwise it's a plain round-robin
    repartition.
    """
    if use_duckdb():
        return df
    view = create_temp_view(df)
    backend = cast(Any, get_runtime_backend())
    spark_df = backend.connection._session.sql(f"SELECT * FROM {view}")
    repartitioned = spark_df.repartition(num_partitions, *columns)
    repartitioned_view = make_temp_view_name()
    repartitioned.createOrReplaceTempView(repartitioned_view)
    return backend.connection.table(repartitioned_view)
