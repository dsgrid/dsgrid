"""Compatibility helpers for table operations during the Ibis migration."""

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, cast
from zoneinfo import ZoneInfo

import ibis

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.io import read_csv
from dsgrid.ibis.operations import (
    aggregate_single_value,
    coalesce,
    count_distinct_on_group_by,
    create_temp_view,
    cross_join,
    except_all,
    filter_sql,
    handle_column_spaces,
    intersect,
    join,
    join_multiple_columns,
    make_temp_view_name,
    pivot,
    sql_from_df,
    unpivot,
)
from dsgrid.ibis.temp import drop_temp_tables_and_views
from dsgrid.ibis.types import is_table_empty, use_duckdb
from dsgrid.ibis.session import (
    get_current_time_zone,
    init_runtime_session,
    get_runtime_session,
    set_current_time_zone,
)

__all__ = [
    "aggregate",
    "aggregate_single_value",
    "cache",
    "coalesce",
    "collect_list",
    "count_distinct_on_group_by",
    "create_temp_view",
    "cross_join",
    "drop_temp_tables_and_views",
    "except_all",
    "filter_sql",
    "get_current_time_zone",
    "handle_column_spaces",
    "init_runtime_session",
    "intersect",
    "is_dataframe_empty",
    "join",
    "join_multiple_columns",
    "make_temp_view_name",
    "perform_interval_op",
    "pivot",
    "prepare_timestamps_for_dataframe",
    "read_csv",
    "select_expr",
    "set_current_time_zone",
    "sql_from_df",
    "unpersist",
    "unpivot",
    "write_csv",
]


def aggregate(df: ibis.Table, agg_func: str, column: str, alias: str) -> ibis.Table:
    value = getattr(df[column], agg_func)()
    return df.aggregate(**{alias: value})


def cache(df: ibis.Table) -> ibis.Table:
    """Materialize and cache a table for repeated reads.

    On DuckDB this is a no-op (queries execute against in-memory data
    already). On Spark, the returned table is an Ibis CachedTable whose
    cached data lives until ``unpersist`` is called or the reference is
    garbage collected. Callers must rebind: ``df = cache(df)``.
    """
    if use_duckdb():
        return df
    return df.cache()


def unpersist(df: ibis.Table) -> None:
    """Release a cached table previously returned by :func:`cache`.

    Safe to call on tables that were never cached — the call is a no-op
    when the input has no ``release`` method (e.g. when running on DuckDB).
    """
    release = getattr(df, "release", None)
    if release is not None:
        release()


def collect_list(df: ibis.Table, column: str) -> list:
    return df.select(column).execute()[column].tolist()


def is_dataframe_empty(df: ibis.Table) -> bool:
    return is_table_empty(df)


def perform_interval_op(
    df: ibis.Table, time_column: str, op: str, val: Any, unit: str, alias: str
) -> ibis.Table:
    view = create_temp_view(df)
    cols = df.columns[:]
    if alias == time_column:
        cols.remove(time_column)
    cols_str = ",".join([handle_column_spaces(x) for x in cols])
    time_col = handle_column_spaces(time_column)
    expr = f"{time_col} {op} INTERVAL {val} {unit}"
    query = f"SELECT {expr} AS {alias}, {cols_str} from {view}"
    return get_runtime_session().sql(query)


def _get_local_time_zone_name() -> str:
    path = Path("/etc/localtime").resolve()
    marker = "zoneinfo/"
    path_str = path.as_posix()
    if marker in path_str:
        return path_str.split(marker, maxsplit=1)[1]
    return "UTC"


def prepare_timestamps_for_dataframe(timestamps: Iterable[datetime]) -> Iterable[datetime]:
    if use_duckdb():
        return [x.astimezone(ZoneInfo("UTC")) for x in timestamps]
    return timestamps


def select_expr(df: ibis.Table, exprs: list[str]) -> ibis.Table:
    view = create_temp_view(df)
    cols = ",".join(exprs)
    return get_runtime_session().sql(f"SELECT {cols} FROM {view}")


def write_csv(
    df: ibis.Table, path: Path | str, header: bool = True, overwrite: bool = False
) -> None:
    path_str = path if isinstance(path, str) else str(path)
    path_obj = Path(path_str)
    if path_obj.exists():
        if overwrite:
            path_obj.unlink()
        else:
            raise FileExistsError(path_str)
    if dsgrid.runtime_config.backend_engine == BackendEngine.SPARK:
        df.to_pandas().to_csv(path_str, index=False, header=header)
    else:
        view = create_temp_view(df)
        escaped_path = path_str.replace("'", "''")
        header_arg = "true" if header else "false"
        conn = cast(Any, make_runtime_backend().connection)
        conn.raw_sql(
            f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT CSV, HEADER {header_arg})"
        )
