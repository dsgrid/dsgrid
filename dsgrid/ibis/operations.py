from typing import Any, cast
from tempfile import NamedTemporaryFile

import ibis

from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.temp import make_temp_view_name
from dsgrid.ibis.types import use_duckdb


def create_temp_view(df: ibis.Table) -> str:
    view = make_temp_view_name()
    try:
        make_runtime_backend().create_view(view, df)
    except Exception:
        tmp_file = NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_file.close()
        df.to_parquet(tmp_file.name)
        escaped_path = tmp_file.name.replace("'", "''")
        conn = cast(Any, make_runtime_backend().connection)
        conn.raw_sql(f"CREATE TEMP VIEW {view} AS SELECT * FROM read_parquet('{escaped_path}')")
    return view


def cross_join(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        return _sql_on_df(df1, f"SELECT * from {view1} CROSS JOIN {view2}")
    return df1.cross_join(df2)


def coalesce(df: ibis.Table, num_partitions: int) -> ibis.Table:
    return df


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


def except_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        query = f"""
            SELECT * FROM {view1}
            EXCEPT ALL
            SELECT * FROM {view2}
        """
        return _sql_on_df(df1, query)
    return df1.difference(df2, distinct=False)


def intersect(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        query = f"""
            SELECT * FROM {view1}
            INTERSECT
            SELECT * FROM {view2}
        """
        return _sql_on_df(df1, query)
    return df1.intersect(df2)


def union_all(df1: ibis.Table, df2: ibis.Table) -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        query = f"""
            SELECT * FROM {view1}
            UNION ALL
            SELECT * FROM {view2}
        """
        return _sql_on_df(df1, query)
    return df1.union(df2, distinct=False)


def count_distinct_on_group_by(
    df: ibis.Table, group_by_columns: list[str], agg_column: str, alias: str
) -> ibis.Table:
    if not use_duckdb():
        return df.group_by(group_by_columns).aggregate(**{alias: df[agg_column].nunique()})
    view = create_temp_view(df)
    cols = ",".join([f'"{x}"' for x in group_by_columns])
    query = f"""
        SELECT {cols}, COUNT(DISTINCT "{agg_column}") AS "{alias}"
        FROM {view}
        GROUP BY {cols}
    """
    return _sql_on_df(df, query)


def handle_column_spaces(column: str) -> str:
    if use_duckdb():
        return f'"{column}"'
    return f"`{column}`"


def join(df1: ibis.Table, df2: ibis.Table, column1: str, column2: str, how="inner") -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        view2_columns = ",".join((f'{view2}."{x}"' for x in df2.columns if x not in df1.columns))
        select_columns = f"{view1}.*"
        if view2_columns:
            select_columns += f", {view2_columns}"
        query = f"""
            SELECT {select_columns}
            FROM {view1}
            {how} JOIN {view2}
            ON {view1}."{column1}" = {view2}."{column2}"
        """
        return _sql_on_df(df1, query)
    return df1.join(df2, df1[column1] == df2[column2], how=how)


def join_multiple_columns(
    df1: ibis.Table, df2: ibis.Table, columns: list[str], how="inner"
) -> ibis.Table:
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        view2_columns = ",".join((f'{view2}."{x}"' for x in df2.columns if x not in df1.columns))
        select_columns = f"{view1}.*"
        if view2_columns:
            select_columns += f", {view2_columns}"
        on_str = " AND ".join((f'{view1}."{x}" = {view2}."{x}"' for x in columns))
        query = f"""
            SELECT {select_columns}
            FROM {view1}
            {how} JOIN {view2}
            ON {on_str}
        """
        return _sql_on_df(df1, query)
    return df1.join(df2, columns, how=how)


def sql_from_df(df: ibis.Table, query: str) -> ibis.Table:
    view = create_temp_view(df)
    return _sql_on_df(df, query + f" FROM {view}")


def pivot(df: ibis.Table, name_column: str, value_column: str) -> ibis.Table:
    if use_duckdb():
        view = create_temp_view(df)
        query = f"""
            SELECT * FROM (
                PIVOT {view}
                ON "{name_column}"
                USING SUM({value_column})
            )
        """
        return _sql_on_df(df, query)
    return df.pivot_wider(
        names_from=name_column,
        values_from=value_column,
        values_agg="sum",
    )


def unpivot(df: ibis.Table, pivoted_columns, name_column: str, value_column: str) -> ibis.Table:
    if use_duckdb():
        view = create_temp_view(df)
        cols = ",".join([f'"{x}"' for x in pivoted_columns])
        query = f"""
            SELECT * FROM {view}
            UNPIVOT INCLUDE NULLS (
                "{value_column}"
                FOR "{name_column}" in ({cols})
            )
        """
        return _sql_on_df(df, query)
    return df.pivot_longer(pivoted_columns, names_to=name_column, values_to=value_column)


def aggregate_single_value(df: ibis.Table, agg_func: str, column: str) -> Any:
    return getattr(df[column], agg_func)().execute()


def _get_runtime_session():
    from dsgrid.ibis.session import get_runtime_session

    return get_runtime_session()


def _sql_on_df(df: ibis.Table, query: str) -> ibis.Table:
    return make_runtime_backend().sql(query)
