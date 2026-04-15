import operator
from typing import Any, cast

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.operations import create_temp_view, handle_column_spaces
from dsgrid.utils.py_expression_eval import Parser
from dsgrid.ibis.session import get_runtime_session


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: ibis.Table, dimension_columns: list[str], value_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    def _op(self, other, op):
        orig_self_count = _count_rows(self.df)
        orig_other_count = _count_rows(other.df)
        if orig_self_count != orig_other_count:
            msg = (
                f"{op =} requires that the datasets have the same length "
                f"{orig_self_count =} {orig_other_count =}"
            )
            raise DSGInvalidOperation(msg)

        df = _apply_op_with_sql(self.df, other.df, self.dimension_columns, self.value_columns, op)

        joined_count = _count_rows(df)
        if joined_count != orig_self_count:
            msg = (
                f"join for operation {op =} has a different row count than the original. "
                f"{orig_self_count =} {joined_count =}"
            )
            raise DSGInvalidOperation(msg)

        return DatasetExpressionHandler(df, self.dimension_columns, self.value_columns)

    def __add__(self, other):
        return self._op(other, operator.add)

    def __mul__(self, other):
        return self._op(other, operator.mul)

    def __sub__(self, other):
        return self._op(other, operator.sub)

    def __or__(self, other):
        if self.df.columns != other.df.columns:
            msg = (
                "Union is only allowed when datasets have identical columns: "
                f"{self.df.columns =} vs {other.df.columns =}"
            )
            raise DSGInvalidOperation(msg)
        return DatasetExpressionHandler(
            self.df.union(other.df), self.dimension_columns, self.value_columns
        )


def evaluate_expression(expr: str, dataset_mapping: dict[str, DatasetExpressionHandler]):
    """Evaluates an expresion containing dataset IDs.

    Parameters
    ----------
    expr : str
        Dataset combination expression, such as "dataset1 | dataset2"
    dataset_mapping : dict[str, DatasetExpressionHandler]
        Maps dataset ID to dataset. Each dataset_id in expr must be present in the mapping.

    Returns
    -------
    DatasetExpressionHandler

    """
    return Parser().parse(expr).evaluate(dataset_mapping)


def join_multiple_columns(df1: ibis.Table, df2: ibis.Table, columns: list[str], how="inner"):
    view1 = _create_temp_view(df1)
    view2 = _create_temp_view(df2)
    view2_columns = ",".join(
        (f"{view2}.{handle_column_spaces(x)}" for x in df2.columns if x not in df1.columns)
    )
    select_columns = f"{view1}.*"
    if view2_columns:
        select_columns += f", {view2_columns}"
    on_str = " AND ".join(
        (f"{view1}.{handle_column_spaces(x)} = {view2}.{handle_column_spaces(x)}" for x in columns)
    )
    query = f"""
        SELECT {select_columns}
        FROM {view1}
        {how} JOIN {view2}
        ON {on_str}
    """
    return get_runtime_session().sql(query)


def _create_temp_view(df: ibis.Table) -> str:
    return create_temp_view(df)


def _apply_op_with_sql(
    df1: ibis.Table,
    df2: ibis.Table,
    dimension_columns: list[str],
    value_columns: list[str],
    op,
) -> ibis.Table:
    view1 = _create_temp_view(df1)
    view2 = _create_temp_view(df2)
    op_str = _operator_to_sql(op)
    value_column_set = set(value_columns)
    select_columns = []
    for column in df1.columns:
        quoted = handle_column_spaces(column)
        if column in value_column_set:
            select_columns.append(f"{view1}.{quoted} {op_str} {view2}.{quoted} AS {quoted}")
        else:
            select_columns.append(f"{view1}.{quoted}")
    on_str = " AND ".join(
        (
            f"{view1}.{handle_column_spaces(x)} = {view2}.{handle_column_spaces(x)}"
            for x in dimension_columns
        )
    )
    query = f"""
        SELECT {", ".join(select_columns)}
        FROM {view1}
        INNER JOIN {view2}
        ON {on_str}
    """
    return make_runtime_backend().sql(query)


def _operator_to_sql(op) -> str:
    if op is operator.add:
        return "+"
    if op is operator.mul:
        return "*"
    if op is operator.sub:
        return "-"
    msg = f"Unsupported operator: {op}"
    raise NotImplementedError(msg)


def _count_rows(df: ibis.Table) -> int:
    count = df.count().execute()
    return int(cast(Any, count))
