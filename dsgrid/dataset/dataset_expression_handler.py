import operator
from typing import Any, cast

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.operations import join_multiple_columns, rename_columns
from dsgrid.utils.py_expression_eval import Parser


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
                f"{op=} requires that the datasets have the same length "
                f"{orig_self_count=} {orig_other_count=}"
            )
            raise DSGInvalidOperation(msg)

        renamed_value_cols = {col: f"{col}__other" for col in self.value_columns}
        other_df = rename_columns(other.df, renamed_value_cols)
        joined = join_multiple_columns(self.df, other_df, self.dimension_columns)
        mutations = {
            col: op(joined[col], joined[renamed_value_cols[col]]) for col in self.value_columns
        }
        df = joined.mutate(**mutations).select(*self.df.columns)

        joined_count = _count_rows(df)
        if joined_count != orig_self_count:
            msg = (
                f"join for operation {op=} has a different row count than the original. "
                f"{orig_self_count=} {joined_count=}"
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
                f"{self.df.columns=} vs {other.df.columns=}"
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


def _count_rows(df: ibis.Table) -> int:
    count = df.count().execute()
    return int(cast(Any, count))
