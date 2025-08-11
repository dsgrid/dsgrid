import operator

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.spark.functions import join_multiple_columns
from dsgrid.spark.types import DataFrame
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: DataFrame, dimension_columns: list[str], value_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    def _op(self, other, op):
        orig_self_count = self.df.count()
        orig_other_count = other.df.count()
        if orig_self_count != orig_other_count:
            msg = (
                f"{op=} requires that the datasets have the same length "
                f"{orig_self_count=} {orig_other_count=}"
            )
            raise DSGInvalidOperation(msg)

        def renamed(col):
            return col + "_other"

        other_df = other.df
        for column in self.value_columns:
            other_df = other_df.withColumnRenamed(column, renamed(column))
        df = join_multiple_columns(self.df, other_df, self.dimension_columns)

        for column in self.value_columns:
            other_column = renamed(column)
            df = df.withColumn(column, op(getattr(df, column), getattr(df, other_column)))

        df = df.select(*self.df.columns)
        joined_count = df.count()
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
