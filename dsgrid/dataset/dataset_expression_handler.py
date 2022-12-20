import operator

from pyspark.sql import DataFrame

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: DataFrame, dimension_columns: list[str], pivoted_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.pivoted_columns = pivoted_columns

    def _op(self, other, op):
        orig_self_count = self.df.count()
        orig_other_count = other.df.count()
        if orig_self_count != orig_other_count:
            raise DSGInvalidOperation(
                f"{op=} requires that the datasets have the same length "
                f"{orig_self_count=} {orig_other_count=}"
            )

        expr = [op(self.df[x], other.df[x]).alias(x) for x in self.pivoted_columns]
        df = self.df.join(other.df, on=self.dimension_columns).select(
            *self.dimension_columns, *expr
        )
        joined_count = df.count()
        if joined_count != orig_self_count:
            raise DSGInvalidOperation(
                f"join for operation {op=} has a different row count than the original. "
                f"{orig_self_count=} {joined_count=}"
            )

        return DatasetExpressionHandler(df, self.dimension_columns, self.pivoted_columns)

    def __add__(self, other):
        return self._op(other, operator.add)

    def __mul__(self, other):
        return self._op(other, operator.mul)

    def __sub__(self, other):
        return self._op(other, operator.sub)

    def __or__(self, other):
        if self.df.columns != other.df.columns:
            raise DSGInvalidOperation(
                "Union is only allowed when datasets have identical columns: "
                f"{self.df.columns=} vs {other.df.columns=}"
            )
        return DatasetExpressionHandler(
            self.df.union(other.df), self.dimension_columns, self.pivoted_columns
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
