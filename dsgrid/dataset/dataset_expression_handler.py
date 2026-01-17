import operator

import ibis.expr.types as ir
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(
        self,
        table: ir.Table,
        dimension_columns: list[str],
        value_columns: list[str],
    ):
        self.table = table
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    @property
    def df(self) -> ir.Table:
        """Return the underlying Ibis table."""
        return self.table

    def _op(self, other, op):
        orig_self_count = self.table.count().to_pyarrow().as_py()
        orig_other_count = other.table.count().to_pyarrow().as_py()
        if orig_self_count != orig_other_count:
            msg = (
                f"{op=} requires that the datasets have the same length "
                f"{orig_self_count=} {orig_other_count=}"
            )
            raise DSGInvalidOperation(msg)

        def renamed(col):
            return col + "_other"

        other_table = other.table
        rename_map = {renamed(col): col for col in self.value_columns}
        other_table = other_table.rename(rename_map)

        joined = self.table.join(other_table, self.dimension_columns)

        mutations = {}
        for column in self.value_columns:
            other_column = renamed(column)
            mutations[column] = op(joined[column], joined[other_column])

        joined = joined.mutate(**mutations)
        joined = joined.select(self.table.columns)

        joined_count = joined.count().to_pyarrow().as_py()
        if joined_count != orig_self_count:
            msg = (
                f"join for operation {op=} has a different row count than the original. "
                f"{orig_self_count=} {joined_count=}"
            )
            raise DSGInvalidOperation(msg)

        return DatasetExpressionHandler(joined, self.dimension_columns, self.value_columns)

    def __add__(self, other):
        return self._op(other, operator.add)

    def __mul__(self, other):
        return self._op(other, operator.mul)

    def __sub__(self, other):
        return self._op(other, operator.sub)

    def __or__(self, other):
        if self.table.columns != other.table.columns:
            msg = (
                "Union is only allowed when datasets have identical columns: "
                f"{self.table.columns=} vs {other.table.columns=}"
            )
            raise DSGInvalidOperation(msg)
        joined = self.table.union(other.table)
        try:
            c1 = self.table.count().to_pyarrow().as_py()
            c2 = other.table.count().to_pyarrow().as_py()
            c_joined = joined.count().to_pyarrow().as_py()
            print(f"DEBUG: Union: {c1} + {c2} -> {c_joined}")
        except Exception as e:
            print(f"DEBUG: Union count failed: {e}")
        return DatasetExpressionHandler(joined, self.dimension_columns, self.value_columns)


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
