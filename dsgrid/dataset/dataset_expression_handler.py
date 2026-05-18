import operator

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.operations import join_multiple_columns, rename_columns
from dsgrid.ibis.table_utils import count_rows
from dsgrid.ibis.types import is_table_empty
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: ibis.Table, dimension_columns: list[str], value_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    def _op(self, other, op):
        # Soundness check has two parts: anti-joins for key-set equality,
        # then ``count(joined) == count(self)`` for duplicate detection.
        #
        # Anti-joins (``a.anti_join(b, keys)`` = rows of a whose keys are
        # not in b) cover all missing-key cases that pure counts miss. For
        # example: ``self={A,B}``, ``other={A,A}``. Counts of self, other,
        # and joined all equal 2, but B was silently dropped. The
        # ``self.anti_join(other)`` is non-empty for B and catches it.
        # ``limit(1)`` via ``is_table_empty`` lets the planner short-circuit
        # on the first non-matching key.
        #
        # Once we know key sets match, ``count(joined) == count(self)``
        # implies no duplicate keys on either side (and by transitivity
        # ``== count(other)``). If self had a duplicate key K, K's row
        # multiplies in the inner join and ``count(joined) > count(self)``;
        # same on the other side. This catches the ``test_invalid_lengths``
        # case where one operand was previously unioned.
        #
        # Cost: 2 anti-joins (cheap, short-circuit) + 2 counts. The joined
        # count requires evaluating the inner join — which the caller will
        # evaluate again when consuming ``df`` (Ibis lazy chain). Spark
        # callers chaining operators should wrap in
        # :func:`~dsgrid.ibis.functions.cache` to avoid the re-execution;
        # DuckDB's planner generally reuses the scan.
        renamed_value_cols = {col: f"{col}__other" for col in self.value_columns}
        other_df = rename_columns(other.df, renamed_value_cols)
        joined = join_multiple_columns(self.df, other_df, self.dimension_columns)
        mutations = {
            col: op(joined[col], joined[renamed_value_cols[col]]) for col in self.value_columns
        }
        df = joined.mutate(**mutations).select(*self.df.columns)

        self_keys = self.df.select(*self.dimension_columns)
        other_keys = other.df.select(*self.dimension_columns)
        left_has_extra = not is_table_empty(
            self_keys.anti_join(other_keys, self.dimension_columns)
        )
        right_has_extra = not is_table_empty(
            other_keys.anti_join(self_keys, self.dimension_columns)
        )
        if left_has_extra or right_has_extra:
            msg = (
                f"join for operation {op=} would drop rows: the datasets have "
                f"mismatched dimension coverage "
                f"(left_has_extra={left_has_extra} right_has_extra={right_has_extra}). "
                "Arithmetic between datasets requires the same set of dimension "
                "records on both sides."
            )
            raise DSGInvalidOperation(msg)

        self_count = count_rows(self.df)
        joined_count = count_rows(df)
        if joined_count != self_count:
            msg = (
                f"join for operation {op=} multiplied rows: {self_count=} but "
                f"{joined_count=}. One of the inputs has duplicate dimension-key "
                "rows (often from a prior union of overlapping datasets); "
                "arithmetic between datasets requires unique keys on each side."
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


