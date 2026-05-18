import operator

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.operations import join_multiple_columns, rename_columns
from dsgrid.ibis.table_utils import count_rows
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: ibis.Table, dimension_columns: list[str], value_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    def _op(self, other, op):
        # Soundness check: ``joined`` must have exactly the same row count
        # as BOTH inputs. This catches every failure mode the prior, cheaper
        # variants missed:
        #
        # - ``joined < self``: the right side is missing some of self's keys.
        # - ``joined < other``: the left side is missing some of other's keys
        #   (the variant that only compared joined to self silently dropped
        #   this case, per the post-Phase-14 Copilot review).
        # - ``joined > self`` or ``joined > other``: one side has duplicate
        #   dimension-key rows that multiply the inner join (e.g.
        #   ``dataset1 * (dataset1 | dataset2)`` — caught by the
        #   ``test_invalid_lengths`` regression).
        # - Same row counts on inputs but different key sets: the joined
        #   count drops below both inputs and trips the check.
        #
        # Cost: three counts per binary operator. Each source-table count is
        # a cheap reduction; the joined count requires evaluating the inner
        # join (which the caller will evaluate again when consuming ``df``).
        # For Spark callers concerned about repeated evaluation, wrap the
        # returned table in :func:`~dsgrid.ibis.functions.cache` before
        # chaining further operators. DuckDB's planner reuses the scan
        # across the count and downstream consumers, so the practical cost
        # there is one scan per source plus one join evaluation per op.
        renamed_value_cols = {col: f"{col}__other" for col in self.value_columns}
        other_df = rename_columns(other.df, renamed_value_cols)
        joined = join_multiple_columns(self.df, other_df, self.dimension_columns)
        mutations = {
            col: op(joined[col], joined[renamed_value_cols[col]]) for col in self.value_columns
        }
        df = joined.mutate(**mutations).select(*self.df.columns)

        self_count = count_rows(self.df)
        other_count = count_rows(other.df)
        joined_count = count_rows(df)
        if joined_count != self_count or joined_count != other_count:
            msg = (
                f"join for operation {op=} produced a row count that does not match "
                f"both inputs; the datasets likely have mismatched dimension keys or "
                f"duplicate keys on one side. {self_count=} {other_count=} {joined_count=}"
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


