import operator

import ibis

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.operations import join_multiple_columns, rename_columns
from dsgrid.ibis.table_utils import count_rows, is_table_empty
from dsgrid.utils.py_expression_eval import Parser


class DatasetExpressionHandler:
    """Abstracts SQL expressions for dataset combinations with mathematical expressions."""

    def __init__(self, df: ibis.Table, dimension_columns: list[str], value_columns: list[str]):
        self.df = df
        self.dimension_columns = dimension_columns
        self.value_columns = value_columns

    def _op(self, other, op):
        # Fail with a clear domain error (rather than an opaque Ibis
        # rename/join error) when the operands are not column-compatible.
        # The join renames other's value columns and joins on the
        # dimension columns, so both sets must match on each side.
        # Order is irrelevant: the result is rebuilt via
        # ``.select(*self.df.columns)`` below, so compare as sets.
        if (
            set(self.df.columns) != set(other.df.columns)
            or set(self.dimension_columns) != set(other.dimension_columns)
            or set(self.value_columns) != set(other.value_columns)
        ):
            msg = (
                f"Arithmetic between datasets requires identical columns: "
                f"{self.df.columns=} vs {other.df.columns=}, "
                f"{self.dimension_columns=} vs {other.dimension_columns=}, "
                f"{self.value_columns=} vs {other.value_columns=}"
            )
            raise DSGInvalidOperation(msg)

        # Soundness check has two parts:
        #
        # 1. Anti-joins on the dimension columns — both must be empty —
        #    prove that the key sets match. This catches cases that counts
        #    alone miss, e.g. ``self={A,B}``, ``other={A,A}``: every count
        #    is 2 but B is silently dropped; the ``self.anti_join(other)``
        #    is non-empty for B and triggers the error.
        #
        # 2. ``count(self) == count(other) == count(self.distinct(dim))``
        #    proves there are no duplicate dimension-key rows on either
        #    side. Given key sets match (from #1), the distinct key counts
        #    are equal across sides, so checking distinct against self
        #    implies the same for other by transitivity. Catches cases like
        #    ``self={A,A}``, ``other={A}``: counts differ, fails. And
        #    ``self={A,A}``, ``other={A,A}``: counts equal each other but
        #    differ from distinct (which is 1), fails.
        #
        # Cost: this is not cheap. The two anti-joins are each a full
        # join over both datasets (the ``is_table_empty`` LIMIT 1 only
        # short-circuits scanning, not the join), and the three counts
        # (self, other, self.distinct) are three more full passes over
        # the inputs. None of the five evaluates the inner join itself,
        # so the caller's downstream consumption of ``df`` pays for the
        # join only once.
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
        other_count = count_rows(other.df)
        self_distinct_count = count_rows(self_keys.distinct())
        if self_count != other_count or self_count != self_distinct_count:
            msg = (
                f"join for operation {op=} would produce duplicate output rows: "
                "at least one of the inputs has duplicate dimension-key rows "
                "(often from a prior union of overlapping datasets). "
                f"{self_count=} {other_count=} {self_distinct_count=}"
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
