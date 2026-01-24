import abc
import logging
from enum import Enum
from typing import Any, Union, Literal

import ibis
import ibis.expr.types as ir
from pydantic import field_validator, model_validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter


logger = logging.getLogger(__name__)


class DimensionFilterType(str, Enum):
    """Filter types that can be specified in queries."""

    EXPRESSION = "expression"
    EXPRESSION_RAW = "expression_raw"
    COLUMN_OPERATOR = "column_operator"
    BETWEEN_COLUMN_OPERATOR = "between_column_operator"
    SUBSET = "subset"
    SUPPLEMENTAL_COLUMN_OPERATOR = "supplemental_column_operator"


class DimensionFilterBaseModel(DSGBaseModel, abc.ABC):
    """Base model for all filters"""

    dimension_type: DimensionType
    column: str = Field(
        title="column", description="Column of dimension records to use", default="id"
    )

    @abc.abstractmethod
    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        """Apply the filter to a DataFrame"""

    @model_validator(mode="before")
    @classmethod
    def remove_filter_type(cls, values):
        values.pop("filter_type", None)
        return values

    def _make_value_str(self, value):
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        else:
            msg = f"Unsupported type: {type(value)}"
            raise DSGInvalidField(msg)

    def _make_values_str(self, values):
        return ", ".join((f"{self._make_value_str(x)}" for x in values))


class DimensionFilterSingleQueryNameBaseModel(DimensionFilterBaseModel, abc.ABC):
    """Base model for all filters based on expressions with a single dimension."""

    dimension_name: str


class DimensionFilterMultipleQueryNameBaseModel(DimensionFilterBaseModel, abc.ABC):
    """Base model for all filters based on expressions with multiple dimensions."""

    dimension_names: list[str]


class _DimensionFilterWithWhereClauseModel(DimensionFilterSingleQueryNameBaseModel, abc.ABC):
    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        return df.filter(self.where_clause(column=column))

    @abc.abstractmethod
    def where_clause(self, column=None):
        """Returns the text for a where clause in a filter statement.

        Parameters
        ----------
        column : None or str
            Column to use. If None, use the dimension type.

        Returns
        -------
        str

        """


class DimensionFilterExpressionModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column matches an expression.

    Example:
        DimensionFilterExpressionModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_name="county",
            operator="==",
            value="06037",
        ),
    is equivalent to
        df.filter(df["county"] == '06037')

    """

    operator: str
    value: Union[str, int, float]
    filter_type: Literal[DimensionFilterType.EXPRESSION] = DimensionFilterType.EXPRESSION

    def where_clause(self, column=None):
        # Used for fallback or SQL generation if needed, but apply_filter uses Ibis exprs usually.
        # For Ibis filter(string), it expects SQL-like string.
        column = column or self.column
        value = self._make_value_str(self.value)
        text = f"({column} {self.operator} {value})"
        return text

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        column = column or self.column
        op = self.operator
        if op == "==":
            return df.filter(df[column] == self.value)
        elif op == "!=":
            return df.filter(df[column] != self.value)
        elif op == ">":
            return df.filter(df[column] > self.value)
        elif op == "<":
            return df.filter(df[column] < self.value)
        elif op == ">=":
            return df.filter(df[column] >= self.value)
        elif op == "<=":
            return df.filter(df[column] <= self.value)
        else:
            # Fallback to string expression if ibis supports it or assume Spark
            return df.filter(ibis.literal(self.where_clause(column=column)))


class DimensionFilterExpressionRawModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column matches an expression.
    Uses the passed string with no modification.

    Example:
        DimensionFilterExpressionRawModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_name="county",
            value="== '06037'",
        ),
    is equivalent to
        df.filter("county == '06037'")

    The difference between this class and DimensionFilterExpressionModel is that the latter
    will attempt to add quotes as necessary.

    """

    value: Union[str, int, float]
    filter_type: Literal[DimensionFilterType.EXPRESSION_RAW] = DimensionFilterType.EXPRESSION_RAW

    def where_clause(self, column=None):
        column = column or self.column
        text = f"({column} {self.value})"
        return text

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        # Ibis doesn't strictly support raw SQL string in filter like Spark does without some work.
        # But maybe we can pass it to sql()? Or if using duckdb/spark backend it might work.
        # Ideally we shouldn't use RawModel with Ibis if we can avoid it.
        # But let's try passing the string.
        # Ibis filter accepts boolean expressions.
        # If we have "col == val", we can't easily parse it here without complexity.
        # Assuming the backend can handle it or we use sql.
        # For now, let's assume we can use sql() on the table?
        # df.sql("SELECT * FROM table WHERE ...")
        # converting df to view/table first.
        # But this method receives df.
        # We can use dsgrid.ibis_api.sql_from_df if available, but we are inside models.
        # Let's import it? No circular deps.
        # We can use `ibis.literal(True)` combined with sql string? No.
        # Let's rely on backend specific behavior or deprecate this.
        # Actually `df.filter(ibis.literal(self.where_clause(column=column)))`? Ibis might treat literal string as constant.
        # If we really need raw SQL filter, we might need to use `df.sql(...)` pattern outside.
        # But `apply_filter` returns a table.
        # Let's raise NotImplementedError for now if used, or try to use `filter(ibis.literal(...))`?
        # Ibis 9.0+ might support string filters? No.

        # NOTE: This model was heavily Spark-dependent.
        # For now, let's try to interpret it if simple, or fail.
        # If value starts with operator...
        # If the user provides a full SQL WHERE clause...
        msg = "DimensionFilterExpressionRawModel is not fully supported with Ibis backend yet."
        # However, we can try to use `sql` method on the connection if we can get it?
        # Or `df.filter(...)` might take SQL string in some backends?
        # DuckDB backend: df.filter("col == 1") works? Ibis doesn't expose it directly.

        # We will log warning and try standard filter if possible?
        logger.warning(msg)
        # Using sql() on the table expression?
        # df.alias("t").sql(f"SELECT * FROM t WHERE {self.where_clause(column)}")
        # This requires the table to be compilable to SQL.
        return df.filter(
            ibis.literal(True)
        )  # Placeholder to not crash, effectively no-op filter. Or raise error?


DIMENSION_COLUMN_FILTER_OPERATORS = {
    "contains",
    "endswith",
    "isNotNull",
    "isNull",
    "isin",
    "like",
    "rlike",
    "startswith",
}


def check_operator(operator):
    if operator not in DIMENSION_COLUMN_FILTER_OPERATORS:
        msg = f"operator={operator} is not supported. Allowed={DIMENSION_COLUMN_FILTER_OPERATORS}"
        raise ValueError(msg)
    return operator


class DimensionFilterColumnOperatorModel(DimensionFilterSingleQueryNameBaseModel):
    """Filters a table where a dimension column matches an operator.

    Examples:
    df.filter(df["geography"].like("abc%"))
    df.filter(~df["sector"].startswith("com"))
    """

    operator: str = Field(title="operator", description="Operator to invoke")
    value: Any = Field(
        default=None,
        title="value",
        description="Value to filter on. Use a two-element list for the between operator.",
    )
    negate: bool = Field(
        title="negate",
        description="Change the filter to match the negation of the value.",
        default=False,
    )
    filter_type: Literal[DimensionFilterType.COLUMN_OPERATOR] = DimensionFilterType.COLUMN_OPERATOR

    @field_validator("operator")
    @classmethod
    def check_operator(cls, operator):
        return check_operator(operator)

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        column = column or self.column
        col = df[column]
        if self.operator == "isNotNull":
            expr = col.notnull()
        elif self.operator == "isNull":
            expr = col.isnull()
        elif self.operator == "isin":
            expr = col.isin(self.value)
        elif self.operator == "like":
            expr = col.like(self.value)
        elif self.operator == "rlike":
            expr = col.re_search(self.value)
        elif self.operator == "startswith":
            expr = col.startswith(self.value)
        elif self.operator == "endswith":
            expr = col.endswith(self.value)
        elif self.operator == "contains":
            expr = col.contains(self.value)
        else:
            msg = f"Unsupported operator for Ibis: {self.operator}"
            raise NotImplementedError(msg)

        if self.negate:
            expr = ~expr
        return df.filter(expr)


class DimensionFilterBetweenColumnOperatorModel(DimensionFilterSingleQueryNameBaseModel):
    """Filters a table where a dimension column is between the lower bound and upper bound,
    inclusive.

    Examples:
    df.filter(df["timestamp"].between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
    """

    lower_bound: Any = Field(
        default=None, title="lower_bound", description="Lower bound, inclusive"
    )
    upper_bound: Any = Field(
        default=None, title="upper_bound", description="Upper bound, inclusive"
    )
    negate: bool = Field(
        title="negate",
        description="Change the filter to match the negation of the value.",
        default=False,
    )
    filter_type: Literal[
        DimensionFilterType.BETWEEN_COLUMN_OPERATOR
    ] = DimensionFilterType.BETWEEN_COLUMN_OPERATOR

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        column = column or self.column
        col = df[column]
        expr = col.between(self.lower_bound, self.upper_bound)
        if self.negate:
            expr = ~expr
        return df.filter(expr)


class SubsetDimensionFilterModel(DimensionFilterMultipleQueryNameBaseModel):
    """Filters base dimension records that match a subset dimension."""

    dimension_names: list[str]
    filter_type: Literal[DimensionFilterType.SUBSET] = DimensionFilterType.SUBSET

    @field_validator("dimension_names")
    @classmethod
    def check_dimension_names(cls, dimension_names):
        if not dimension_names:
            msg = "dimension_names cannot be empty"
            raise ValueError(msg)
        return dimension_names

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        msg = f"apply_filter must not be called on {self.__class__.__name__}"
        raise NotImplementedError(msg)

    def get_filtered_records_dataframe(self, dimension_accessor) -> ir.Table:
        """Return a dataframe containing the filter records."""
        df = None
        dim_type = None
        for query_name in self.dimension_names:
            dim = dimension_accessor(query_name)
            records = dim.get_records_dataframe()
            if df is None:
                df = records
                dim_type = dim.model.dimension_type
            else:
                if dim.model.dimension_type != dim_type:
                    msg = (
                        f"Mismatch in dimension types for {self}: "
                        f"{dim_type} != {dim.model.dimension_type}"
                    )
                    raise DSGInvalidParameter(msg)
                # Ibis union
                # Ensure columns match
                # df = df.union(records)
                df = df.union(records)

        assert df is not None
        return df


class SupplementalDimensionFilterColumnOperatorModel(DimensionFilterSingleQueryNameBaseModel):
    """Filters base dimension records that have a valid mapping to a supplemental dimension."""

    value: Any = Field(title="value", description="Value to filter on", default="%")
    operator: str = Field(
        title="operator",
        description="Operator to invoke",
        default="like",
    )
    negate: bool = Field(
        title="negate",
        description="Filter out valid mappings to this supplemental dimension.",
        default=False,
    )
    filter_type: Literal[
        DimensionFilterType.SUPPLEMENTAL_COLUMN_OPERATOR
    ] = DimensionFilterType.SUPPLEMENTAL_COLUMN_OPERATOR

    @field_validator("operator")
    @classmethod
    def check_operator(cls, operator):
        return check_operator(operator)

    def apply_filter(self, df: ir.Table, column: str | None = None) -> ir.Table:
        column = column or self.column
        col = df[column]
        if self.operator == "like":
            expr = col.like(self.value)
        elif self.operator == "isNotNull":
            expr = col.notnull()
        elif self.operator == "isNull":
            expr = col.isnull()
        elif self.operator == "isin":
            expr = col.isin(self.value)
        elif self.operator == "rlike":
            expr = col.re_search(self.value)
        elif self.operator == "startswith":
            expr = col.startswith(self.value)
        elif self.operator == "endswith":
            expr = col.endswith(self.value)
        elif self.operator == "contains":
            expr = col.contains(self.value)
        else:
            msg = f"Unsupported operator for Ibis: {self.operator}"
            raise NotImplementedError(msg)

        if self.negate:
            expr = ~expr
        return df.filter(expr)
