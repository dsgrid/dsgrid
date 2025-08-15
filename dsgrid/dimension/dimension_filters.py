import abc
import logging
from enum import Enum
from typing import Any, Union, Literal

from pydantic import field_validator, model_validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter
from dsgrid.spark.types import DataFrame, F


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
    def apply_filter(self, df, column=None):
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
    def apply_filter(self, df, column=None):
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
        df.filter("county == '06037'")

    """

    operator: str
    value: Union[str, int, float]
    filter_type: Literal[DimensionFilterType.EXPRESSION] = DimensionFilterType.EXPRESSION

    def where_clause(self, column=None):
        column = column or self.column
        value = self._make_value_str(self.value)
        text = f"({column} {self.operator} {value})"
        return text


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
    """Filters a table where a dimension column matches a Spark SQL operator.

    Examples:
    import pyspark.sql.functions as F
    df.filter(F.col("geography").like("abc%"))
    df.filter(~F.col("sector").startswith("com"))
    """

    operator: str = Field(
        title="operator", description="Method on pyspark.sql.functions.col to invoke"
    )
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

    def apply_filter(self, df, column=None):
        column = column or self.column
        col = F.col(column)
        method = getattr(col, self.operator)
        if self.negate:
            return df.filter(~method(self.value))
        return df.filter(method(self.value))


class DimensionFilterBetweenColumnOperatorModel(DimensionFilterSingleQueryNameBaseModel):
    """Filters a table where a dimension column is between the lower bound and upper bound,
    inclusive.

    Examples:
    import pyspark.sql.functions as F
    df.filter(F.col("timestamp").between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
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

    def apply_filter(self, df, column=None):
        column = column or self.column
        if self.negate:
            return df.filter(~F.col(column).between(self.lower_bound, self.upper_bound))
        return df.filter(F.col(column).between(self.lower_bound, self.upper_bound))


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

    def apply_filter(self, df, column=None):
        msg = f"apply_filter must not be called on {self.__class__.__name__}"
        raise NotImplementedError(msg)

    def get_filtered_records_dataframe(self, dimension_accessor) -> DataFrame:
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
                if records.columns != df.columns:
                    msg = (
                        f"Mismatch in records columns for {self}: "
                        f"{df.columns} != {records.columns}"
                    )
                    raise DSGInvalidParameter(msg)
                df = df.union(records)

        assert df is not None
        return df


class SupplementalDimensionFilterColumnOperatorModel(DimensionFilterSingleQueryNameBaseModel):
    """Filters base dimension records that have a valid mapping to a supplemental dimension."""

    value: Any = Field(title="value", description="Value to filter on", default="%")
    operator: str = Field(
        title="operator",
        description="Method on pyspark.sql.functions.col to invoke",
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

    def apply_filter(self, df, column=None):
        column = column or self.column
        col = F.col(column)
        method = getattr(col, self.operator)
        if self.negate:
            return df.filter(~method(self.value))
        return df.filter(method(self.value))
