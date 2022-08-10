import abc
import logging
from typing import Any, Union

import pyspark.sql.functions as F
from pydantic import Field, validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


logger = logging.getLogger(__name__)


class DimensionFilterBaseModel(DSGBaseModel, abc.ABC):

    dimension_type: DimensionType
    dimension_query_name: str

    @abc.abstractmethod
    def apply_filter(self, df):
        """Apply the filter to a DataFrame"""

    @staticmethod
    def _make_value_str(value):
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)

    @staticmethod
    def _make_values_str(values):
        if isinstance(values[0], str):
            text = ", ".join((f"'{x}'" for x in values))
        elif isinstance(values[0], int) or isinstance(values[0], float):
            text = ", ".join((f"{x}" for x in values))
        else:
            raise Exception(f"Unsupported type: {type(values[0])}")  # TODO: dsg exception

        return f"({text})"


class _DimensionFilterWithWhereClauseModel(DimensionFilterBaseModel, abc.ABC):
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


class DimensionFilterValueModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column has a specific value.

    Example:
        DimensionFilterValueModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_query_name="county",
            value="06037",
        ),
    is equivalent to
        df.filter("county == '06037'")

    """

    value: Any

    def where_clause(self, column=None):
        if column is None:
            column = self.dimension_type.value
        value = self._make_value_str(self.value)
        text = f"({column} == {value})"
        return text


class DimensionFilterExpressionModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column matches an expression.
    Builds the filter string based on inferred types.

    Example:
        DimensionFilterExpressionModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_query_name="county",
            operator="=="
            value="06037",
        ),
    is equivalent to
        df.filter("county == '06037'")

    """

    operator: str
    value: Union[str, int, float]

    def where_clause(self, column=None):
        if column is None:
            column = self.dimension_type.value
        value = self._make_value_str(self.value)
        text = f"({column} {self.operator} {value})"
        return text


class DimensionFilterExpressionRawModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column matches an expression.
    Uses the passed string with no modification.

    Example:
        DimensionFilterExpressionModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_query_name="county",
            value="== '06037'",
        ),
    is equivalent to
        df.filter("county == '06037'")

    The difference between this class and DimensionFilterExpressionModel is that the latter
    will attempt to add quotes as necessary.

    """

    value: Union[str, int, float]

    def where_clause(self, column=None):
        if column is None:
            column = self.dimension_type.value
        text = f"({column} {self.value})"
        return text


DIMENSION_COLUMN_FILTER_OPERATORS = {
    "between",
    "contains",
    "endswith",
    "isNotNull",
    "isNull",
    "isin",
    "like",
    "rlike",
    "startswith",
}


class DimensionFilterColumnOperatorModel(DimensionFilterBaseModel):
    """Filters a table where a dimension column matches a Spark SQL operator.

    Examples:
    import pyspark.sql.functions as F
    df.filter(F.col("geography").like("abc%"))
    df.filter(~F.col("sector").startswith("com"))
    df.filter(F.col("timestamp").between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
    """

    value: Any = Field(title="value", description="Value to filter on")
    operator: str = Field(
        title="operator", description="Method on pyspark.sql.functions.col to invoke"
    )
    negate: bool = Field(
        title="negate",
        description="Change the filter to match the negation of the value.",
        default=False,
    )

    @validator("operator")
    def check_operator(cls, operator):
        if operator not in DIMENSION_COLUMN_FILTER_OPERATORS:
            raise ValueError(
                f"operator={operator} is not supported. Allowed={DIMENSION_COLUMN_FILTER_OPERATORS}"
            )
        return operator

    def apply_filter(self, df, column=None):
        if column is None:
            column = self._dimension_type.value
        col = F.col(column)
        method = getattr(col, self.operator)
        if self.negate:
            return df.filter(~method(self.value))
        return df.filter(method(self.value))
