import abc
import logging
from typing import Any, Dict, Union

import pyspark.sql.functions as F
from pydantic import Field, validator, root_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidField


logger = logging.getLogger(__name__)


class DimensionFilterBaseModel(DSGBaseModel, abc.ABC):
    """Base model for all filters"""

    dimension_type: DimensionType
    dimension_query_name: str
    column: str = Field(
        title="column", description="Column of dimension records to use", default="id"
    )

    @abc.abstractmethod
    def apply_filter(self, df):
        """Apply the filter to a DataFrame"""

    def preprocess(self, project_config):
        """Perform optional preprocessing activities with the ProjectConfig."""

    def _make_value_str(self, value):
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        else:
            raise DSGInvalidField(f"Unsupported type: {type(value)}")

    def _make_values_str(self, values):
        return ", ".join((f"{self._make_value_str(x)}" for x in values))

    def dict(self, *args, **kwargs):
        # Add the type of the class so that we can deserialize with the right model.
        data = super().dict(*args, **kwargs)
        data["filter_type"] = self.__class__.__name__
        return data

    @root_validator(pre=True)
    def remove_filter_type(cls, values):
        values.pop("filter_type", None)
        return values


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


class DimensionFilterExpressionModel(_DimensionFilterWithWhereClauseModel):
    """Filters a table where a dimension column matches an expression.

    Example:
        DimensionFilterExpressionModel(
            dimension_type=DimensionType.GEOGRAPHY,
            dimension_query_name="county",
            operator="==",
            value="06037",
        ),
    is equivalent to
        df.filter("county == '06037'")

    """

    operator: str
    value: Union[str, int, float]

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
        raise ValueError(
            f"operator={operator} is not supported. Allowed={DIMENSION_COLUMN_FILTER_OPERATORS}"
        )
    return operator


class DimensionFilterColumnOperatorModel(DimensionFilterBaseModel):
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
        title="value",
        description="Value to filter on. Use a two-element list for the between operator.",
    )
    negate: bool = Field(
        title="negate",
        description="Change the filter to match the negation of the value.",
        default=False,
    )

    @validator("operator")
    def check_operator(cls, operator):
        return check_operator(operator)

    def apply_filter(self, df, column=None):
        column = column or self.column
        col = F.col(column)
        method = getattr(col, self.operator)
        if self.negate:
            return df.filter(~method(self.value))
        return df.filter(method(self.value))


class DimensionFilterBetweenColumnOperatorModel(DimensionFilterBaseModel):
    """Filters a table where a dimension column is between the lower bound and upper bound,
    inclusive.

    Examples:
    import pyspark.sql.functions as F
    df.filter(F.col("timestamp").between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
    """

    lower_bound: Any = Field(title="lower_bound", description="Lower bound, inclusive")
    upper_bound: Any = Field(title="upper_bound", description="Upper bound, inclusive")
    negate: bool = Field(
        title="negate",
        description="Change the filter to match the negation of the value.",
        default=False,
    )

    def apply_filter(self, df, column=None):
        column = column or self.column
        if self.negate:
            return df.filter(~F.col(column).between(self.lower_bound, self.upper_bound))
        return df.filter(F.col(column).between(self.lower_bound, self.upper_bound))


class SupplementalDimensionFilterColumnOperatorModel(DimensionFilterBaseModel):
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

    @validator("operator")
    def check_operator(cls, operator):
        return check_operator(operator)

    def apply_filter(self, df, column=None):
        column = column or self.column
        col = F.col(column)
        method = getattr(col, self.operator)
        if self.negate:
            return df.filter(~method(self.value))
        return df.filter(method(self.value))


def _get_filter_subclasses(filter_class, subclasses=None):
    if subclasses is None:
        subclasses = {}
    for cls in filter_class.__subclasses__():
        subclasses[str(cls.__name__)] = cls
        if cls.__subclasses__():
            # Recurse.
            subclasses = _get_filter_subclasses(cls, subclasses)
    return subclasses


def make_dimension_filter(values: Dict):
    """Construct the correct filter per the key filter_type"""
    if values["filter_type"] not in _FILTER_SUBCLASSES:
        raise Exception(f"{values['filter_type']} is not defined in dimension_filters.py")
    return _FILTER_SUBCLASSES[values["filter_type"]](**values)


# Keep this definition at the end of the file.
_FILTER_SUBCLASSES = _get_filter_subclasses(DimensionFilterBaseModel)
