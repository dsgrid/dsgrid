import abc
import logging

# from typing import Any, List, Union

# from dsgrid.data_models import DSGBaseModel
# from dsgrid.dimension.base_models import DimensionType


logger = logging.getLogger(__name__)


class DimensionFilterBase(abc.ABC):
    def __and__(self, other):
        return DimensionFilterCombo(self, other, "and").where_clause()

    def __or__(self, other):
        return DimensionFilterCombo(self, other, "or").where_clause()

    @abc.abstractmethod
    def apply_filter(self, df):
        """Apply the filter to a DataFrame"""

    @abc.abstractmethod
    def dimension_type(self):
        """Return the dimension type."""

    @abc.abstractmethod
    def query_name(self):
        """Return the query name."""

    @abc.abstractmethod
    def values(self):
        """Return the filter values.

        Returns
        -------
        list
        """

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


# class DimensionFilterValueModel(DSGBaseModel):

#     dimension_type: DimensionType
#     query_name: str
#     value: Any


class DimensionFilterValue(DimensionFilterBase):
    def __init__(self, dimension_type, query_name, value):
        self._dimension_type = dimension_type
        self._query_name = query_name
        self._value = value

    def __str__(self):
        # TODO DT: or __repr__?
        return f"DimensionFilterValue: {self.where_clause}"

    def apply_filter(self, df):
        pass

    @property
    def query_name(self):
        return self._query_name

    def where_clause(self, column=None):
        if column is None:
            column = self._dimension_type.value
        value = self._make_value_str(self._value)
        text = f"({column} == {value})"
        logger.debug(
            "dimension_type=%s query_name=%s query=%s",
            self._dimension_type,
            self._query_name,
            text,
        )
        return text
        # return f"({self._query_name} == {value})"

    @property
    def dimension_type(self):
        return self._dimension_type

    def values(self):
        return [self._value]


# class DimensionFilterListModel(DSGBaseModel):

#     dimension_type: DimensionType
#     query_name: str
#     values: List[Any]


class DimensionFilterList(DimensionFilterBase):
    def __init__(self, dimension_type, query_name, values):
        self._dimension_type = dimension_type
        self._query_name = query_name
        self._values = values

    def apply_filter(self, df):
        pass

    @property
    def query_name(self):
        return self._query_name

    def where_clause(self):
        values = self._make_values_str(self._values)
        # return f"({self._query_name} in {values})"
        text = f"({self._dimension_type.value} in {values})"
        logger.debug(
            "dimension_type=%s query_name=%s query=%s",
            self._dimension_type,
            self._query_name,
            text,
        )
        return text

    @property
    def dimension_type(self):
        return self._dimension_type

    def values(self):
        return self._values


# class DimensionFilterComboModel(DSGBaseModel):

#     first: Union[DimensionFilterValueModel, DimensionFilterListModel]
#     second: Union[DimensionFilterValueModel, DimensionFilterListModel]
#     operator: str


class DimensionFilterCombo:  # TODO: base type?
    def __init__(self, first, second, operator):
        self._first = first
        self._second = second
        self._operator = operator

    def where_clause(self):
        return f"({self._first.where_clause()} {self._operator} {self._second.where_clause()})"
