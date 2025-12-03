from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class ValueFormat(StrEnum):
    """Defines the format of value columns in a dataset."""

    PIVOTED = "pivoted"
    STACKED = "stacked"


class TableFormat(StrEnum):
    """Defines the table structure of a dataset."""

    ONE_TABLE = "one_table"
    TWO_TABLE = "two_table"


# Keep old name as alias for backward compatibility during migration
TableFormatType = ValueFormat


class PivotedTableFormatModel(DSGBaseModel):
    """Defines a pivoted table format where one dimension's records are columns."""

    format_type: Literal[ValueFormat.PIVOTED] = ValueFormat.PIVOTED
    pivoted_dimension_type: DimensionType = Field(
        title="pivoted_dimension_type",
        description="The dimension type whose records are columns that contain data values.",
    )


class StackedTableFormatModel(DSGBaseModel):
    """Defines a stacked (unpivoted) table format with a single value column."""

    format_type: Literal[ValueFormat.STACKED] = ValueFormat.STACKED


# Alias for backward compatibility
UnpivotedTableFormatModel = StackedTableFormatModel


TableFormatModel = Annotated[
    Union[PivotedTableFormatModel, StackedTableFormatModel],
    Field(discriminator="format_type"),
]
