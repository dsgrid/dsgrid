from enum import Enum
from typing import Literal, Union

from pydantic import Field
from typing_extensions import Annotated

from dsgrid.common import VALUE_COLUMN
from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class TableFormatType(str, Enum):
    """Defines the format of value columns in a dataset."""

    PIVOTED = "pivoted"
    UNPIVOTED = "unpivoted"


class PivotedTableFormatModel(DSGBaseModel):
    format_type: Literal[TableFormatType.PIVOTED] = TableFormatType.PIVOTED
    pivoted_dimension_type: Annotated[
        DimensionType,
        Field(
            alias="load_data_column_dimension",  # TODO: remove when datasets have been converted
            title="pivoted_dimension_type",
            description="The data dimension whose records are columns (pivoted) that contain "
            "data values (numeric) in the load_data table.",
        ),
    ]


class UnpivotedTableFormatModel(DSGBaseModel):
    format_type: Literal[TableFormatType.UNPIVOTED] = TableFormatType.UNPIVOTED
    value_column: Annotated[
        str,
        Field(
            default=VALUE_COLUMN,
            title="value_column",
            description="The name of the load_data column that contains data values (numeric).",
        ),
    ]


TableFormatModel = Annotated[
    Union[PivotedTableFormatModel, UnpivotedTableFormatModel],
    Field(
        description="Defines the format of the value columns of the result table.",
        discriminator="format_type",
        title="table_format",
    ),
]
