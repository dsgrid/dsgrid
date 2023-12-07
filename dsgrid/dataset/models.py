from typing import Literal, Union

from pydantic import Field
from typing_extensions import Annotated

from dsgrid.common import VALUE_COLUMN
from dsgrid.data_models import DSGBaseModel, DSGEnum, EnumValue
from dsgrid.dimension.base_models import DimensionType


class TableFormatType(DSGEnum):
    """Defines the format of value columns in a dataset."""

    PIVOTED = EnumValue(
        value="pivoted",
        description="The dataset value columns records are the records of one dimension.",
    )
    UNPIVOTED = EnumValue(
        value="unpivoted",
        description="The dataset has one column that contains values.",
    )


class PivotedTableFormatModel(DSGBaseModel):
    format_type: Literal[TableFormatType.PIVOTED.value] = TableFormatType.PIVOTED.value
    pivoted_dimension_type: Annotated[
        DimensionType,
        Field(
            alias="load_data_column_dimension",  # TODO: remove when datasets have been converted
            title="pivoted_dimension_type",
            description="The data dimension for which its values are in column form (pivoted) in the load_data table.",
        ),
    ]


class UnpivotedTableFormatModel(DSGBaseModel):
    format_type: Literal[TableFormatType.UNPIVOTED.value] = TableFormatType.UNPIVOTED.value
    value_column: Annotated[
        str,
        Field(
            default=VALUE_COLUMN,
            title="value_column",
            description="The name of the column in the dataset that has load values.",
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
