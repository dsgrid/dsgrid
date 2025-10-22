from asyncio.log import logger
from enum import StrEnum
from typing import Literal, Union

from pydantic import Field, model_validator
from typing_extensions import Annotated

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class TableFormatType(StrEnum):
    """Defines the format of value columns in a dataset."""

    PIVOTED = "pivoted"
    UNPIVOTED = "unpivoted"


class TableFormatModelBase(DSGBaseModel):
    format_type: TableFormatType


class PivotedTableFormatModel(TableFormatModelBase):
    format_type: Literal[TableFormatType.PIVOTED] = TableFormatType.PIVOTED
    pivoted_dimension_type: DimensionType = Field(
        title="pivoted_dimension_type",
        description="The data dimension whose records are columns (pivoted) that contain "
        "data values (numeric) in the load_data table.",
    )


class UnpivotedTableFormatModel(TableFormatModelBase):
    format_type: Literal[TableFormatType.UNPIVOTED] = TableFormatType.UNPIVOTED

    @model_validator(mode="before")
    @classmethod
    def handle_legacy(cls, values: dict) -> dict:
        if "value_column" in values:
            logger.warning("Removing deprecated value_column field from unpivoted table format.")
            values.pop("value_column")

        return values


TableFormatModel = Annotated[
    Union[PivotedTableFormatModel, UnpivotedTableFormatModel],
    Field(
        description="Defines the format of the value columns of the result table.",
        discriminator="format_type",
        title="table_format",
    ),
]
