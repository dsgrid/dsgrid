"""Defines a supplemental dimension."""

from pydantic import Field, field_validator

from typing_extensions import Annotated

from dsgrid.data_models import DSGBaseModel
from .dimensions import DimensionModel
from .mapping_tables import MappingTableByNameModel


class SupplementalDimensionModel(DimensionModel):
    """Defines a supplemental dimension."""

    mapping: Annotated[
        MappingTableByNameModel,
        Field(
            description="Defines how the supplemental dimension will be mapped to the project's base "
            "dimension.",
            title="mapping",
        ),
    ]


class SupplementalDimensionsListModel(DSGBaseModel):
    """Defines a list of supplemental dimensions."""

    supplemental_dimensions: Annotated[
        list[SupplementalDimensionModel],
        Field(
            title="supplemental_dimensions",
            description="List of supplemental dimensions. They will be automatically registered "
            "during project registration and then converted to supplemental_dimension_references.",
        ),
    ]

    @field_validator("supplemental_dimensions")
    @classmethod
    def check_supplemental_dimensions(cls, supplemental_dimensions: list) -> list:
        if not supplemental_dimensions:
            raise ValueError("supplemental_dimensions cannot be empty")
        return supplemental_dimensions
