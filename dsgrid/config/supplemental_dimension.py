"""Defines a supplemental dimension."""

from pydantic import Field

from typing_extensions import Annotated

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
