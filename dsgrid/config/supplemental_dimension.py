"""Defines a supplemental dimension."""

from pydantic import Field, conlist


from dsgrid.data_models import DSGBaseModel
from .dimensions import DimensionModel
from .mapping_tables import MappingTableByNameModel


class SupplementalDimensionModel(DimensionModel):
    """Defines a supplemental dimension."""

    mapping: MappingTableByNameModel = Field(
        description="Defines how the supplemental dimension will be mapped to the project's base "
        "dimension.",
        title="mapping",
    )


class SupplementalDimensionsListModel(DSGBaseModel):
    """Defines a list of supplemental dimensions."""

    supplemental_dimensions: conlist(SupplementalDimensionModel, min_length=1) = Field(
        description="List of supplemental dimensions and mappings to be registered"
    )
