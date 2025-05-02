"""Defines simplified data models for testing and filtering."""

from typing import Optional

from pydantic import field_validator, model_validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class DimensionSimpleModel(DSGBaseModel):

    dimension_type: DimensionType
    dimension_name: Optional[str] = None
    record_ids: list[str]


class DimensionsSimpleModel(DSGBaseModel):

    base_dimensions: list[DimensionSimpleModel]
    supplemental_dimensions: list[DimensionSimpleModel] = Field(default=[])

    @field_validator("base_dimensions")
    @classmethod
    def check_base_dimensions(cls, base_dimensions):
        dimension_types = {x.dimension_type for x in base_dimensions}
        if len(dimension_types) != len(base_dimensions):
            raise ValueError("base_dimensions cannot contain duplicate dimension types")
        return base_dimensions

    @model_validator(mode="after")
    def check_supplemental_dimensions(self) -> "DimensionsSimpleModel":
        for dim in self.supplemental_dimensions:
            if dim.dimension_name is None:
                raise ValueError(f"supplemental dimensions must define dimension_name: {dim}")
        return self


class DatasetSimpleModel(DSGBaseModel):

    dataset_id: str
    dimensions: list[DimensionSimpleModel]


class ProjectSimpleModel(DSGBaseModel):

    project_id: str
    dimensions: DimensionsSimpleModel


class RegistrySimpleModel(DSGBaseModel):

    projects: list[ProjectSimpleModel]
    datasets: list[DatasetSimpleModel]
