"""Defines simplified data models for testing and filtering."""

from typing import List, Optional

from pydantic import root_validator, validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class DimensionSimpleModel(DSGBaseModel):

    dimension_type: DimensionType
    dimension_query_name: Optional[str]
    record_ids: List[str]


class DimensionsSimpleModel(DSGBaseModel):

    base_dimensions: List[DimensionSimpleModel]
    supplemental_dimensions: List[DimensionSimpleModel] = Field(default=[])

    @validator("base_dimensions")
    def check_base_dimensions(cls, base_dimensions):
        dimension_types = {x.dimension_type for x in base_dimensions}
        if len(dimension_types) != len(base_dimensions):
            raise ValueError("base_dimensions cannot contain duplicate dimension types")
        return base_dimensions

    @root_validator
    def check_supplemental_dimensions(cls, values):
        for dim in values["supplemental_dimensions"]:
            if dim.dimension_query_name is None:
                raise ValueError(
                    f"supplemental dimensions must define dimension_query_name: {dim}"
                )
        return values


class DatasetSimpleModel(DSGBaseModel):

    dataset_id: str
    dimensions: List[DimensionSimpleModel]


class ProjectSimpleModel(DSGBaseModel):

    project_id: str
    dimensions: DimensionsSimpleModel


class RegistrySimpleModel(DSGBaseModel):

    name: str
    projects: List[ProjectSimpleModel]
    datasets: List[DatasetSimpleModel]
