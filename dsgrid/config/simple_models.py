"""Defines simplified data models for testing and filtering."""

from typing import Optional

from pydantic import root_validator, validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType


class DimensionSimpleModel(DSGBaseModel):

    dimension_type: DimensionType
    dimension_query_name: Optional[str]
    record_ids: list[str]


class DimensionsSimpleModel(DSGBaseModel):

    base_dimensions: list[DimensionSimpleModel]
    supplemental_dimensions: list[DimensionSimpleModel] = Field(default=[])

    @validator("base_dimensions")
    def check_base_dimensions(cls, base_dimensions):
        dimension_types = {x.dimension_type for x in base_dimensions}
        if len(dimension_types) != len(base_dimensions):
            raise ValueError("base_dimensions cannot contain duplicate dimension types")
        return base_dimensions

    @root_validator
    def check_supplemental_dimensions(cls, values):
        supp = values.get("supplemental_dimensions")
        if supp is None:
            return values

        for dim in values["supplemental_dimensions"]:
            if dim.dimension_query_name is None:
                raise ValueError(
                    f"supplemental dimensions must define dimension_query_name: {dim}"
                )
        return values


class DatasetSimpleModel(DSGBaseModel):

    dataset_id: str
    dimensions: list[DimensionSimpleModel]


class ProjectSimpleModel(DSGBaseModel):

    project_id: str
    dimensions: DimensionsSimpleModel


class RegistrySimpleModel(DSGBaseModel):

    name: str
    projects: list[ProjectSimpleModel]
    datasets: list[DatasetSimpleModel]
