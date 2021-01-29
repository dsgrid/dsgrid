import abc
import os
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Field, ValidationError
from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator


class DimensionType(Enum):
    """Dimension types"""
    END_USE = "end_use"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"


class TimezoneType(Enum):
    """Dimension types"""
    PST = "PST"
    MST = "MST"
    CST = "CST"
    EST = "EST"
    NONE = "None"


class DSGBaseModel(BaseModel, abc.ABC):
    """Base model for all dsgrid models"""

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"  # TODO: consider changing this after we get this working
        use_enum_values = True


class DimensionBase(DSGBaseModel, abc.ABC):
    """Common attributes for all dimensions"""
    name: str = Field(
        title="name",
        description="dimension name",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="type of the dimension",
    )


class Dimension(DimensionBase):
    """Defines a dimension"""
    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing dimension records",
    )

    @validator("filename")
    def validate_file(cls, val):
        assert os.path.isfile(val), f"{val} does not exist"
        return val


class TimeDimension(DimensionBase):
    """Defines a time dimension"""
    start: datetime = Field(
        title="start",
        description="first timestamp in the data",
    )
    # TODO: Is this inclusive or exclusive?
    end: datetime = Field(
        title="end",
        description="last timestamp in the data",
    )
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="timestamp format",
    )
    frequency: str = Field(
        title="frequency",
        description="resolution of the timestamps",
    )
    includes_dst: bool = Field(
        title="includes_dst",
        description="includes daylight savings time",
    )
    # TODO: do we need this? We can determine it programmatically.
    includes_leap_day: Optional[bool] = Field(
        title="includes_leap_day",
        default=False,
        description="includes a leap day",
    )
    leap_day_adjustment: Optional[str] = Field(
        title="leap_day_adjustment",
        default="",
        description="TODO",
    )
    model_years: Optional[List[int]] = Field(
        title="model_years",
        default=[],
        description="",
    )
    period: str = Field(
        title="period",
        description="TODO",
    )
    timezone: TimezoneType = Field(
        title="timezone",
        description="timezone of data",
    )
    value_representation: str = Field(
        title="value_representation",
        default="mean",
        description="TODO",
    )

    @root_validator(pre=True)
    def validate_time_dimension(cls, values: dict) -> dict:
        # Just make sure these parse.
        datetime.strptime(values["start"], values["str_format"])
        datetime.strptime(values["end"], values["str_format"])
        # TODO: validate consistency between start, end, frequency
        return values


class Dimensions(DSGBaseModel):
    """Contains dimensions defined by a dataset"""
    model_sectors: List[str] = Field(
        title="model_sectors",
        description="model sectors used in the project",
    )
    models: List[str] = Field(
        title="models",
        description="models used by the project",
    )
    sectors: List[str] = Field(
        title="sectors",
        description="sectors used in the project",
    )
    project_dimensions: List[Union[Dimension, TimeDimension]] = Field(
        title="project_dimensions",
        description="dimensions defined by the project",
    )
    supplemental_dimensions: Optional[List[Union[Dimension, TimeDimension]]] = Field(
        title="supplemental_dimensions",
        description="supplemental dimensions",
        default=[],
    )


class InputDataset(DSGBaseModel):
    """Defines an input dataset"""
    dataset_id: str = Field(
        title="dataset_id",
        alias="id",
        description="dataset ID",
    )
    #dataset_type: str = Field(
    #    title=,
    #    description=,
    #)
    #version: str = Field(
    #    title=,
    #    description=,
    #)
    #dimensions: List[Dimension] = Field(
    #    title=,
    #    description=,
    #)
    #metadata: Optional[Dict] = Field(
    #    title=,
    #    description=,
    #)
    model_name: str = Field(
        title="model_name",
        description="model name",
    )
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    sectors: List[str] = Field(
        title="sectors",
        description="sectors used in the project",
    )


class InputDatasets(DSGBaseModel):
    """Defines all input datasets for a project"""
    # TODO: incorrect
    benchmark: List[str] = Field(
        title="benchmark",
        default=[],
        description="benchmark",
    )
    # TODO: incorrect
    historical: List[str] = Field(
        title="historical",
        default=[],
        description="historical",
    )
    datasets: List[InputDataset] = Field(
        title="datasets",
        description="project input datasets",
    )


class ProjectConfig(DSGBaseModel):
    """Represents project configurations"""
    project_id: str = Field(
        title="project_id",
        alias="id",
        description="project identifier",
    )
    name: str = Field(
        title="name",
        description="project name",
    )
    version: str = Field(
        title="version",
        description="project version",
    )
    input_datasets: InputDatasets = Field(
        title="input_datasets",
        description="input datasets for the project",
    )
    dimensions: Dimensions = Field(
        title="dimensions",
        description="dimensions",
    )
