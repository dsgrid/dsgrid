import abc
import os
from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field
from pydantic.fields import Field
from pydantic.class_validators import root_validator


class DimensionType(Enum):
    """Dimension types"""
    END_USE = "end_use"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"


class TimezoneType(Enum):
    """Dimension types"""
    PST = "Pacific Standard Time"
    MST = "Mountain Standard Time"
    CST = "Central Standard Time"
    EST = "Eastern Standard Time"
    NONE = "None"


class DSGBaseModel(BaseModel, abc.ABC):
    """Base model for all dsgrid models"""

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"
        use_enum_values = True


class Dimension(DSGBaseModel):
    """Defines a dimension"""
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="type of the dimension",
    )
    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing dimension records",
    )
    name: str = Field(
        title="name",
        description="dimension name",
    )

    @root_validator(pre=True)
    def validate_dimension(cls, values: dict) -> dict:
        if not os.path.isfile(values["file"]):
            raise FileNotFoundError(f"{values['file']} does not exist")
        return values


class TimeDimension(Dimension):
    """Defines a time dimension"""
    start: datetime = Field(
        title="start",
        description="first timestamp in the data",
    )
    # TODO: We have start and interval. Do we need this?
    #end: datetime = Field(
    #    title="end",
    #    description="last timestamp in the data",
    #)
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="timestamp format",
    )
    frequency: str = Field(
        title="frequency",
        description="resolution of the timestamps",
    )
    interval: str = Field(
        title="interval",
        description="total duration",
    )
    includes_dst: bool = Field(
        title="includes_dst",
        description="includes daylight savings time",
    )
    # TODO: do we need this? We can determine it programmatically.
    #includes_leap_day: bool = Field
    #    title="includes_leap_day",
    #    description="includes a leap day",
    #)
    leap_day_adjustment: Optional[str] = Field(
        title="leap_day_adjustment",
        description="TODO",
    )
    # TODO: do we need this? We already have start + interval
    #model_years: List[int] = Field(
    #    title="model_years",
    #    description="",
    #)
    period: str = Field(
        title="period",
        description="TODO",
    )
    # TODO: can't the data have more than one timezone?
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
        values = super(self).validate_dimension(values)
        start = datetime.strptime(valus["start"], values["str_format"])
        end = datetime.strptime(valus["end"], values["str_format"])
        # TODO: validate consistency between start, end, frequency
        return values


class Dimensions(DSGBaseModel):
    """Defines all dimensions for a dataset"""
    end_use: Optional[List[Dimension]] = Field(
        title="end_use",
        default = [],
        description="end use dimension",
    )
    geography: Optional[List[Dimension]] = Field(
        title="geography",
        default = [],
        description="geographic dimension",
    )
    sector: Optional[List[Dimension]] = Field(
        title="sector",
        default = [],
        description="sector dimension",
    )
    subsector: Optional[List[Dimension]] = Field(
        title="subsector",
        default = [],
        description="subsector dimension",
    )
    time_dimension: Optional[List[TimeDimension]] = Field(
        title="time",
        alias="time",
        default = [],
        description="time dimension",
    )

class DimensionsContainer(DSGBaseModel):
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
    project: Dimensions = Field(
        title="project",
        description="dimensions defined by the project",
    )
    supplemental: Dimensions = Field(
        title="supplemental",
        description="supplemental dimensions",
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
    dimensions: DimensionsContainer = Field(
        title="dimensions",
        description="dimensions",
    )
