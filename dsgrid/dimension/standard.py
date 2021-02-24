"""Standard dimension classes for dsgrid"""

from typing import List, Optional, Union

from pydantic import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.dimension.base import (
    EndUseDimensionModel, GeographicDimensionModel, ModelDimensionModel,
    ModelYearDimensionModel, ScenarioDimensionModel, SectorDimensionModel,
    SubSectorDimensionModel, TimeDimensionModel, WeatherDimensionModel
)


# ---------------------------
# GEOGRAPHIC DIMENSIONS
# ---------------------------
class CensusDivision(GeographicDimensionModel):
    """Census Region attributes"""


class CensusRegion(GeographicDimensionModel):
    """Census Region attributes"""


class County(GeographicDimensionModel):
    """County attributes"""
    state: str

class State(GeographicDimensionModel):
    """State attributes"""
    is_conus: bool
    census_division: str = ""
    census_region: str = ""


# ---------------------------
# SECTOR DIMENSIONS
# ---------------------------
class Sector(SectorDimensionModel):
    """Sector attributes"""
    category: Optional[str]


# ---------------------------
# SUBSECTOR DIMENSIONS
# ---------------------------
class SubSector(SubSectorDimensionModel):
    """Subsector attributes"""
    # NOTE: making optional for now, we may remove because it should be handled in the association tables
    sector: Optional[str]
    abbr: Optional[str] = Field(
        default="",
    )

    @validator('abbr', pre=True)
    def validate_abbr(cls, value: Union[str, None]) -> str:
        return value or ""


# ---------------------------
# ENDUSE DIMENSIONS
# ---------------------------
class EndUse(EndUseDimensionModel):
    """End use attributes"""
    #sector: str  # TODO: the raw data doesn't have this field
    fuel_id: str
    units: str


# ---------------------------
# TIME DIMENSIONS
# ---------------------------
class Time(TimeDimensionModel):
    """Time attributes"""


class Timezone(TimeDimensionModel):
    """Timezone attributes"""


class DayType(TimeDimensionModel):
    """Day Type attributes"""


class Season(TimeDimensionModel):
    """Season attributes"""


# ---------------------------
# OTHER DIMENSIONS
# ---------------------------
class Weather(WeatherDimensionModel):
    """ attributes"""


class ModelYear(ModelYearDimensionModel):
    """ attributes"""


class Model(ModelDimensionModel):
    """Model attributes"""


class Scenario(ScenarioDimensionModel):
    """Scenario attributes"""
