"""Standard dimension classes for dsgrid"""

from pydantic.dataclasses import dataclass

from dsgrid.dimension.base import (
    EndUseDimensionType, GeographicDimensionType, ModelDimensionType, ModelYearDimensionType,
    ScenarioDimensionType, SectorDimensionType, SubSectorDimensionType, TimeDimensionType,
    WeatherDimensionType)


# ---------------------------
# GEOGRAPHIC DIMENSIONS
# ---------------------------
@dataclass(frozen=True)
class CensusDivision(GeographicDimensionType):
    """Census Region attributes"""


@dataclass(frozen=True)
class CensusRegion(GeographicDimensionType):
    """Census Region attributes"""


@dataclass(frozen=True)
class County(GeographicDimensionType):
    """County attributes"""
    state: str
    timezone: str = "Unknown"


@dataclass(frozen=True)
class State(GeographicDimensionType):
    """State attributes"""
    is_conus: bool
    census_division: str = ""
    census_region: str = ""


# ---------------------------
# SECTOR DIMENSIONS
# ---------------------------
@dataclass(frozen=True)
class Sector(SectorDimensionType):
    """Sector attributes"""
    category: str


# ---------------------------
# SUBSECTOR DIMENSIONS
# ---------------------------
@dataclass(frozen=True)
class SubSector(SubSectorDimensionType):
    """Subsector attributes"""
    category: str


# ---------------------------
# ENDUSE DIMENSIONS
# ---------------------------
@dataclass(frozen=True)
class EndUse(EndUseDimensionType):
    """End use attributes"""
    sector: str


# ---------------------------
# TIME DIMENSIONS
# ---------------------------
class Time(TimeDimensionType):
    """Time attributes"""


@dataclass(frozen=True)     # TODO: is this a "dimension" technically?
class Timezone(TimeDimensionType):
    """Timezone attributes"""


@dataclass(frozen=True)
class DayType(TimeDimensionType):
    """Day Type attributes"""


@dataclass(frozen=True)
class Season(TimeDimensionType):
    """Season attributes"""


# ---------------------------
# OTHER DIMENSIONS
# ---------------------------
@dataclass(frozen=True)
class Weather(WeatherDimensionType):
    """ attributes"""


@dataclass(frozen=True)
class ModelYear(ModelYearDimensionType):
    """ attributes"""


@dataclass(frozen=True)
class Model(ModelDimensionType):
    """Model attributes"""


@dataclass(frozen=True)
class Scenario(ScenarioDimensionType):
    """Scenario attributes"""
