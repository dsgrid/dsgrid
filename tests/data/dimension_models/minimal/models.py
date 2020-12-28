
from dataclasses import dataclass
import datetime
import os


from dsgrid.dimension.base import EndUseDimension, GeographicDimension, SectorDimension, TimeDimension
from dsgrid.transform.types import OneToMany, OneToOne, ProgrammaticOneToOne
from dsgrid.time_conversions import convert_datetime_to_day_type, convert_datetime_to_season


@dataclass(frozen=True)
class CensusDivision(GeographicDimension):
    """Census Region attributes"""


@dataclass(frozen=True)
class CensusRegion(GeographicDimension):
    """Census Region attributes"""


@dataclass(frozen=True)
class County(GeographicDimension):
    """County attributes"""
    state: str
    timezone: str = "Unknown"


@dataclass(frozen=True)
class DayType(TimeDimension):
    """Day Type attributes"""


@dataclass(frozen=True)
class Enduse(EndUseDimension):
    """Enduse attributes"""
    sector: str


@dataclass(frozen=True)
class Season(TimeDimension):
    """Season attributes"""


@dataclass(frozen=True)
class Sector(SectorDimension):
    """Sector attributes"""
    category: str


@dataclass(frozen=True)
class State(GeographicDimension):
    """State attributes"""
    is_conus: bool
    census_division: str = ""
    census_region: str = ""


@dataclass(frozen=True)
class Timezone(GeographicDimension):
    """Timezone attributes"""


_LOCAL = os.path.abspath(os.path.dirname(__file__))

MODEL_MAPPINGS = {
    CensusDivision: f"{_LOCAL}/census_divisions.json",
    CensusRegion: f"{_LOCAL}/census_regions.json",
    County: f"{_LOCAL}/counties.json",
    DayType: f"{_LOCAL}/day_types.json",
    Enduse: f"{_LOCAL}/enduses_electric_ind.json",
    Season: f"{_LOCAL}/seasons.json",
    Sector: f"{_LOCAL}/industrial_sectors.json",
    State: f"{_LOCAL}/states.json",
    Timezone: f"{_LOCAL}/timezones.json",
}

ONE_TO_MANY = [
    OneToMany(CensusDivision, "id", State, "census_division"),
    OneToMany(CensusRegion, "id", State, "census_region"),
    OneToMany(State, "id", County, "state"),
]

# TODO: consider better term
ONE_TO_ONE = [
    OneToOne(County, "timezone", Timezone, "id"),
    OneToOne(County, "state", State, "id"),
    OneToOne(State, "census_division", CensusDivision, "id"),
    OneToOne(State, "census_region", CensusRegion, "id"),
]

PROGRAMMATIC_ONE_TO_ONE = [
    ProgrammaticOneToOne(datetime.datetime, convert_datetime_to_season, Season, "id"),
    ProgrammaticOneToOne(datetime.datetime, convert_datetime_to_day_type, DayType, "id"),
]

# TODO
# Filter records by field? Such as reduce all states to states that are CONUS
