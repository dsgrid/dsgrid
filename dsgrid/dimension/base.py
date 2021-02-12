"""Dimension types for dsgrid"""

import abc
import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class DSGBaseDimension(abc.ABC):    # TODO: Add Type here?
    """Base class for all dsgrid dimensions"""
    id: str
    name: str


@dataclass(frozen=True)
class EndUseDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all end use dimensions"""


@dataclass(frozen=True)
class GeographicDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all geography dimensions"""


@dataclass(frozen=True)
class ModelDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all load model dimenions"""


@dataclass(frozen=True)
class ModelYearDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all model year dimensions"""


@dataclass(frozen=True)
class ScenarioDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all scenario dimensions"""


@dataclass(frozen=True)
class SectorDimensionType(DSGBaseDimension, abc.ABC):


@dataclass(frozen=True)
class SubSectorDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all subsector dimenions"""


@dataclass(frozen=True)
class TimeDimensionType(DSGBaseDimension, abc.ABC):
    """Base class for all time dimensions"""


@dataclass(frozen=True)
class WeatherDimensionType(DSGBaseDimension, abc.ABC):
    """ attributes"""










class DayType(enum.Enum):
    """Day types"""
    WEEKEND = "weekend"
    WEEKDAY = "weekday"


class Season(enum.Enum):
    """Seasons"""
    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    FALL = "autumn"
