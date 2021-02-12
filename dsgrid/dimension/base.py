"""Dimension types for dsgrid"""

import abc
import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class DSGBaseDimension(abc.ABC):
    """Base class for all dsgrid dimensions"""
    id: str
    name: str


@dataclass(frozen=True)
class EndUseDimension(DSGBaseDimension, abc.ABC):
    """Base class for all end use dimensions"""


@dataclass(frozen=True)
class GeographicDimension(DSGBaseDimension, abc.ABC):
    """Base class for all geography dimensions"""


@dataclass(frozen=True)
class SectorDimension(DSGBaseDimension, abc.ABC):
    """Base class for all sector dimensions"""


@dataclass(frozen=True)
class TimeDimension(DSGBaseDimension, abc.ABC):
    """Base class for all time dimensions"""


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
