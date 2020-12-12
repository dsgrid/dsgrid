"""Dimension types for dsgrid"""

import abc
import enum
from dataclasses import dataclass


@dataclass
class DSGBaseDimension(abc.ABC):
    """Base class for all dsgrid dimenions"""


@dataclass
class EndUseDimension(DSGBaseDimension, abc.ABC):
    """Base class for all end use dimenions"""


@dataclass
class GeographicDimension(DSGBaseDimension, abc.ABC):
    """Base class for all geography dimensions"""


@dataclass
class SectorDimension(DSGBaseDimension, abc.ABC):
    """Base class for all sector dimenions"""


@dataclass
class TimeDimension(DSGBaseDimension, abc.ABC):
    """Base class for all time dimenions"""


class DayType(enum.Enum):
    WEEKEND = "weekend"
    WEEKDAY = "weekday"


class Season(enum.Enum):
    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    FALL = "autumn"
