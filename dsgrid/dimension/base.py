"""Dimension types for dsgrid"""

import enum
import os

from pydantic import BaseModel, Field
from semver import VersionInfo

# from dsgrid.dimension.models import (
#    EndUseDimensionModel,
#    GeographicDimensionModel,
#    ModelDimensionModel,
#    ModelYearDimensionModel,
#    ScenarioDimensionModel,
#    SectorDimensionModel,
#    SubSectorDimensionModel,
#    TimeDimensionModel,
#    WeatherDimensionModel,
# )


class DimensionType(enum.Enum):
    """Dimension types"""

    END_USE = "end_use"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"
    WEATHER = "weather"
    MODEL_YEAR = "model_year"
    SCENARIO = "scenario"
    MODEL = "model"


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
