"""Types related to time"""

from datetime import datetime
from typing import NamedTuple

from dsgrid.data_models import DSGEnum


class DayType(DSGEnum):
    """Day types"""

    WEEKEND = "weekend"
    WEEKDAY = "weekday"


class Season(DSGEnum):
    """Seasons"""

    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    FALL = "autumn"


# The types below represent the timestamps that exist as columns in all datasets.


class DatetimeTimestampType(NamedTuple):
    """Single column with datetime."""

    timestamp: datetime


class AnnualTimestampType(NamedTuple):
    """Single column with only year."""

    time_year: int


class OneWeekPerMonthByHourType(NamedTuple):
    """Columns of representative time with one week per month."""

    month: int
    day_of_week: int
    hour: int


class OneWeekdayDayAndOneWeekendDayPerMonthByHourType(NamedTuple):
    """Columns of representative time with month, hour, and weekday vs weekend."""

    month: int
    is_weekday: bool
    hour: int
