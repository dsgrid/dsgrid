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
    # 0 = Monday, 6 = Sunday. Follows pyspark.sql.functions.weekday and Python datetime.weekday.
    day_of_week: int
    hour: int


class OneWeekdayDayAndOneWeekendDayPerMonthByHourType(NamedTuple):
    """Columns of representative time with month, hour, and weekday vs weekend."""

    month: int
    is_weekday: bool
    hour: int


class IndexTimestampType(NamedTuple):
    """Single column with numerical indices."""

    time_index: int


class StringTimestampType(NamedTuple):
    """Single column with time (must include offset) as str."""

    timestamp: str
