"""Types related to time"""

from collections import namedtuple

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

DatetimeTimestampType = namedtuple("DatetimeTimestampType", ["timestamp"])

AnnualTimestampType = namedtuple("AnnualTimestampType", ["year"])

OneWeekPerMonthByHourType = namedtuple(
    "OneWeekPerMonthByHourType", ["month", "day_of_week", "hour"]
)
