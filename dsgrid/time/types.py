"""Types related to time"""

from enum import Enum


class DayType(Enum):
    """Day types"""

    WEEKEND = "weekend"
    WEEKDAY = "weekday"


class Season(Enum):
    """Seasons"""

    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    FALL = "autumn"
