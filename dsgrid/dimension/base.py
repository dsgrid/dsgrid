"""Dimension types for dsgrid"""

import enum


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
