"""Types related to time"""

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
