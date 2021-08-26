"""Functions to perform time conversions"""

from datetime import datetime

from dsgrid.time.types import DayType, Season


def convert_datetime_to_day_type(timestamp):
    """Returns the day type for the datetime.

    Parameters
    ----------
    timestamp : datetime.datetime

    Returns
    -------
    str
        DayType id

    """
    # Monday is 0.
    if timestamp.weekday() <= 4:
        return DayType.WEEKDAY.value
    return DayType.WEEKEND.value


def convert_datetime_to_season(timestamp):
    """Returns the season for the datetime.

    Parameters
    ----------
    timestamp : datetime.datetime

    Returns
    -------
    str
        Season id

    """
    # TODO: dates do change slightly every year. Is this close enough?
    # dates also change by region, it's weather driven.
    year = timestamp.year
    if timestamp < datetime(year, 3, 20) or timestamp > datetime(year, 12, 21):
        season = Season.WINTER.value
    elif timestamp < datetime(year, 6, 20):
        season = Season.SPRING.value
    elif timestamp < datetime(year, 9, 22):
        season = Season.SUMMER.value
    else:
        season = Season.AUTUMN.value

    return season


def interpret_datetime(timestamp):
    """Return a datetime object from a timestamp string.

    Parameters
    ----------
    timestamp : str

    Returns
    -------
    datetime.datetime

    """
    formats = (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    )

    for i, fmt in enumerate(formats):
        try:
            return datetime.strptime(timestamp, fmt)
        except ValueError:
            if i == len(formats) - 1:
                raise
