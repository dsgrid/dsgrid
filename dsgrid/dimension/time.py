"""Dimensions related to time"""

from enum import Enum
from dsgrid.data_models import Enum, DSGEnum, EnumValue


class LeapDayAdjustmentType(DSGEnum):
    """Timezone enum types"""

    DROP_DEC31 = EnumValue(
        value="drop_dec31",
        description="To adjust for leap years, December 31st gets dropped",
    )
    DROP_FEB29 = EnumValue(
        value="drop_feb29",
        description="Feburary 29th is dropped. Currently not yet supported by dsgrid.",
    )
    DROP_JAN1 = EnumValue(
        value="drop_jan1",
        description="To adjust for leap years, January 1st gets dropped",
    )


class Period(DSGEnum):
    """Time period enum types"""

    PERIOD_ENDING = EnumValue(
        value="period_ending",
        description="A time period that is period ending is coded by the end time. E.g., 2pm (with"
        " freq=1h) represents a period of time between 1-2pm.",
    )
    PERIOD_BEGINNING = EnumValue(
        value="period_beginning",
        description="A time period that is period beginning is coded by the beginning time. E.g., "
        "2pm (with freq=1h) represents a period of time between 2-3pm. This is the dsgrid default.",
    )
    INSTANTANEOUS = EnumValue(
        value="instantaneous",
        description="The time record value represents measured, instantaneous time",
    )


# TODO: R2PD uses a different set; do we want to align?
# https://github.com/Smart-DS/R2PD/blob/master/R2PD/tshelpers.py#L15
#


class TimeValueMeasurement(DSGEnum):
    """Time value measurement enum types"""

    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    MEASURED = "measured"
    TOTAL = "total"


class TimezoneType(DSGEnum):
    """Timezone enum types"""

    UTC = "UTC"
    HST = "HawaiiAleutianStandard"
    AST = "AlaskaStandard"
    APT = "AlaskaPrevailingStandard"
    PST = "PacificStandard"
    PPT = "PacificPrevailing"
    MST = "MountainStandard"
    MPT = "MountainPrevailing"
    CST = "CentralStandard"
    CPT = "CentralPrevailing"
    EST = "EasternStandard"
    EPT = "EasternPrevailing"
    LOCAL = "LOCAL"  # Implies that the geography's timezone will be dynamically applied.


class DatetimeRange:
    def __init__(self, start, end, frequency):
        self.start = start
        self.end = end
        self.frequency = frequency

    def iter_time_range(self, period: Period, leap_day_adjustment: LeapDayAdjustmentType):
        """Return a generator of datetimes for a time range.

        Parameters
        ----------
        period : Period
        leap_day_adjustment : LeapDayAdjustmentType

        Yields
        ------
        datetime

        """
        cur = self.start
        end = self.end + self.frequency if period == period.PERIOD_ENDING else self.end
        while cur < end:
            if not (
                leap_day_adjustment == LeapDayAdjustmentType.DROP_FEB29
                and cur.month == 2
                and cur.day == 29
            ):
                yield cur
            cur += self.frequency

    def list_time_range(self, period: Period, leap_day_adjustment: LeapDayAdjustmentType):
        """Return a list of datetimes for a time range.

        Parameters
        ----------
        period : Period
        leap_day_adjustment : LeapDayAdjustmentType

        Returns
        -------
        list
            list of datetime

        """
        return list(self.iter_time_range(period, leap_day_adjustment))
