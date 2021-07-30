"""Dimensions related to time"""

from enum import Enum


class LeapDayAdjustmentType(Enum):
    """Timezone enum types"""

    # TODO: need some kind of mapping from this enum to leap day
    #       adjustment methods
    DROP_DEC31 = "drop_dec31"
    DROP_FEB29 = "drop_feb29"
    DROP_JAN1 = "drop_jan1"


class Period(Enum):
    """Time period enum types"""

    # TODO: R2PD uses a different set; do we want to align?
    # https://github.com/Smart-DS/R2PD/blob/master/R2PD/tshelpers.py#L15
    PERIOD_ENDING = "period_ending"
    PERIOD_BEGINNING = "period_beginning"
    INSTANTANEOUS = "instantaneous"


class TimeValueMeasurement(Enum):
    """Time value measurement enum types"""

    # TODO: any kind of mappings/conversions for this?
    # TODO: may want a way to alarm if input data != project data measurement
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    MEASURED = "measured"
    TOTAL = "total"


class TimezoneType(Enum):
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
