"""Dimensions related to time"""
import datetime
from zoneinfo import ZoneInfo
import logging

from dsgrid.data_models import DSGEnum, EnumValue

logger = logging.getLogger(__name__)


class TimeDimensionType(DSGEnum):
    """Defines the supported time formats in the load data."""

    DATETIME = "datetime"
    ANNUAL = "annual"
    REPRESENTATIVE_PERIOD = "representative_period"
    NOOP = "noop"


class RepresentativePeriodFormat(DSGEnum):
    """Defines the supported formats for representative period data."""

    # All instances of this Enum must declare frequency.
    # This Enum may be replaced by a generic implementation in order to support a large
    # number of permutations (seasons, weekend day vs week day, sub-hour time, etc).

    ONE_WEEK_PER_MONTH_BY_HOUR = EnumValue(
        value="one_week_per_month_by_hour",
        frequency=datetime.timedelta(hours=1),
        description="load_data columns use 'month', 'day_of_week', 'hour' to specify time",
    )


class LeapDayAdjustmentType(DSGEnum):
    """Leap day adjustment enum types"""

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
    NONE = EnumValue(value="none", description="No leap day adjustment made.")


class TimeIntervalType(DSGEnum):
    """Time interval enum types"""

    # TODO: R2PD uses a different set; do we want to align?
    # https://github.com/Smart-DS/R2PD/blob/master/R2PD/tshelpers.py#L15

    PERIOD_ENDING = EnumValue(
        value="period_ending",
        description="A time interval that is period ending is coded by the end time. E.g., 2pm (with"
        " freq=1h) represents a period of time between 1-2pm.",
    )
    PERIOD_BEGINNING = EnumValue(
        value="period_beginning",
        description="A time interval that is period beginning is coded by the beginning time. E.g.,"
        " 2pm (with freq=01:00:00) represents a period of time between 2-3pm. This is the dsgrid"
        " default.",
    )
    INSTANTANEOUS = EnumValue(
        value="instantaneous",
        description="The time record value represents measured, instantaneous time",
    )


class MeasurementType(DSGEnum):
    """Time value measurement enum types"""

    MEAN = EnumValue(
        value="mean",
        description="Data values represent the average value in a time range",
    )
    MIN = EnumValue(
        value="min",
        description="Data values represent the minimum value in a time range",
    )
    MAX = EnumValue(
        value="max",
        description="Data values represent the maximum value in a time range",
    )
    MEASURED = EnumValue(
        value="measured",
        description="Data values represent the measured value at that reported time",
    )
    TOTAL = EnumValue(
        value="total",
        description="Data values represent the sum of values in a time range",
    )


class TimeZone(DSGEnum):
    """Time zone enum types
    - tz: zoneinfo.available_timezones()
    - tz_name: spark uses Java timezones: https://jenkov.com/tutorials/java-date-time/java-util-timezone.html
    """

    UTC = EnumValue(
        value="UTC",
        description="Coordinated Universal Time",
        tz=ZoneInfo("UTC"),
        tz_name="UTC",
    )
    HST = EnumValue(
        value="HawaiiAleutianStandard",
        description="Hawaii Standard Time (UTC=-10). Does not include DST shifts.",
        tz=ZoneInfo("US/Hawaii"),
        tz_name="HST",
    )
    AST = EnumValue(
        value="AlaskaStandard",
        description="Alaskan Standard Time (UTC=-9). Does not include DST shifts.",
        tz=ZoneInfo("Etc/GMT+9"),
        tz_name="AST",
    )
    APT = EnumValue(
        value="AlaskaPrevailing",
        description="Alaska Prevailing Time. Commonly called Alaska Local Time. Includes DST"
        " shifts during DST times.",
        tz=ZoneInfo("US/Alaska"),
        tz_name="US/Alaska",
    )
    PST = EnumValue(
        value="PacificStandard",
        description="Pacific Standard Time (UTC=-8). Does not include DST shifts.",
        tz=ZoneInfo("Etc/GMT+8"),
        tz_name="PST",
    )
    PPT = EnumValue(
        value="PacificPrevailing",
        description="Pacific Prevailing Time. Commonly called Pacific Local Time. Includes DST"
        " shifts ,during DST times.",
        tz=ZoneInfo("US/Pacific"),
        tz_name="US/Pacific",
    )
    MST = EnumValue(
        value="MountainStandard",
        description="Mountain Standard Time (UTC=-7). Does not include DST shifts.",
        tz=ZoneInfo("Etc/GMT+7"),
        tz_name="MST",
    )
    MPT = EnumValue(
        value="MountainPrevailing",
        description="Mountain Prevailing Time. Commonly called Mountain Local Time. Includes DST"
        " shifts during DST times.",
        tz=ZoneInfo("US/Mountain"),
        tz_name="US/Mountain",
    )
    CST = EnumValue(
        value="CentralStandard",
        description="Central Standard Time (UTC=-6). Does not include DST shifts.",
        tz=ZoneInfo("Etc/GMT+6"),
        tz_name="CST",
    )
    CPT = EnumValue(
        value="CentralPrevailing",
        description="Central Prevailing Time. Commonly called Central Local Time. Includes DST"
        " shifts during DST times.",
        tz=ZoneInfo("US/Central"),
        tz_name="US/Central",
    )
    EST = EnumValue(
        value="EasternStandard",
        description="Eastern Standard Time (UTC=-5). Does not include DST shifts.",
        tz=ZoneInfo("Etc/GMT+5"),
        tz_name="EST",
    )
    EPT = EnumValue(
        value="EasternPrevailing",
        description="Eastern Prevailing Time. Commonly called Eastern Local Time. Includes DST"
        " shifts during DST times.",
        tz=ZoneInfo("US/Eastern"),
        tz_name="US/Eastern",
    )
    NONE = EnumValue(
        value="none",
        description="No timezone, suitable for temporally aggregated data",
        tz=None,
        tz_name="none",
    )

    def get_standard_time(self):
        """get equivalent standard time"""
        if self == TimeZone.UTC:
            return TimeZone.UTC
        if self == TimeZone.HST:
            return TimeZone.HST
        if self in [TimeZone.AST, TimeZone.APT]:
            return TimeZone.AST
        if self in [TimeZone.PST, TimeZone.PPT]:
            return TimeZone.PST
        if self in [TimeZone.MST, TimeZone.MPT]:
            return TimeZone.MST
        if self in [TimeZone.CST, TimeZone.CPT]:
            return TimeZone.CST
        if self in [TimeZone.EST, TimeZone.EPT]:
            return TimeZone.EST
        if self == TimeZone.NONE:
            return TimeZone.NONE
        raise NotImplementedError(f"BUG: case not covered: {self}")

    def get_prevailing_time(self):
        """get equivalent prevailing time"""
        if self == TimeZone.UTC:
            return TimeZone.UTC
        if self == TimeZone.HST:
            return TimeZone.HST
        if self in [TimeZone.AST, TimeZone.APT]:
            return TimeZone.APT
        if self in [TimeZone.PST, TimeZone.PPT]:
            return TimeZone.PPT
        if self in [TimeZone.MST, TimeZone.MPT]:
            return TimeZone.MPT
        if self in [TimeZone.CST, TimeZone.CPT]:
            return TimeZone.CPT
        if self in [TimeZone.EST, TimeZone.EPT]:
            return TimeZone.EPT
        if self == TimeZone.NONE:
            logger.info(f"TimeZone={self.value} does not have meaningful standard time.")
            return TimeZone.NONE
        raise NotImplementedError(f"BUG: case not covered: {self}")

    def is_standard(self):
        lst = [
            TimeZone.UTC,
            TimeZone.HST,
            TimeZone.AST,
            TimeZone.PST,
            TimeZone.MST,
            TimeZone.CST,
            TimeZone.EST,
        ]
        if self in lst:
            return True
        return False

    def is_prevailing(self):
        lst = [TimeZone.APT, TimeZone.PPT, TimeZone.MPT, TimeZone.CPT, TimeZone.EPT]
        if self in lst:
            return True
        return False


class DatetimeRange:
    def __init__(
        self,
        start,
        end,
        frequency,
        leap_day_adjustment: LeapDayAdjustmentType,
        time_interval_type: TimeIntervalType,
    ):
        self.start = start
        self.end = end
        self.tzinfo = start.tzinfo
        self.frequency = frequency
        self.leap_day_adjustment = leap_day_adjustment
        self.time_interval_type = time_interval_type

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"(start={self.start}, end={self.end}, frequency={self.frequency}, "
            + f"leap_day_adjustment={self.leap_day_adjustment}, "
            + f"time_interval_type={self.time_interval_type})"
        )

    def __str__(self):
        return self.show_range()

    def show_range(self, n_show=5):
        output = self.list_time_range()
        n_show = min(len(output) // 2, n_show)
        n_head = ", ".join([str(x) for x in output[:n_show]])
        n_tail = ", ".join([str(x) for x in output[-n_show:]])
        return n_head + ",\n ... , \n" + n_tail

    def iter_timestamps(self):
        """Return a generator of datetimes for a time range ('start' and 'end' times are inclusive).
        TODO: for future-selves, test functionality of LeapDayAdjustmentType in relation to TimeIntervalType to make sure drop behavior is expected.

        Yields
        ------
        datetime

        """
        cur = self.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
        end = (
            self.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + self.frequency
        )  # to make end time inclusive

        while cur < end:
            month = cur.astimezone(self.tzinfo).month
            day = cur.astimezone(self.tzinfo).day
            if not (
                self.leap_day_adjustment == LeapDayAdjustmentType.DROP_FEB29
                and month == 2
                and day == 29
            ):
                if not (
                    self.leap_day_adjustment == LeapDayAdjustmentType.DROP_DEC31
                    and month == 12
                    and day == 31
                ):
                    if not (
                        self.leap_day_adjustment == LeapDayAdjustmentType.DROP_JAN1
                        and month == 1
                        and day == 1
                    ):
                        yield cur.astimezone(self.tzinfo)
            cur += self.frequency

    def list_time_range(self):
        """Return a list of timestamps (datetime obj) for a time range.
        Returns
        -------
        list
            list of datetime

        """
        return list(self.iter_timestamps())


class AnnualTimeRange(DatetimeRange):
    def iter_timestamps(self):
        """
        Return a list of years (datetime obj) on Jan 1st
        Might be okay to not convert to UTC for iteration, since it's annual

        """
        start = self.start.to_pydatetime()
        end = self.end.to_pydatetime()
        tz = self.tzinfo
        for year in range(start.year, end.year + 1):
            yield datetime.datetime(year=year, month=1, day=1, tzinfo=tz)


class NoOpTimeRange(DatetimeRange):
    def iter_timestamps(self):
        yield None


def make_time_range(start, end, frequency, leap_day_adjustment, time_interval_type):
    """
    factory function that decides which TimeRange func to use based on frequency
    """
    if frequency == datetime.timedelta(days=365):
        return AnnualTimeRange(start, end, frequency, leap_day_adjustment, time_interval_type)
    elif frequency == datetime.timedelta(days=0):
        return NoOpTimeRange(start, end, frequency, leap_day_adjustment, time_interval_type)
    return DatetimeRange(start, end, frequency, leap_day_adjustment, time_interval_type)
