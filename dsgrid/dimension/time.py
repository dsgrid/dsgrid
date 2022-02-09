"""Dimensions related to time"""
import datetime
import pytz

from dsgrid.data_models import DSGEnum, EnumValue


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


class TimeInvervalType(DSGEnum):
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
    """
    Time zone enum types
    Caveats:
    # Arizona, except tribal area is always in PST
    # Indiana has two timezones (Central and Eastern)

    pytz list of timezones:
    Pacific/Honolulu (HST)
    America/Anchorage (APT)
    America/Phoenix (PST)
    America/Los_Angeles (PPT)
    America/Denver (MPT)
    America/Chicago (CPT)
    America/New_York (EPT)
    """

    UTC = EnumValue(
        value="UTC",
        description="Coordinated Universal Time",
        tz=pytz.timezone("UTC"),
    )
    HST = EnumValue(
        value="HawaiiAleutianStandard",
        description="Hawaii Standard Time (UTC=-10). Does not include DST shifts.",
        tz=pytz.timezone("US/Hawaii"),
    )
    AST = EnumValue(
        value="AlaskaStandard",
        description="Alaskan Standard Time (UTC=-9). Does not include DST shifts.",
        tz=pytz.timezone("Etc/GMT+9"),
    )
    APT = EnumValue(
        value="AlaskaPrevailing",
        description="Alaska Prevailing Time. Commonly called Alaska Local Time. Includes DST"
        " shifts during DST times.",
        tz=pytz.timezone("US/Alaska"),
    )
    PST = EnumValue(
        value="PacificStandard",
        description="Pacific Standard Time (UTC=-8). Does not include DST shifts.",
        tz=pytz.timezone("Etc/GMT+8"),
    )
    PPT = EnumValue(
        value="PacificPrevailing",
        description="Pacific Prevailing Time. Commonly called Pacific Local Time. Includes DST"
        " shifts ,during DST times.",
        tz=pytz.timezone("US/Pacific"),
    )
    MST = EnumValue(
        value="MountainStandard",
        description="Mountain Standard Time (UTC=-7). Does not include DST shifts.",
        tz=pytz.timezone("Etc/GMT+7"),
    )
    MPT = EnumValue(
        value="MountainPrevailing",
        description="Mountain Prevailing Time. Commonly called Mountain Local Time. Includes DST"
        " shifts during DST times.",
        tz=pytz.timezone("US/Mountain"),
    )
    CST = EnumValue(
        value="CentralStandard",
        description="Central Standard Time (UTC=-6). Does not include DST shifts.",
        tz=pytz.timezone("Etc/GMT+6"),
    )
    CPT = EnumValue(
        value="CentralPrevailing",
        description="Central Prevailing Time. Commonly called Central Local Time. Includes DST"
        " shifts during DST times.",
        tz=pytz.timezone("US/Central"),
    )
    EST = EnumValue(
        value="EasternStandard",
        description="Eastern Standard Time (UTC=-5). Does not include DST shifts.",
        tz=pytz.timezone("Etc/GMT+5"),
    )
    EPT = EnumValue(
        value="EasternPrevailing",
        description="Eastern Prevailing Time. Commonly called Eastern Local Time. Includes DST"
        " shifts during DST times.",
        tz=pytz.timezone("US/Eastern"),
    )
    NONE = EnumValue(
        value="NONE",
        description="No timezone, suitable for temporally aggregated data",
        tz=None,
    )
    LOCAL = EnumValue(
        value="LOCAL",
        description="Local time. Implies that the geography's timezone will be dynamically applied"
        " when converting local time to other time zones.",
        tz=None,  # TODO: needs handling: DSGRID-171
    )


class DatetimeRange:
    def __init__(self, start, end, frequency, leap_day_adjustment: LeapDayAdjustmentType):
        self.start = start
        self.end = end
        self.frequency = frequency
        self.leap_day_adjustment = leap_day_adjustment

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"(start={self.start}, end={self.end}, frequency={self.frequency}, "
            + f"leap_day_adjustment={self.leap_day_adjustment})"
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

        cur = self.start.to_pydatetime()
        end = self.end.to_pydatetime() + self.frequency  # to make end time inclusive

        while cur < end:
            if not (
                self.leap_day_adjustment == LeapDayAdjustmentType.DROP_FEB29
                and cur.month == 2
                and cur.day == 29
            ):
                if not (
                    self.leap_day_adjustment == LeapDayAdjustmentType.DROP_DEC31
                    and cur.month == 12
                    and cur.day == 31
                ):
                    if not (
                        self.leap_day_adjustment == LeapDayAdjustmentType.DROP_JAN1
                        and cur.month == 1
                        and cur.day == 1
                    ):
                        yield cur
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
        """Return a list of years (datetime obj) on Jan 1st"""
        start = self.start.to_pydatetime()
        end = self.end.to_pydatetime()
        tz = start.tzinfo
        for year in range(start.year, end.year + 1):
            yield datetime.datetime(year=year, month=1, day=1, tzinfo=tz)


class NoOpTimeRange(DatetimeRange):
    def iter_timestamps(self):
        yield None


def make_time_range(start, end, frequency, leap_day_adjustment):
    """
    factory function that decides which TimeRange func to use based on frequency
    """
    if frequency == datetime.timedelta(days=365):
        return AnnualTimeRange(start, end, frequency, leap_day_adjustment)
    elif frequency == datetime.timedelta(days=0):
        return NoOpTimeRange(start, end, frequency, leap_day_adjustment)
    return DatetimeRange(start, end, frequency, leap_day_adjustment)


def find_time_delta(timestamp, from_tz: str, to_tz: str):
    """
    find datetime.timedelta (in seconds) between two time zones

    Returns
    --------
    int
        Difference between the two time zones in seconds.
    """

    time_delta = timestamp.replace(tzinfo=TimeZone(from_tz).tz) - timestamp.replace(
        tzinfo=TimeZone(to_tz).tz
    )

    return time_delta.total_seconds()
