"""Dimensions related to time"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
from typing_extensions import Annotated
from typing import Union, Optional
from pydantic import Field

from dsgrid.data_models import DSGEnum, EnumValue, DSGBaseModel

logger = logging.getLogger(__name__)


class TimeDimensionType(DSGEnum):
    """Defines the supported time formats in the load data."""

    DATETIME = "datetime"
    ANNUAL = "annual"
    REPRESENTATIVE_PERIOD = "representative_period"
    INDEXED = "indexed"
    NOOP = "noop"


class RepresentativePeriodFormat(DSGEnum):
    """Defines the supported formats for representative period data."""

    # All instances of this Enum must declare frequency.
    # This Enum may be replaced by a generic implementation in order to support a large
    # number of permutations (seasons, weekend day vs week day, sub-hour time, etc).

    ONE_WEEK_PER_MONTH_BY_HOUR = EnumValue(
        value="one_week_per_month_by_hour",
        frequency=timedelta(hours=1),
        description="load_data columns use 'month', 'day_of_week', 'hour' to specify time",
    )
    ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR = EnumValue(
        value="one_weekday_day_and_one_weekend_day_per_month_by_hour",
        frequency=timedelta(hours=1),
        description="load_data columns use 'month', 'hour', 'is_weekday' to specify time",
    )


class LeapDayAdjustmentType(DSGEnum):
    """Leap day adjustment enum types"""

    DROP_DEC31 = EnumValue(
        value="drop_dec31",
        description="To adjust for leap years, December 31st timestamps and data get dropped.",
    )
    DROP_FEB29 = EnumValue(
        value="drop_feb29",
        description="Feburary 29th timestamps and data are dropped. Currently not yet supported by dsgrid.",
    )
    DROP_JAN1 = EnumValue(
        value="drop_jan1",
        description="To adjust for leap years, January 1st timestamps and data get dropped.",
    )
    NONE = EnumValue(value="none", description="No leap day adjustment made.")


class DaylightSavingSpringForwardType(DSGEnum):
    """Daylight saving spring forward adjustment enum types"""

    DROP = EnumValue(
        value="drop",
        description="Drop timestamp(s) and associated data for the spring forward hour (2AM in March)",
    )
    NONE = EnumValue(value="none", description="No daylight saving adjustment for data.")


class DaylightSavingFallBackType(DSGEnum):
    """Daylight saving fall back adjustment enum types"""

    INTERPOLATE = EnumValue(
        value="interpolate",
        description="Fill data by interpolating between the left and right edges of the dataframe.",
    )
    DUPLICATE = EnumValue(
        value="duplicate",
        description="Fill data by duplicating the fall-back hour (1AM in November)",
    )
    NONE = EnumValue(value="none", description="No daylight saving adjustment for data.")


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
        description="Hawaii Standard Time (UTC=-10). No daylight saving shifts.",
        tz=ZoneInfo("US/Hawaii"),
        tz_name="-10:00",
    )
    AST = EnumValue(
        value="AlaskaStandard",
        description="Alaskan Standard Time (UTC=-9). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+9"),
        tz_name="-09:00",
    )
    APT = EnumValue(
        value="AlaskaPrevailing",
        description="Alaska Prevailing Time. Commonly called Alaska Local Time. "
        "Includes daylight saving.",
        tz=ZoneInfo("US/Alaska"),
        tz_name="US/Alaska",
    )
    PST = EnumValue(
        value="PacificStandard",
        description="Pacific Standard Time (UTC=-8). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+8"),
        tz_name="-08:00",
    )
    PPT = EnumValue(
        value="PacificPrevailing",
        description="Pacific Prevailing Time. Commonly called Pacific Local Time. "
        "Includes daylight saving.",
        tz=ZoneInfo("US/Pacific"),
        tz_name="US/Pacific",
    )
    MST = EnumValue(
        value="MountainStandard",
        description="Mountain Standard Time (UTC=-7). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+7"),
        tz_name="-07:00",
    )
    MPT = EnumValue(
        value="MountainPrevailing",
        description="Mountain Prevailing Time. Commonly called Mountain Local Time. "
        "Includes daylight saving.",
        tz=ZoneInfo("US/Mountain"),
        tz_name="US/Mountain",
    )
    CST = EnumValue(
        value="CentralStandard",
        description="Central Standard Time (UTC=-6). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+6"),
        tz_name="-06:00",
    )
    CPT = EnumValue(
        value="CentralPrevailing",
        description="Central Prevailing Time. Commonly called Central Local Time. "
        "Includes daylight saving.",
        tz=ZoneInfo("US/Central"),
        tz_name="US/Central",
    )
    EST = EnumValue(
        value="EasternStandard",
        description="Eastern Standard Time (UTC=-5). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+5"),
        tz_name="-05:00",
    )
    EPT = EnumValue(
        value="EasternPrevailing",
        description="Eastern Prevailing Time. Commonly called Eastern Local Time. "
        "Includes daylight saving.",
        tz=ZoneInfo("US/Eastern"),
        tz_name="US/Eastern",
    )
    ARIZONA = EnumValue(
        value="USArizona",
        description="US/Arizona = Mountain Standard Time (UTC=-7). No daylight saving shifts. "
        "For Arizona state except Navajo County",
        tz=ZoneInfo("US/Arizona"),
        tz_name="US/Arizona",
    )  # for Arizona except Navajo County
    LOCAL = EnumValue(
        value="Local",
        description="Local Time, where the time zone is defined by a geography dimension.",
        tz=None,
        tz_name="Local",
    )
    LOCAL_MODEL = EnumValue(
        value="LocalModel",
        description="Local Model Time. Clock time local to a geography dimension but laid out "
        "like Standard Time, requires daylight saving adjustment to both time and data.",
        tz=None,
        tz_name="LocalModel",
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
        if self == TimeZone.ARIZONA:
            return TimeZone.ARIZONA
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
        if self == TimeZone.ARIZONA:
            return TimeZone.ARIZONA
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
            TimeZone.ARIZONA,
        ]
        if self in lst:
            return True
        return False

    def is_prevailing(self):
        lst = [
            TimeZone.APT,
            TimeZone.PPT,
            TimeZone.MPT,
            TimeZone.CPT,
            TimeZone.EPT,
            TimeZone.ARIZONA,
        ]
        if self in lst:
            return True
        return False


def get_dls_springforward_time_change_by_year(
    year: Union[int, list[int]], time_zone: TimeZone
) -> list[datetime]:
    """Return the start of daylight savings based on year,
    i.e., the spring forward timestamp (2AM in ST or 3AM in DT)."""

    if time_zone in [TimeZone.LOCAL, TimeZone.LOCAL_MODEL]:
        raise ValueError(f"{time_zone=} cannot be local.")
    if time_zone.is_standard():
        # no daylight saving
        return []
    time_zone_st = time_zone.get_standard_time()

    if isinstance(year, int):
        year = [year]

    # Spring forward time - the missing hour
    timestamps = []
    for yr in year:
        cur_st = datetime(yr, 2, 28, 3, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        end_st = datetime(yr, 3, 31, 3, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        prev_st = cur_st - timedelta(days=1)
        while cur_st < end_st:
            cur = cur_st.astimezone(time_zone.tz)
            prev = prev_st.astimezone(time_zone.tz)
            if cur.dst() == timedelta(hours=1) and prev.dst() == timedelta(hours=0):
                spring_forward_hour = cur - timedelta(hours=1)  # 2AM in standard time
                timestamps.append(spring_forward_hour)
            prev_st = cur_st
            cur_st += timedelta(days=1)

    return timestamps


def get_dls_springforward_time_change_by_time_range(
    from_timestamp: datetime,
    to_timestamp: datetime,
    frequency: Optional[timedelta] = None,
) -> list[datetime]:
    """Return the start of daylight savings based on time range,
    i.e., the spring forward timestamp (2AM in ST or 3AM in DT).
    Note:
        1. Time range is inclusive of both edges.
        2. If frequency is None, return the 3AM DT (2AM ST), else, return timestamp based on frequency.
        3. If timestamps are not tz_aware, use EPT to extract time change.
        4. Returns [] if inputs are in standard time.
    """

    if from_timestamp.tzinfo != to_timestamp.tzinfo:
        raise ValueError(f"{from_timestamp=} and {to_timestamp=} do not have the same time zone.")
    tz = from_timestamp.tzinfo

    tz_aware = True
    if from_timestamp.tzinfo is None:
        tz_aware = False
        from_timestamp = from_timestamp.replace(tzinfo=TimeZone.EPT.tz)
        to_timestamp = to_timestamp.replace(tzinfo=TimeZone.EPT.tz)

    cur_utc = from_timestamp.astimezone(ZoneInfo("UTC"))
    end_utc = to_timestamp.astimezone(ZoneInfo("UTC"))
    assert cur_utc < end_utc, "Invalid time range"
    if frequency is None:
        frequency = timedelta(hours=1)
        if cur_utc.minute > 0 or cur_utc.second > 0 or cur_utc.microsecond > 0:
            # round down to the nearest hour
            cur_utc = cur_utc.replace(minute=0, second=0, microsecond=0)

    timestamps = []
    prev_utc = cur_utc
    sf_start = None
    while cur_utc < end_utc:
        cur, prev = cur_utc.astimezone(tz), prev_utc.astimezone(tz)
        if cur.month == 3 and cur.hour == 3:
            if cur.dst() == timedelta(hours=1) and prev.dst() == timedelta(hours=0):
                sf_start = cur
                timestamps.append(cur)
            elif sf_start is not None and sf_start.day == cur.day:
                timestamps.append(cur)
        prev_utc = cur_utc
        cur_utc += frequency

    if not tz_aware:
        timestamps = [ts.replace(tzinfo=None) for ts in timestamps]

    return timestamps


def get_dls_fallback_time_change_by_year(
    year: Union[int, list[int]], time_zone: TimeZone
) -> list[datetime]:
    """Return the end of daylight savings based on year,
    i.e., fall back timestamp (1AM in ST)."""

    if time_zone in [TimeZone.LOCAL, TimeZone.LOCAL_MODEL]:
        raise ValueError(f"{time_zone=} cannot be local.")
    if time_zone.is_standard():
        # no daylight saving
        return []
    time_zone_st = time_zone.get_standard_time()

    if isinstance(year, int):
        year = [year]

    # Fall back time - the duplicated hour (1AM)
    timestamps = []
    for yr in year:
        cur_st = datetime(yr, 10, 31, 2, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        end_st = datetime(yr, 11, 30, 2, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        prev_st = cur_st - timedelta(days=1)
        while cur_st < end_st:
            cur = cur_st.astimezone(time_zone.tz)
            prev = prev_st.astimezone(time_zone.tz)
            if cur.dst() == timedelta(hours=0) and prev.dst() == timedelta(hours=1):
                fall_back_hour = cur  # 1AM in standard time
                timestamps.append(fall_back_hour)
            prev_st = cur_st
            cur_st += timedelta(days=1)

    return timestamps


def get_dls_fallback_time_change_by_time_range(
    from_timestamp: datetime,
    to_timestamp: datetime,
    frequency: Optional[timedelta] = None,
) -> list[datetime]:
    """Return the end of daylight savings based on year,
    i.e., fall back timestamp (1AM in ST).
    Note:
        1. Time range is inclusive of both edges.
        2. If frequency is None, return the 1AM (in ST) timestamp, else, return timestamp based on frequency.
        3. If timestamps are not tz_aware, use EPT to extract time change.
        4. Returns [] if inputs are in standard time.
    """

    if from_timestamp.tzinfo != to_timestamp.tzinfo:
        raise ValueError(f"{from_timestamp=} and {to_timestamp=} do not have the same time zone.")
    tz = from_timestamp.tzinfo

    tz_aware = True
    if from_timestamp.tzinfo is None:
        tz_aware = False
        from_timestamp = from_timestamp.replace(tzinfo=TimeZone.EPT.tz)
        to_timestamp = to_timestamp.replace(tzinfo=TimeZone.EPT.tz)

    # Format time range
    cur_utc = from_timestamp.astimezone(ZoneInfo("UTC"))
    end_utc = to_timestamp.astimezone(ZoneInfo("UTC"))
    assert cur_utc < end_utc, "Invalid time range"
    if frequency is None:
        frequency = timedelta(hours=1)
        if cur_utc.minute > 0 or cur_utc.second > 0 or cur_utc.microsecond > 0:
            # round down to the nearest hour
            cur_utc = cur_utc.replace(minute=0, second=0, microsecond=0)

    timestamps = []
    prev_utc = cur_utc - frequency
    fb_start = None
    while cur_utc < end_utc:
        cur, prev = cur_utc.astimezone(tz), prev_utc.astimezone(tz)
        if cur.month == 11 and cur.hour == 1:
            if cur.dst() == timedelta(hours=0) and prev.dst() == timedelta(hours=1):
                fb_start = cur
                timestamps.append(cur)
            elif fb_start is not None and fb_start.day == cur.day:
                timestamps.append(cur)
        prev_utc = cur_utc
        cur_utc += frequency

    if not tz_aware:
        timestamps = [ts.replace(tzinfo=None) for ts in timestamps]
    return timestamps


class DaylightSavingAdjustmentModel(DSGBaseModel):
    """Defines how to drop and add data along with timestamps to convert standard time
    load profiles to clock time"""

    spring_forward_hour: Annotated[
        DaylightSavingSpringForwardType,
        Field(
            title="spring_forward_hour",
            description="Data adjustment for spring forward hour (a 2AM in March)",
            default=DaylightSavingSpringForwardType.DROP,
            json_schema_extra={
                "options": DaylightSavingSpringForwardType.format_descriptions_for_docs(),
            },
        ),
    ]
    fall_back_hour: Annotated[
        DaylightSavingFallBackType,
        Field(
            title="fall_back_hour",
            description="Data adjustment for spring forward hour (a 2AM in November)",
            default=DaylightSavingFallBackType.INTERPOLATE,
            json_schema_extra={
                "options": DaylightSavingFallBackType.format_descriptions_for_docs(),
            },
        ),
    ]


class DataAdjustmentModel(DSGBaseModel):
    """Defines how data needs to be adjusted with respect to time.
    For leap day adjustment, up to one full day of timestamps and data are dropped.
    For daylight savings, the dataframe is adjusted alongside the timestamps.
    This is useful when the load profiles are modeled in standard time and
    need to be converted to get clock time load profiles.
    """

    leap_day_adjustment: Annotated[
        LeapDayAdjustmentType,
        Field(
            title="leap_day_adjustment",
            description="Leap day adjustment method applied to time data",
            default=LeapDayAdjustmentType.NONE,
            json_schema_extra={
                "options": LeapDayAdjustmentType.format_descriptions_for_docs(),
                "notes": (
                    "The dsgrid default is None, i.e., no adjustment made to leap years.",
                    "Adjustments are made to leap years only.",
                ),
            },
        ),
    ]
    daylight_saving_adjustment: Annotated[
        DaylightSavingAdjustmentModel,
        Field(
            title="daylight_saving_adjustment",
            description="Daylight saving adjustment method applied to time data",
            default={
                "spring_forward_hour": DaylightSavingSpringForwardType.NONE,
                "fall_back_hour": DaylightSavingFallBackType.NONE,
            },
        ),
    ]


class DatetimeRange:
    def __init__(
        self,
        start,
        end,
        frequency,
        data_adjustment: DataAdjustmentModel,
        time_interval_type: TimeIntervalType,
    ):
        self.start = start
        self.end = end
        self.tzinfo = start.tzinfo
        self.frequency = frequency
        self.leap_day_adjustment = data_adjustment.leap_day_adjustment
        self.dls_springforward_adjustment = (
            data_adjustment.daylight_saving_adjustment.spring_forward_hour
        )
        self.dls_fallback_adjustment = data_adjustment.daylight_saving_adjustment.fall_back_hour
        self.time_interval_type = time_interval_type

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"(start={self.start}, end={self.end}, frequency={self.frequency}, "
            + f"leap_day_adjustment={self.leap_day_adjustment}, "
            + f"dls_springforward_adjustment={self.dls_springforward_adjustment}, "
            + f"dls_fallback_adjustment={self.dls_fallback_adjustment}, "
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

    def _iter_timestamps(self):
        """Return a generator of datetimes for a time range ('start' and 'end' times are inclusive).
        There could be duplicates.

        TODO: for future-selves, test functionality of LeapDayAdjustmentType in relation to TimeIntervalType to make sure drop behavior is expected.

        Yields
        ------
        datetime

        """
        cur = self.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
        end = self.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + self.frequency

        while cur < end:
            frequency = self.frequency
            cur_tz = cur.astimezone(self.tzinfo)
            month = cur_tz.month
            day = cur_tz.day
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
                        yield cur_tz

            cur += frequency

    def list_time_range(self):
        """Return a list of timestamps for a time range.

        Returns
        -------
        list[datetime]
        """
        return list(self._iter_timestamps())
        # return sorted(set(self._iter_timestamps()))


class AnnualTimeRange(DatetimeRange):
    def _iter_timestamps(self):
        """
        Return a list of years (datetime obj) on Jan 1st
        Might be okay to not convert to UTC for iteration, since it's annual

        """
        start = self.start.to_pydatetime()
        end = self.end.to_pydatetime()
        tz = self.tzinfo
        for year in range(start.year, end.year + 1):
            yield datetime(year=year, month=1, day=1, tzinfo=tz)


class IndexTimeRange(DatetimeRange):
    def __init__(
        self,
        start,
        end,
        frequency,
        leap_day_adjustment: LeapDayAdjustmentType,
        time_interval_type: TimeIntervalType,
        start_index,
        step,
    ):
        super().__init__(start, end, frequency, leap_day_adjustment, time_interval_type)
        self.start_index = start_index
        self.step = step

    def _iter_timestamps(self):
        cur = self.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
        cur_idx = self.start_index
        end = (
            self.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + self.frequency
        )  # to make end time inclusive

        while cur < end:
            frequency = self.frequency
            step = self.step
            cur_tz = cur.astimezone(self.tzinfo)
            month = cur_tz.month
            day = cur_tz.day
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
                        yield cur_idx
            cur += frequency
            cur_idx += step


class NoOpTimeRange(DatetimeRange):
    def _iter_timestamps(self):
        yield None


def make_time_range(
    start, end, frequency, data_adjustment, time_interval_type, start_index=None, step=None
):
    """
    factory function that decides which TimeRange func to use based on frequency
    """
    if start_index is not None or step is not None:
        return IndexTimeRange(
            start, end, frequency, data_adjustment, time_interval_type, start_index, step
        )
    if frequency == timedelta(days=365):
        return AnnualTimeRange(start, end, frequency, data_adjustment, time_interval_type)
    elif frequency == timedelta(days=0):
        return NoOpTimeRange(start, end, frequency, data_adjustment, time_interval_type)
    return DatetimeRange(start, end, frequency, data_adjustment, time_interval_type)
