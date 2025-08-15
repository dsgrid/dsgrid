"""Dimensions related to time"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import logging
from pydantic import Field
from enum import Enum


from dsgrid.data_models import DSGEnum, EnumValue, DSGBaseModel

logger = logging.getLogger(__name__)


class TimeDimensionType(DSGEnum):
    """Defines the supported time formats in the load data."""

    DATETIME = "datetime"
    ANNUAL = "annual"
    REPRESENTATIVE_PERIOD = "representative_period"
    INDEX = "index"
    NOOP = "noop"


class DatetimeFormat(str, Enum):
    """Defines the time format of the datetime config model"""

    ALIGNED = "aligned"
    LOCAL = "local"
    LOCAL_AS_STRINGS = "local_as_strings"


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
        tz_name="Etc/GMT+10",
    )
    AST = EnumValue(
        value="AlaskaStandard",
        description="Alaskan Standard Time (UTC=-9). No daylight saving shifts.",
        tz=ZoneInfo("Etc/GMT+9"),
        tz_name="Etc/GMT+9",
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
        tz_name="Etc/GMT+8",
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
        tz_name="Etc/GMT+7",
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
        tz_name="Etc/GMT+6",
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
        tz_name="Etc/GMT+5",
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
        msg = f"BUG: case not covered: {self}"
        raise NotImplementedError(msg)

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
        msg = f"BUG: case not covered: {self}"
        raise NotImplementedError(msg)

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


_TIME_ZONE_NAME_TO_ZONE_INFO = {x.tz_name: x.tz for x in TimeZone}

assert len(_TIME_ZONE_NAME_TO_ZONE_INFO) == len(TimeZone)


def get_zone_info_from_tz_name(tz_name: str) -> ZoneInfo:
    """Return the ZoneInfo matching tz_name."""
    return _TIME_ZONE_NAME_TO_ZONE_INFO[tz_name]


def get_zone_info_from_spark_session(tz_name: str) -> ZoneInfo:
    """Return the ZoneInfo matching tz_name, which must have been read from the Spark session."""
    try:
        # We set the Spark session time zone to tz_name which is incompatible with ZoneInfo.
        return ZoneInfo(key=tz_name)
    except ZoneInfoNotFoundError:
        return get_zone_info_from_tz_name(tz_name)


class DaylightSavingAdjustmentModel(DSGBaseModel):
    """Defines how to drop and add data along with timestamps to convert standard time
    load profiles to clock time"""

    spring_forward_hour: DaylightSavingSpringForwardType = Field(
        title="spring_forward_hour",
        description="Data adjustment for spring forward hour (a 2AM in March)",
        default=DaylightSavingSpringForwardType.NONE,
        json_schema_extra={
            "options": DaylightSavingSpringForwardType.format_descriptions_for_docs(),
        },
    )

    fall_back_hour: DaylightSavingFallBackType = Field(
        title="fall_back_hour",
        description="Data adjustment for spring forward hour (a 2AM in November)",
        default=DaylightSavingFallBackType.NONE,
        json_schema_extra={
            "options": DaylightSavingFallBackType.format_descriptions_for_docs(),
        },
    )


class TimeBasedDataAdjustmentModel(DSGBaseModel):
    """Defines how data needs to be adjusted with respect to time.
    For leap day adjustment, up to one full day of timestamps and data are dropped.
    For daylight savings, the dataframe is adjusted alongside the timestamps.
    This is useful when the load profiles are modeled in standard time and
    need to be converted to get clock time load profiles.
    """

    leap_day_adjustment: LeapDayAdjustmentType = Field(
        default=LeapDayAdjustmentType.NONE,
        title="leap_day_adjustment",
        description="Leap day adjustment method applied to time data. The dsgrid default is None, "
        "i.e., no adjustment made to leap years. Adjustments are made to leap years only.",
    )
    daylight_saving_adjustment: DaylightSavingAdjustmentModel = Field(
        title="daylight_saving_adjustment",
        description="Daylight saving adjustment method applied to time data",
        default=DaylightSavingAdjustmentModel(
            spring_forward_hour=DaylightSavingSpringForwardType.NONE,
            fall_back_hour=DaylightSavingFallBackType.NONE,
        ),
    )


class DatetimeRange:
    def __init__(
        self,
        start,
        end,
        frequency,
        time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
    ):
        if time_based_data_adjustment is None:
            time_based_data_adjustment = TimeBasedDataAdjustmentModel()
        self.start = start
        self.end = end
        self.tzinfo = start.tzinfo
        self.frequency = frequency
        self.leap_day_adjustment = time_based_data_adjustment.leap_day_adjustment
        self.dls_springforward_adjustment = (
            time_based_data_adjustment.daylight_saving_adjustment.spring_forward_hour
        )
        self.dls_fallback_adjustment = (
            time_based_data_adjustment.daylight_saving_adjustment.fall_back_hour
        )

    def __repr__(self):
        return (
            self.__class__.__qualname__
            + f"(start={self.start}, end={self.end}, frequency={self.frequency}, "
            + f"leap_day_adjustment={self.leap_day_adjustment}, "
            + f"dls_springforward_adjustment={self.dls_springforward_adjustment}, "
            + f"dls_fallback_adjustment={self.dls_fallback_adjustment}."
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
            cur_tz = cur.astimezone(self.tzinfo)
            cur_tz = adjust_timestamp_by_dst_offset(cur_tz, self.frequency)
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

            cur += self.frequency

    def list_time_range(self):
        """Return a list of timestamps for a time range.

        Returns
        -------
        list[datetime]
        """
        return list(self._iter_timestamps())


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
        start_index,
        time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
    ):
        super().__init__(
            start, end, frequency, time_based_data_adjustment=time_based_data_adjustment
        )
        self.start_index = start_index

    def _iter_timestamps(self):
        cur = self.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
        cur_idx = self.start_index
        end = (
            self.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + self.frequency
        )  # to make end time inclusive

        while cur < end:
            cur_tz = cur.astimezone(self.tzinfo)
            cur_tz = adjust_timestamp_by_dst_offset(cur_tz, self.frequency)
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
            cur += self.frequency
            cur_idx += 1


def adjust_timestamp_by_dst_offset(timestamp, frequency):
    """Reduce the timestamps within the daylight saving range by 1 hour.
    Used to ensure that a time series at daily (or lower) frequency returns each day at the
    same timestamp in prevailing time, an expected behavior in most standard libraries.
    (e.g., ensure a time series can return 2018-03-11 00:00, 2018-03-12 00:00...
    instead of 2018-03-11 00:00, 2018-03-12 01:00...)

    """
    if frequency < timedelta(hours=24):
        return timestamp

    offset = timestamp.dst() or timedelta(hours=0)
    return timestamp - offset
