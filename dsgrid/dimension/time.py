"""Dimesions related to time"""

from enum import Enum
from dsgrid.data_models import Enum, DSGEnum, EnumValue

from pydantic.dataclasses import dataclass


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


class TimeFrequency(DSGEnum):
    # TODO: this is incomplete; good enough for first pass
    # TODO: it would be nice if this could be
    # TODO: do we want to support common frequency aliases, e.g.:
    # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
    _15_MIN = "15 min"
    _1_HOUR = "1 hour"
    _1_DAY = "1 day"
    _1_WEEK = "1 week"
    _1_MONTH = "1 month"
    _1_YEAR = "1 year"


class TimezoneType(DSGEnum):
    """Timezone enum types"""

    # TODO: TimezoneType enum is likely incomplete
    UTC = "UTC"
    PST = "PST"
    MST = "MST"
    CST = "CST"
    EST = "EST"
    LOCAL = "LOCAL"


@dataclass
class Timezone:
    # TODO: Timezone class  is likely incomplete
    id: str
    utc_offset: int
    includes_dst: bool
    tz: str


# TODO: move this to some kind of time module
# TODO: TIME_ZONE_MAPPING is incomplete
# EXAMPLE of applying time zone attributes to TimezoneType enum
TIME_ZONE_MAPPING = {
    TimezoneType.UTC: Timezone(id="UTC", utc_offset=0, includes_dst=False, tz="Etc/GMT+0"),
    TimezoneType.PST: Timezone(id="PST", utc_offset=-8, includes_dst=False, tz="Etc/GMT+8"),
    TimezoneType.MST: Timezone(id="MST", utc_offset=-7, includes_dst=False, tz="Etc/GMT+7"),
    TimezoneType.CST: Timezone(id="CST", utc_offset=-6, includes_dst=False, tz="Etc/GMT+6"),
    TimezoneType.EST: Timezone(id="EST", utc_offset=-5, includes_dst=False, tz="Etc/GMT+5"),
}
