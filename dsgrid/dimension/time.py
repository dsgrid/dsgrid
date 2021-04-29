"""Dimesions related to time"""

from enum import Enum

from pydantic.dataclasses import dataclass


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


class TimeFrequency(Enum):
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


class TimezoneType(Enum):
    """Timezone enum types"""

    # TODO: TimezoneType enum is likely incomplete
    UTC = "UTC"
    PST = "PST"
    MST = "MST"
    CST = "CST"
    EST = "EST"


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
