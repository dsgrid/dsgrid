"""Functions related to time"""

from datetime import datetime

import logging


import pandas as pd

from dsgrid.dimension.time import (
    DatetimeRange,
    TimeBasedDataAdjustmentModel,
    TimeDimensionType,
)
from dsgrid.config.dimensions import TimeRangeModel, AnnualRangeModel


logger = logging.getLogger(__name__)


def build_annual_ranges(
    time_ranges: list[AnnualRangeModel],
    tz: str | None = None,
) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    ranges = []
    for time_range in time_ranges:
        start = datetime.strptime(time_range.start, time_range.str_format)
        end = datetime.strptime(time_range.end, time_range.str_format)
        assert isinstance(time_range.frequency, int)
        freq = time_range.frequency
        ranges.append((pd.Timestamp(start, tz=tz), pd.Timestamp(end, tz=tz), freq))

    ranges.sort(key=lambda x: x[0])
    return ranges


def build_time_ranges(
    time_ranges: list[TimeRangeModel],
    tz: str | None = None,
) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.Timedelta]]:
    ranges = []
    for time_range in time_ranges:
        start = datetime.strptime(time_range.start, time_range.str_format)
        end = datetime.strptime(time_range.end, time_range.str_format)
        start_adj = datetime(
            year=start.year,
            month=start.month,
            day=start.day,
            hour=start.hour,
            minute=start.minute,
            second=start.second,
            microsecond=start.microsecond,
        )
        end_adj = datetime(
            year=end.year,
            month=end.month,
            day=end.day,
            hour=end.hour,
            minute=end.minute,
            second=end.second,
            microsecond=end.microsecond,
        )
        freq = pd.Timedelta(time_range.frequency)
        ranges.append((pd.Timestamp(start_adj, tz=tz), pd.Timestamp(end_adj, tz=tz), freq))

    ranges.sort(key=lambda x: x[0])
    return ranges


def get_time_ranges(
    time_dimension_config,  #: DateTimeDimensionConfig,
    time_zone: str = None,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = None,
):
    dim_model = time_dimension_config.model
    if time_zone is None:
        time_zone = time_dimension_config.get_tzinfo()

    if dim_model.time_type == TimeDimensionType.DATETIME:
        dt_ranges = dim_model.ranges
    elif dim_model.time_type == TimeDimensionType.INDEX:
        dt_ranges = time_dimension_config._create_represented_time_ranges()
    else:
        msg = f"Cannot support time_dimension_config model of time_type {dim_model.time_type}."
        raise ValueError(msg)

    ranges = []
    for start, end, freq in build_time_ranges(dt_ranges, tz=time_zone):
        ranges.append(
            DatetimeRange(
                start=start,
                end=end,
                frequency=freq,
                time_based_data_adjustment=time_based_data_adjustment,
            )
        )

    return ranges


def is_leap_year(year: int) -> bool:
    """Return True if the year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
