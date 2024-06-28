"""Functions related to time"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
from typing import Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
    IntegerType,
)
import pyspark.sql.functions as F

import pandas as pd

from dsgrid.dimension.time import (
    DatetimeRange,
    IndexTimeRange,
    TimeZone,
    TimeIntervalType,
    TimeBasedDataAdjustmentModel,
    DaylightSavingFallBackType,
    LeapDayAdjustmentType,
    TimeDimensionType,
    adjust_timestamp_by_dst_offset,
)
from dsgrid.config.dimensions import TimeRangeModel
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.time.types import DatetimeTimestampType, IndexTimestampType
from dsgrid.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def get_dls_springforward_time_change_by_year(
    years: list[int], time_zone: Optional[TimeZone] = None
) -> dict:
    """Return the starting hour of daylight savings based on year(s),
    i.e., the spring forward timestamp (2AM in ST or 3AM in DT)."""

    tz_aware = True
    if time_zone is None:
        tz_aware = False
        time_zone = TimeZone.EPT
    elif time_zone.is_standard():
        # no daylight saving
        return {}
    time_zone_st = time_zone.get_standard_time()

    # Spring forward time - the missing hour
    timestamps = []
    for yr in years:
        cur_st = datetime(yr, 2, 28, 3, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        end_st = datetime(yr, 3, 31, 3, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        prev_st = cur_st - timedelta(days=1)
        while cur_st < end_st:
            cur = cur_st.astimezone(time_zone.tz)
            prev = prev_st.astimezone(time_zone.tz)
            if cur.dst() == timedelta(hours=1) and prev.dst() == timedelta(hours=0):
                spring_forward_hour = cur - cur.dst()  # 2AM in standard time
                timestamps.append(spring_forward_hour)
                break
            prev_st = cur_st
            cur_st += timedelta(days=1)

    if not tz_aware:
        timestamps = [ts.replace(tzinfo=None) for ts in timestamps]

    return dict(zip(years, timestamps))


def get_dls_springforward_time_change(
    year: int, time_zone: Optional[TimeZone] = None
) -> Optional[datetime]:
    dct = get_dls_springforward_time_change_by_year([year], time_zone=time_zone)
    if dct:
        val = list(dct.values())
        assert len(val) == 1
        return val[0]
    return


def get_dls_springforward_time_change_by_time_range(
    from_timestamp: datetime,
    to_timestamp: datetime,
    frequency: timedelta = timedelta(hours=1),
) -> list[datetime]:
    """Return all timestamps within the starting hour of daylight savings based on time range,
    e.g., the spring forward timestamp (2AM in ST or 3AM in DT).
    Note:
        1. Time range is inclusive of both edges.
        2. If frequency is None, return the 3AM DT (2AM ST), else, return timestamp based on frequency.
        3. If timestamps are not tz_aware, use EPT to extract time change.
        4. Returns [] if inputs are in standard time.
    """

    if from_timestamp.tzinfo != to_timestamp.tzinfo:
        raise ValueError(f"{from_timestamp=} and {to_timestamp=} do not have the same time zone.")

    tz_aware = True
    if from_timestamp.tzinfo is None:
        tz_aware = False
        from_timestamp = from_timestamp.replace(tzinfo=TimeZone.EPT.tz)
        to_timestamp = to_timestamp.replace(tzinfo=TimeZone.EPT.tz)
    tz = from_timestamp.tzinfo

    cur_utc = from_timestamp.astimezone(ZoneInfo("UTC"))
    end_utc = to_timestamp.astimezone(ZoneInfo("UTC"))
    assert cur_utc < end_utc, "Invalid time range"

    timestamps = []
    prev_utc = cur_utc - frequency
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
    years: list[int], time_zone: Optional[TimeZone] = None
) -> dict:
    """Return the ending hour of daylight savings based on year(s),
    i.e., fall back timestamp (1AM in ST)."""

    tz_aware = True
    if time_zone is None:
        tz_aware = False
        time_zone = TimeZone.EPT
    elif time_zone.is_standard():
        # no daylight saving
        return {}
    time_zone_st = time_zone.get_standard_time()

    # Fall back time - the duplicated hour (1AM)
    timestamps = []
    for yr in years:
        cur_st = datetime(yr, 10, 31, 2, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        end_st = datetime(yr, 11, 30, 2, 0, 0, tzinfo=time_zone.tz).astimezone(time_zone_st.tz)
        prev_st = cur_st - timedelta(days=1)
        while cur_st < end_st:
            cur = cur_st.astimezone(time_zone.tz)
            prev = prev_st.astimezone(time_zone.tz)
            if cur.dst() == timedelta(hours=0) and prev.dst() == timedelta(hours=1):
                fall_back_hour = cur  # 1AM in standard time
                timestamps.append(fall_back_hour)
                break
            prev_st = cur_st
            cur_st += timedelta(days=1)

    if not tz_aware:
        timestamps = [ts.replace(tzinfo=None) for ts in timestamps]

    return dict(zip(years, timestamps))


def get_dls_fallback_time_change(
    year: int, time_zone: Optional[TimeZone] = None
) -> Optional[datetime]:
    dct = get_dls_fallback_time_change_by_year([year], time_zone=time_zone)
    if dct:
        val = list(dct.values())
        assert len(val) == 1
        return val[0]
    return


def get_dls_fallback_time_change_by_time_range(
    from_timestamp: datetime,
    to_timestamp: datetime,
    frequency: timedelta = timedelta(hours=1),
) -> list[datetime]:
    """Return all timestamps within the ending hour of daylight savings based on time range,
    e.g., fall back timestamp (1AM in ST).
    Note:
        1. Time range is inclusive of both edges.
        2. If frequency is None, return the 1AM (in ST) timestamp, else, return timestamp based on frequency.
        3. If timestamps are not tz_aware, use EPT to extract time change.
        4. Returns [] if inputs are in standard time.
    """

    if from_timestamp.tzinfo != to_timestamp.tzinfo:
        raise ValueError(f"{from_timestamp=} and {to_timestamp=} do not have the same time zone.")

    tz_aware = True
    if from_timestamp.tzinfo is None:
        tz_aware = False
        from_timestamp = from_timestamp.replace(tzinfo=TimeZone.EPT.tz)
        to_timestamp = to_timestamp.replace(tzinfo=TimeZone.EPT.tz)

    tz = from_timestamp.tzinfo

    # Format time range
    cur_utc = from_timestamp.astimezone(ZoneInfo("UTC"))
    end_utc = to_timestamp.astimezone(ZoneInfo("UTC"))
    assert cur_utc < end_utc, "Invalid time range"

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


def build_time_ranges(
    time_ranges: TimeRangeModel,
    str_format: str,
    tz: Optional[TimeZone] = None,
):
    ranges = []
    for time_range in time_ranges:
        start = datetime.strptime(time_range.start, str_format)
        end = datetime.strptime(time_range.end, str_format)
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
        ranges.append((pd.Timestamp(start_adj, tz=tz), pd.Timestamp(end_adj, tz=tz)))

    ranges.sort(key=lambda x: x[0])
    return ranges


def get_time_ranges(
    time_dimension_config,  #: DateTimeDimensionConfig,
    timezone: TimeZone = None,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = None,
):
    dim_model = time_dimension_config.model
    if timezone is None:
        timezone = time_dimension_config.get_tzinfo()

    if dim_model.time_type == TimeDimensionType.DATETIME:
        dt_ranges = dim_model.ranges
    elif dim_model.time_type == TimeDimensionType.INDEX:
        dt_ranges = time_dimension_config._create_represented_time_ranges()
    else:
        msg = f"Cannot support time_dimension_config model of time_typ {dim_model.time_type}."
        raise ValueError(msg)

    ranges = []
    for start, end in build_time_ranges(dt_ranges, dim_model.str_format, tz=timezone):
        ranges.append(
            DatetimeRange(
                start=start,
                end=end,
                frequency=dim_model.frequency,
                time_based_data_adjustment=time_based_data_adjustment,
            )
        )

    return ranges


def get_index_ranges(
    time_dimension_config,  #: IndexTimeDimensionConfig,
    timezone: TimeZone = None,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = None,
):
    dim_model = time_dimension_config.model
    if timezone is None:
        timezone = dim_model.get_tzinfo()
    dt_ranges = time_dimension_config._create_represented_time_ranges()
    ranges = []
    time_ranges = build_time_ranges(dt_ranges, dim_model.str_format, tz=timezone)
    for index_range, time_range in zip(dim_model.ranges, time_ranges):
        ranges.append(
            IndexTimeRange(
                start=time_range[0],
                end=time_range[1],
                frequency=dim_model.frequency,
                time_based_data_adjustment=time_based_data_adjustment,
                start_index=index_range.start,
            )
        )

    return ranges


def list_timestamps(
    time_dimension_config,  #: DateTimeDimensionConfig,
    timezone: TimeZone = None,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = None,
):
    timestamps = []
    for time_range in get_time_ranges(
        time_dimension_config,
        timezone=timezone,
        time_based_data_adjustment=time_based_data_adjustment,
    ):
        timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
    return timestamps


def list_time_indices(
    time_dimension_config,  #: IndexTimeDimensionConfig,
    timezone: TimeZone = None,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = None,
):
    indices = []
    for index_range in get_index_ranges(
        time_dimension_config,
        timezone=timezone,
        time_based_data_adjustment=time_based_data_adjustment,
    ):
        indices += [IndexTimestampType(x) for x in index_range.list_time_range()]
    return indices


def build_index_time_map(
    time_dimension_config,  #: IndexTimeDimensionConfig,
    timezone=None,
    time_based_data_adjustment: Optional[TimeBasedDataAdjustmentModel] = None,
):
    time_col = time_dimension_config.get_load_data_time_columns()
    assert len(time_col) == 1, time_col
    time_col = time_col[0]
    indices = list_time_indices(
        time_dimension_config,
        timezone=timezone,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    timestamps = list_timestamps(
        time_dimension_config,
        timezone=timezone,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    ts_time_col = timestamps[0]._fields[0]
    schema = StructType(
        [
            StructField(time_col, IntegerType(), False),
            StructField(ts_time_col, TimestampType(), False),
        ]
    )
    data = []
    for a, b in zip(indices, timestamps):
        data.append((a[0], b[0]))
    df_time = get_spark_session().createDataFrame(data, schema=schema)

    return df_time


# def build_datetime_dataframe(
#     time_dimension_config, timezone=None, time_based_data_adjustment=None
# ):

#     time_col = time_dimension_config.get_load_data_time_columns()
#     assert len(time_col) == 1, time_col
#     time_col = time_col[0]
#     model_time = list_timestamps(
#         time_dimension_config,
#         timezone=timezone,
#         time_based_data_adjustment=time_based_data_adjustment,
#     )
#     schema = StructType([StructField(time_col, TimestampType(), False)])
#     df_time = get_spark_session().createDataFrame(model_time, schema=schema)
#     return df_time


# def build_index_time_dataframe(
#     time_dimension_config, timezone=None, time_based_data_adjustment=None
# ):

#     time_col = time_dimension_config.get_load_data_time_columns()
#     assert len(time_col) == 1, time_col
#     time_col = time_col[0]
#     model_time = list_time_indices(
#         time_dimension_config,
#         timezone=timezone,
#         time_based_data_adjustment=time_based_data_adjustment,
#     )
#     schema = StructType([StructField(time_col, IntegerType(), False)])
#     df_time = get_spark_session().createDataFrame(model_time, schema=schema)
#     return df_time


def create_adjustment_map_from_model_time(
    time_dimension_config,  #: IndexTimeDimensionConfig,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel,
    time_zone: TimeZone,
):
    """Create data adjustment mapping from model_time to prevailing time (timestamp) of input time_zone."""
    time_col = list(DatetimeTimestampType._fields)
    assert len(time_col) == 1, time_col
    time_col = time_col[0]
    ld_adj = time_based_data_adjustment.leap_day_adjustment
    fb_adj = time_based_data_adjustment.daylight_saving_adjustment.fall_back_hour

    TZ_st, TZ_pt = time_zone.get_standard_time(), time_zone.get_prevailing_time()
    ranges = get_time_ranges(
        time_dimension_config,
        timezone=TZ_pt.tz,
        time_based_data_adjustment=time_based_data_adjustment,
    )
    freq = time_dimension_config.model.frequency
    model_time, prevailing_time, multipliers = [], [], []
    for rg in ranges:
        cur_pt = rg.start.to_pydatetime()
        end_pt = rg.end.to_pydatetime()

        if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
            fb_times = get_dls_fallback_time_change_by_time_range(
                cur_pt, end_pt, frequency=freq
            )  # in PT
            fb_repeats = [0 for x in fb_times]

        cur = rg.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
        end = rg.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + freq

        while cur < end:
            multiplier = 1.0
            frequency = freq
            cur_pt = cur.astimezone(TZ_pt.tz)
            cur_pt = adjust_timestamp_by_dst_offset(cur_pt, freq)
            model_ts = cur_pt.replace(tzinfo=TZ_st.tz)
            month = cur_pt.month
            day = cur_pt.day
            if ld_adj == LeapDayAdjustmentType.DROP_FEB29 and month == 2 and day == 29:
                cur += frequency
                continue
            if ld_adj == LeapDayAdjustmentType.DROP_DEC31 and month == 12 and day == 31:
                cur += frequency
                continue
            if ld_adj == LeapDayAdjustmentType.DROP_JAN1 and month == 1 and day == 1:
                cur += frequency
                continue

            if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
                for i, ts in enumerate(fb_times):
                    if cur == ts.astimezone(ZoneInfo("UTC")):
                        if fb_repeats[i] == 0:
                            frequency = timedelta(hours=0)
                            multiplier = 0.5
                        elif fb_repeats[i] == 1:
                            model_ts = (
                                (cur + timedelta(hours=1))
                                .astimezone(TZ_pt.tz)
                                .replace(tzinfo=TZ_st.tz)
                            )
                            multiplier = 0.5
                        fb_repeats[i] += 1

            model_time.append(model_ts)
            prevailing_time.append(cur_pt)
            multipliers.append(multiplier)
            cur += frequency

    schema = StructType(
        [
            StructField("model_time", TimestampType(), False),
            StructField(time_col, TimestampType(), False),
            StructField("multiplier", DoubleType(), False),
        ]
    )
    table = get_spark_session().createDataFrame(
        zip(model_time, prevailing_time, multipliers), schema=schema
    )
    return table


def get_tzinfo_from_geography(geography_dim):
    """Get tzinfo from time_zone column of geography dimension record"""
    # TODO not currently in use
    geo_records = geography_dim.get_records_dataframe()
    geo_tz_values = [row.time_zone for row in geo_records.select("time_zone").distinct().collect()]
    geo_tzinfos = [TimeZone(tz).tz for tz in geo_tz_values]
    return geo_tzinfos


def filter_to_project_timestamps(df, project_time_dim):
    df_ptime = project_time_dim.build_time_dataframe()
    df = df_ptime.join(df, df_ptime.columns, "inner")
    return df


def shift_time_interval(
    df,
    time_column: str,
    from_time_interval: TimeIntervalType,
    to_time_interval: TimeIntervalType,
    time_step: timedelta,
    new_time_column: Optional[str] = None,
):
    """
    Shift time_column by time_step in df as needed by comparing from_time_interval
    to to_time_interval. If new_time_column is None, time_column is shifted in
    place, else shifted time is added as new_time_column in df.
    """
    assert (
        from_time_interval != to_time_interval
    ), f"{from_time_interval=} is the same as {to_time_interval=}"

    if new_time_column is None:
        new_time_column = time_column

    if TimeIntervalType.INSTANTANEOUS in (from_time_interval, to_time_interval):
        raise NotImplementedError(
            "aligning time intervals with instantaneous is not yet supported"
        )

    match (from_time_interval, to_time_interval):
        case (TimeIntervalType.PERIOD_BEGINNING, TimeIntervalType.PERIOD_ENDING):
            df = df.withColumn(
                new_time_column,
                F.col(time_column) + F.expr(f"INTERVAL {time_step.seconds} SECONDS"),
            )
        case (TimeIntervalType.PERIOD_ENDING, TimeIntervalType.PERIOD_BEGINNING):
            df = df.withColumn(
                new_time_column,
                F.col(time_column) - F.expr(f"INTERVAL {time_step.seconds} SECONDS"),
            )

    return df


def time_difference(df, project_time_dim, difference: str = "left"):
    """Compare the time col in df and project_time_dim"""

    time_col = project_time_dim.get_load_data_time_columns()
    assert len(time_col) == 1, time_col
    time_col = time_col[0]

    project_time = {
        row[0]
        for row in project_time_dim.build_time_dataframe().select(time_col).distinct().collect()
    }
    dataset_time = {
        row[0]
        for row in df.select(time_col).filter(f"{time_col} IS NOT NULL").distinct().collect()
    }
    if difference == "left":
        return dataset_time.difference(project_time)

    if difference == "right":
        return project_time.difference(dataset_time)

    if difference == "symmetric":
        return dataset_time.symmetric_difference(project_time)

    raise ValueError(f"Unsupported function input {difference=}")


def apply_time_wrap(df, project_time_dim, diff: set):
    """Apply time-wrapping"""

    time_col = project_time_dim.get_load_data_time_columns()
    assert len(time_col) == 1, time_col
    time_col = time_col[0]

    project_time = {
        row[time_col]
        for row in project_time_dim.build_time_dataframe().select(time_col).distinct().collect()
    }

    # extract time_delta based on if diff is to the left or right of project_time
    time_delta = (
        max(project_time) - min(project_time) + project_time_dim.get_frequency()
    ).total_seconds()
    if min(diff) > max(project_time):
        time_delta *= -1

    # time-wrap by "changing" the year with time_delta
    df = (
        df.filter(F.col(time_col).isin(diff))
        .withColumn(
            time_col,
            F.from_unixtime(F.unix_timestamp(time_col) + time_delta).cast("timestamp"),
        )
        .union(df.filter(~F.col(time_col).isin(diff)))
    )

    dataset_time = {
        row[0]
        for row in df.select(time_col).filter(f"{time_col} IS NOT NULL").distinct().collect()
    }
    # check
    if dataset_time.symmetric_difference(project_time):
        left_msg, right_msg = "", ""
        if left_diff := sorted(dataset_time.difference(project_time)):
            left_msg = f"\nProcessed dataset time contains {len(left_diff)} extra timestamp(s): {left_diff[:min(5,len(left_diff))]}"
        if right_diff := sorted(project_time.difference(dataset_time)):
            right_msg = f"\nProcessed dataset time is missing {len(right_diff)} timestamp(s): {right_diff[:min(5,len(right_diff))]}"
        raise DSGInvalidOperation(
            f"Dataset time cannot be processed to match project time. {left_msg}{right_msg}"
        )

    return df


def is_leap_year(year: int) -> bool:
    """Return True if the year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
