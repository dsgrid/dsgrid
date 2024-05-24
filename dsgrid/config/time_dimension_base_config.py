import abc
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
)

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import TimeZone, TimeIntervalType
from dsgrid.exceptions import DSGInvalidOperation, DSGInvalidDimension
from dsgrid.config.dimensions import TimeRangeModel
from dsgrid.dimension.time import (
    DataAdjustmentModel,
    LeapDayAdjustmentType,
    DaylightSavingFallBackType,
    get_dls_fallback_time_change_by_time_range,
)
from dsgrid.time.types import DatetimeTimestampType
from dsgrid.utils.spark import get_spark_session


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles, abc.ABC):
    """Base class for all time dimension configs"""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, load_data_df, time_columns: list[str]):
        """Check consistency of the load data with the time dimension.

        Parameters
        ----------
        load_data_df : pyspark.sql.DataFrame
        time_columns : list[str]

        Raises
        ------
        DSGInvalidDataset
            Raised if the dataset is inconsistent with the time dimension.

        """

    @abc.abstractmethod
    def build_time_dataframe(
        self,
        model_years: Optional[list[int]] = None,
        timezone: Optional[ZoneInfo] = None,
        data_adjustment: Optional[DataAdjustmentModel] = None,
    ):
        """Build time dimension as specified in config in a spark dataframe.

        Parameters
        ----------
        model_years : None | list[int]
            If specified, repeat the timestamps for each model year.

        Returns
        -------
        pyspark.sql.DataFrame

        """

    # @abc.abstractmethod
    # def build_time_dataframe_with_time_zone(self):
    #     """Build time dataframe so that relative to spark.sql.session.timeZone, it
    #     appears as expected in config time zone.
    #     Notes: the converted time will need to be converted back to session.timeZone
    #        so spark can intepret it correctly when saving to file in UTC.
    #     Returns
    #     -------
    #     pyspark.sql.DataFrame

    #     """

    @abc.abstractmethod
    def convert_dataframe(
        self,
        df,
        project_time_dim,
        model_years: Optional[list[int]] = None,
        value_columns: Optional[set[str]] = None,
        wrap_time_allowed: bool = False,
    ):
        """Convert input df to use project's time format and time zone.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        project_time_dim : TimeDimensionBaseConfig
        model_years : None | list[int]
            Model years required by the project. Not required for all time dimension types.
        value_columns : None | set[str]
            Columns in the dataframe that represent load values. Not required for all time
            dimension types.
        wrapped_time_allowed : bool
            Whether to allow time-wrapping to align time zone

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @abc.abstractmethod
    def get_frequency(self):
        """Return the frequency.

        Returns
        -------
        timedelta

        """

    @abc.abstractmethod
    def get_load_data_time_columns(self) -> list[str]:
        """Return the required timestamp columns in the load data table.

        Returns
        -------
        list

        """

    def list_load_data_columns_for_query_name(self) -> list[str]:
        """Return the time columns expected in the load data table for this dimension's query name.

        Returns
        -------
        list[str]

        """
        # This may need to be re-implemented by child classes.
        return [self.model.dimension_query_name]

    def map_timestamp_load_data_columns_for_query_name(self, df):
        """Map the timestamp columns in the load data table to those specified by the query name.

        Parameters
        ----------
        df : pyspark.sql.DataFrame

        Returns
        -------
        pyspark.sql.DataFrame

        """
        time_cols = self.get_load_data_time_columns()
        if len(time_cols) > 1:
            raise NotImplementedError(
                "Handling of multiple time columns needs to be implemented in the child class: "
                f"{type(self)}: {time_cols=}"
            )

        return df.withColumnRenamed(time_cols[0], self.model.dimension_query_name)

    @abc.abstractmethod
    def get_time_ranges(
        self,
        model_years: Optional[list[int]] = None,
        timezone: Optional[ZoneInfo] = None,
        data_adjustment: Optional[DataAdjustmentModel] = None,
    ):
        """Return time ranges with timezone applied.

        Parameters
        ----------
        model_years : None | list[int]
            If set, replace the base year in the time ranges with these model years. In this case
            each range must be in the same year.
        timezone : None | ZoneInfo
            If set, override the self timezone info when creating the time ranges

        Returns
        -------
        list
            list of DatetimeRange

        """

    @abc.abstractmethod
    def get_tzinfo(self):
        """Return a tzinfo instance for this dimension.

        Returns
        -------
        tzinfo | None

        """

    @abc.abstractmethod
    def get_time_interval_type(self):
        """Return the time interval type for this dimension.

        Returns
        -------
        TimeIntervalType | None

        """

    @abc.abstractmethod
    def list_expected_dataset_timestamps(self, model_years: Optional[list[int]] = None):
        """Return a list of the timestamps expected in the load_data table.

        Parameters
        ----------
        model_years : None | list[int]
            If set, replace the base year in the time ranges with these model years. In this case
            each range must be in the same year.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """

    def _convert_time_to_project_time_interval(
        self, df, project_time_dim=None, wrap_time: bool = False
    ):
        """
        Shift time to match project time based on TimeIntervalType, time zone,
        and other attributes.
        - Time-wrapping is applied as needed as part of align_time_interval_type
        - Separately, input wrap_time allows time-wrapping to be applied if dataset
        has a different time zone than project. wrap_time_allowed is specified by
        the project's InputDatasetModel.
        """
        if project_time_dim is None:
            return df

        df = self._align_time_interval_type(df, project_time_dim)

        if wrap_time:
            diff = self._time_difference(df, project_time_dim, difference="symmetric")
            df = self._apply_time_wrap(df, project_time_dim, diff)

        return df

    def _align_time_interval_type(self, df, project_time_dim):
        """Align time interval type between df and project_time_dim.
        If time range spills over into another year after time interval alignment,
        the time range will be wrapped around so it's bounded within the year.
        """
        dtime_interval = self.get_time_interval_type()
        ptime_interval = project_time_dim.get_time_interval_type()
        time_col = project_time_dim.get_load_data_time_columns()

        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        if dtime_interval == ptime_interval:
            return df

        df = self._shift_time_interval(
            df, time_col, dtime_interval, ptime_interval, self.get_frequency()
        )
        breakpoint()
        diff = self._time_difference(df, project_time_dim, difference="symmetric")
        breakpoint()
        if diff:
            df = self._apply_time_wrap(df, project_time_dim, diff)

        return df

    @staticmethod
    def _shift_time_interval(
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

    @staticmethod
    def _time_difference(df, project_time_dim, difference: str = "left"):
        """Compare the time col in df and project_time_dim"""

        time_col = project_time_dim.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        project_time = {
            row[0]
            for row in project_time_dim.build_time_dataframe()
            .select(time_col)
            .filter(f"{time_col} IS NOT NULL")
            .distinct()
            .collect()
        }
        dataset_time = {
            row[0]
            for row in df.select(time_col).filter(f"{time_col} IS NOT NULL").distinct().collect()
        }
        breakpoint()
        if difference == "left":
            return dataset_time.difference(project_time)

        if difference == "right":
            return project_time.difference(dataset_time)

        if difference == "symmetric":
            return dataset_time.symmetric_difference(project_time)

        raise ValueError(f"Unsupported function input {difference=}")

    @staticmethod
    def _apply_time_wrap(df, project_time_dim, diff: set):
        """Apply time-wrapping"""

        time_col = project_time_dim.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        project_time = {
            row[time_col]
            for row in project_time_dim.build_time_dataframe()
            .select(time_col)
            .filter(f"{time_col} IS NOT NULL")
            .distinct()
            .collect()
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
            if left_diff := dataset_time.difference(project_time):
                left_msg = f"\nProcessed dataset time contains {len(left_diff)} extra timestamp(s): {left_diff[:min(5,len(left_diff))]}"
            if right_diff := project_time.difference(dataset_time):
                right_msg = f"\nProcessed dataset time is missing {len(right_diff)} timestamp(s): {right_diff[:min(5,len(right_diff))]}"
            raise DSGInvalidOperation(
                f"Dataset time cannot be processed to match project time. {left_msg}{right_msg}"
            )

        return df

    def _build_time_ranges(
        self,
        time_ranges: TimeRangeModel,
        str_format: str,
        model_years: Optional[list[int]] = None,
        tz: Optional[TimeZone] = None,
    ):
        ranges = []
        allowed_year = None
        for time_range in time_ranges:
            start = datetime.strptime(time_range.start, str_format)
            end = datetime.strptime(time_range.end, str_format)
            if model_years is not None:
                if start.year != end.year or (
                    allowed_year is not None and start.year != allowed_year
                ):
                    raise DSGInvalidDimension(
                        f"All time ranges must be in the same year if model_years is set: {model_years=}"
                    )
                allowed_year = start.year
            for year in model_years or [start.year]:
                start_adj = datetime(
                    year=year,
                    month=start.month,
                    day=start.day,
                    hour=start.hour,
                    minute=start.minute,
                    second=start.second,
                    microsecond=start.microsecond,
                )
                end_adj = datetime(
                    year=end.year + year - start.year,
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

    def _create_adjustment_map_from_model_time(
        self, data_adjustment: DataAdjustmentModel, time_zone: TimeZone, model_years=None
    ):
        """Create data adjustment mapping from model_time to prevailing time (timestamp) of input time_zone."""
        time_col = list(DatetimeTimestampType._fields)
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        ld_adj = data_adjustment.leap_day_adjustment
        fb_adj = data_adjustment.daylight_saving_adjustment.fall_back_hour

        TZ_st, TZ_pt = time_zone.get_standard_time(), time_zone.get_prevailing_time()
        ranges = self.get_time_ranges(
            model_years=model_years, timezone=TZ_pt.tz, data_adjustment=data_adjustment
        )
        freq = self.model.frequency
        model_time, prevailing_time, multipliers = [], [], []
        for range in ranges:
            cur_pt = range.start.to_pydatetime()
            end_pt = range.end.to_pydatetime()

            if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
                fb_times = get_dls_fallback_time_change_by_time_range(
                    cur_pt, end_pt, frequency=freq
                )  # in PT
                fb_repeats = [0 for x in fb_times]
                print(fb_times)

            cur = range.start.to_pydatetime().astimezone(ZoneInfo("UTC"))
            end = range.end.to_pydatetime().astimezone(ZoneInfo("UTC")) + freq

            while cur < end:
                multiplier = 1.0
                frequency = freq
                cur_pt = cur.astimezone(TZ_pt.tz)
                model_ts = cur_pt.replace(tzinfo=TZ_st.tz)
                month = cur_pt.month
                day = cur_pt.day
                if ld_adj == LeapDayAdjustmentType.DROP_FEB29 and month == 2 and day == 29:
                    cur += frequency
                    pass
                if ld_adj == LeapDayAdjustmentType.DROP_DEC31 and month == 12 and day == 31:
                    cur += frequency
                    pass
                if ld_adj == LeapDayAdjustmentType.DROP_JAN1 and month == 1 and day == 1:
                    cur += frequency
                    pass

                if fb_adj == DaylightSavingFallBackType.INTERPOLATE:
                    for i, ts in enumerate(fb_times):
                        if cur == ts.astimezone(ZoneInfo("UTC")):
                            if fb_repeats[i] == 0:
                                frequency = timedelta(hours=0)
                                multiplier = 0.5
                            if fb_repeats[i] == 1:
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

    def _get_tzinfo_from_geography(geography_dim):
        """Get tzinfo from time_zone column of geography dimension record"""
        # TODO not currently in use
        geo_records = geography_dim.get_records_dataframe()
        geo_tz_values = [
            row.time_zone for row in geo_records.select("time_zone").distinct().collect()
        ]
        geo_tzinfos = [TimeZone(tz).tz for tz in geo_tz_values]
        return geo_tzinfos
