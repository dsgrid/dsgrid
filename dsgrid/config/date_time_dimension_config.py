import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

import chronify

from dsgrid.dimension.time import DatetimeRange, DatetimeFormat, TimeZone
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidParameter
from dsgrid.spark.types import DataFrame, F, StructType, StructField, TimestampType
from dsgrid.time.types import DatetimeTimestampType
from dsgrid.dimension.time import TimeIntervalType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.common import VALUE_COLUMN

logger = logging.getLogger(__name__)


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class() -> DateTimeDimensionModel:
        return DateTimeDimensionModel

    def supports_chronify(self) -> bool:
        return True

    def to_chronify(self) -> chronify.DatetimeRange:
        time_cols = self.get_load_data_time_columns()
        assert len(self._model.ranges) == 1
        assert len(time_cols) == 1
        # TODO: issue #341: this is actually tied to the weather_year problem #340
        # If there are no ranges, all of this must be dynamic.
        # The two issues should be solved together.
        return chronify.DatetimeRange(
            time_column=time_cols[0],
            start=pd.Timestamp(self.get_start_times()[0]),
            length=self.get_lengths()[0],
            resolution=self._model.frequency,
            measurement_type=self._model.measurement_type,
            interval_type=self._model.time_interval_type,
        )

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check DateTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            msg = f"DateTimeDimensionConfig expects only one time column, but has {time_columns=}"
            raise DSGInvalidParameter(msg)

        time_col = time_columns[0]
        tz = self.get_tzinfo()
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        assert (
            load_data_df.schema[time_col].dataType == TimestampType()
        ), f"datetime {time_col} column must be TimestampType"

        if self.model.datetime_format.format_type in [DatetimeFormat.LOCAL_AS_STRINGS]:
            logger.warning(
                "DatetimeFormat.LOCAL_AS_STRINGS has incomplete time check on load_data_df"
            )
            self._check_local_time_for_alignment(load_data_df, time_col)

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            x[time_col].astimezone().astimezone(tz)
            for x in load_data_df.select(time_col)
            .filter(f"{time_col} is not null")
            .distinct()
            .sort(time_col)
            .collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def _check_local_time_for_alignment(self, load_data_df, time_col) -> None:
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        expected_timestamps = time_range.list_time_range()

        if "id" in load_data_df.columns:
            groupby = ["id"]
        else:
            groupby = [x for x in load_data_df.columns if x not in [time_col, VALUE_COLUMN]]
        start_times = load_data_df.groupBy(*groupby).agg(F.min(time_col).alias(time_col))
        min_ts = start_times.select(F.min(time_col).alias(time_col)).collect()[0][time_col]
        start_times = start_times.withColumn("min_ts", F.lit(min_ts))
        time_deltas = start_times.select(
            *groupby, (F.col(time_col) - F.col("min_ts")).alias("time_delta")
        )
        timestamps = load_data_df.select(*groupby, time_col).join(time_deltas, groupby, "left")
        timestamps = timestamps.select(
            *groupby, (F.col(time_col) - F.col("time_delta")).alias("aligned_time")
        )
        timestamp_counts = [
            x["count"] for x in timestamps.groupBy("aligned_time").count().collect()
        ]
        if len(set(timestamp_counts)) != 1:
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not have the same time range when adjusting for offsets."
            )
        if len(timestamp_counts) != len(expected_timestamps):
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not have the expected number of timestamps when adjusting for offsets."
            )
        return

    def build_time_dataframe(self) -> DataFrame:
        # Note: DF.show() displays time in session time, which may be confusing.
        # But timestamps are stored correctly here

        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        model_time = self.list_expected_dataset_timestamps()
        schema = StructType([StructField(time_col, TimestampType(), False)])
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)
        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     time_col = self.get_load_data_time_columns()
    #     assert len(time_col) == 1, time_col
    #     time_col = time_col[0]

    #     df_time = self.build_time_dataframe()
    #     session_tz = _get_spark_session().conf.get("spark.sql.session.timeZone")
    #     df_time = self._convert_time_zone(
    #         df_time, time_col, session_tz, self.model.datetime_format.timezone.tz_name
    #     )

    #     return df_time

    # @staticmethod
    # def _convert_time_zone(df, time_col: str, from_tz, to_tz):
    #     """convert dataframe from one single time zone to another"""
    #     nontime_cols = [col for col in df.columns if col != time_col]
    #     df2 = df.select(
    #         F.from_utc_timestamp(F.to_utc_timestamp(F.col(time_col), from_tz), to_tz).alias(
    #             time_col
    #         ),
    #         *nontime_cols,
    #     )
    #     return df2

    def convert_dataframe(self, *args, **kwargs):
        msg = f"{self.__class__.__name__}.convert_dataframe is implemented through chronify"
        raise NotImplementedError(msg)

    def get_frequency(self) -> timedelta:
        return self.model.frequency

    def get_time_ranges(self) -> list[DatetimeRange]:
        ranges = []
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, tz=self.get_tzinfo()
        ):
            ranges.append(
                DatetimeRange(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                )
            )

        return ranges

    def get_start_times(self) -> list[pd.Timestamp]:
        tz = self.get_tzinfo()
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))
        return start_times

    def get_lengths(self) -> list[int]:
        tz = self.get_tzinfo()
        lengths = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            end = datetime.strptime(trange.end, self.model.str_format)
            assert start.tzinfo is None
            assert end.tzinfo is None
            start_utc = start.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            end_utc = end.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            freq = self.get_frequency()
            length = (end_utc - start_utc) / freq + 1
            assert length % 1 == 0, f"{length=} is not a whole number"
            lengths.append(int(length))
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        return list(DatetimeTimestampType._fields)

    def get_time_zone(self) -> TimeZone | None:
        if self.model.datetime_format.format_type == DatetimeFormat.ALIGNED:
            return self.model.datetime_format.timezone
        if self.model.datetime_format.format_type in [DatetimeFormat.LOCAL_AS_STRINGS]:
            return None
        msg = f"Undefined time zone for {self.model.datetime_format.format_type=}"
        raise NotImplementedError(msg)

    def get_tzinfo(self) -> ZoneInfo:
        time_zone = self.get_time_zone()
        if time_zone is None:
            return None
        return time_zone.tz

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self) -> list[DatetimeTimestampType]:
        timestamps = []
        for time_range in self.get_time_ranges():
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps

    def convert_time_format(self, df: DataFrame, update_model: bool = False) -> DataFrame:
        if self.model.datetime_format.format_type != DatetimeFormat.LOCAL_AS_STRINGS:
            return df
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        df = df.withColumn(
            time_col,
            F.to_timestamp(time_col, self.model.datetime_format.data_str_format),
        )
        if update_model:
            # TODO: The code doesn't support DatetimeFormat.LOCAL.
            # self.model.datetime_format.format_type = DatetimeFormat.LOCAL
            msg = "convert_time_format DatetimeFormat.LOCAL_AS_STRINGS update_model=True"
            raise NotImplementedError(msg)
        return df
