import logging

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
)
import pyspark.sql.functions as F

from dsgrid.dimension.time import DatetimeRange, DatetimeFormat
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidParameter
from dsgrid.time.types import DatetimeTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import get_spark_session
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.common import VALUE_COLUMN

logger = logging.getLogger(__name__)


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class():
        return DateTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns):
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

    def _check_local_time_for_alignment(self, load_data_df, time_col):
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

    def build_time_dataframe(self):
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

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        value_columns: set[str],
        scratch_dir_context: ScratchDirContext,
        wrap_time_allowed=False,
        time_based_data_adjustment=None,
    ):
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        ptime_col = project_time_dim.get_load_data_time_columns()
        assert len(ptime_col) == 1, ptime_col
        ptime_col = ptime_col[0]

        df = self._convert_time_to_project_time(
            df,
            project_time_dim,
            scratch_dir_context,
            wrap_time=wrap_time_allowed,
            time_based_data_adjustment=time_based_data_adjustment,
        )
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self):
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

    def get_load_data_time_columns(self):
        return list(DatetimeTimestampType._fields)

    def get_tzinfo(self):
        if self.model.datetime_format.format_type == DatetimeFormat.ALIGNED:
            return self.model.datetime_format.timezone.tz
        if self.model.datetime_format.format_type in [DatetimeFormat.LOCAL_AS_STRINGS]:
            return None
        msg = f"Undefined tzinfo for {self.model.datetime_format.format_type=}"
        raise NotImplementedError(msg)

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self):
        timestamps = []
        for time_range in self.get_time_ranges():
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps

    def convert_time_format(self, df):
        if self.model.datetime_format.format_type != DatetimeFormat.LOCAL_AS_STRINGS:
            return df
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        df = df.withColumn(
            time_col,
            F.to_timestamp(time_col, self.model.datetime_format.data_str_format),
        )
        return df
