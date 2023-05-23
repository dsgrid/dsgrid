import logging
from pyspark.sql.types import StructType, StructField, TimestampType

from dsgrid.dimension.time import make_time_range
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DatetimeTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


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
            raise ValueError(
                "DateTimeDimensionConfig expects only one column from "
                f"get_load_data_time_columns, but has {time_columns}"
            )
        time_col = time_columns[0]
        tz = self.get_tzinfo()
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            x[time_col].astimezone().astimezone(tz)
            for x in load_data_df.select(time_col)
            .distinct()
            .filter(f"{time_col} is not null")
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

    def build_time_dataframe(self, model_years=None):
        # Note: DF.show() displays time in session time, which may be confusing.
        # But timestamps are stored correctly here

        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, TimestampType(), False)])
        model_time = self.list_expected_dataset_timestamps(model_years=model_years)
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     time_col = self.get_load_data_time_columns()
    #     assert len(time_col) == 1, time_col
    #     time_col = time_col[0]

    #     df_time = self.build_time_dataframe()
    #     session_tz = _get_spark_session().conf.get("spark.sql.session.timeZone")
    #     df_time = self._convert_time_zone(
    #         df_time, time_col, session_tz, self.model.timezone.tz_name
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

    def convert_dataframe(self, df, project_time_dim, model_years=None, value_columns=None):
        df = self._convert_time_to_project_time_interval(df=df, project_time_dim=project_time_dim)
        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self, model_years=None):
        ranges = []
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, model_years=model_years, tz=self.get_tzinfo()
        ):
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    leap_day_adjustment=self.model.leap_day_adjustment,
                    time_interval_type=self.model.time_interval_type,
                )
            )

        return ranges

    def get_load_data_time_columns(self):
        return list(DatetimeTimestampType._fields)

    def get_tzinfo(self):
        return self.model.timezone.tz

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self, model_years=None):
        timestamps = []
        for time_range in self.get_time_ranges(model_years=model_years):
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps
