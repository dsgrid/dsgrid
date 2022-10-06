import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd

from dsgrid.dimension.time import (
    get_equivalent_standard_system_timezone,
    TimeZone,
    make_time_range,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DatetimeTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class():
        return DateTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df):
        logger.info("Check DateTimeDimensionConfig dataset time consistency.")
        time_col = self.get_timestamp_load_data_columns()
        if len(time_col) > 1:
            raise ValueError(
                "DateTimeDimensionConfig expects only one column from "
                f"get_timestamp_load_data_columns, but has {time_col}"
            )
        time_col = time_col[0]
        tz = self.get_tzinfo()
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            x[time_col].astimezone().astimezone(tz)
            for x in load_data_df.select(time_col).distinct().sort(time_col).collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def get_time_dataframe(self):
        time_col = self.get_timestamp_load_data_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, TimestampType(), False)])

        if self.model.timezone in [TimeZone.LOCAL, TimeZone.NONE]:
            # TODO: handle local time zone
            schema = StructType(
                [
                    StructField(time_col, StringType(), False),
                    StructField("timezone", StringType(), False),
                ]
            )
            raise ValueError("TimeZone = LOCAL or NONE needs fixing")

        model_time = self.list_expected_dataset_timestamps()
        spark = SparkSession.builder.appName("dgrid").getOrCreate()
        session_tz = spark.conf.get("spark.sql.session.timeZone")

        # this is not required if we maintain UTC as session time
        # if self.model.timezone.is_standard():
        #     session_tz = get_equivalent_standard_system_timezone(session_tz)

        # this is in local system time
        df_time = spark.createDataFrame(model_time, schema=schema)

        # TO BE DELETED
        # model_time2 = model_time[1668:1668+24] # <----
        # model_time2 = model_time[7385:7385+24]
        # df_time2 = spark.createDataFrame(model_time2, schema=schema)
        # test = df_time2.withColumn("UTC", F.to_utc_timestamp(F.col("timestamp"), session_tz)).withColumn("project", F.from_utc_timestamp(F.col("UTC"), self.model.timezone.tz_name))
        # test.write.parquet("/Users/lliu2/Downloads/time_pyspark.parquet")
        # test.toPandas().head(20)
        # import pandas as pd; pd.DataFrame(test.collect()).head(20)
        # breakpoint()

        # convert to dataset timezone
        df_time = self._convert_time_zone(df_time, time_col, session_tz, self.model.timezone.tz_name)

        return df_time


    @staticmethod
    def _convert_time_zone(df, time_col: str, from_tz, to_tz):
        """ convert dataframe from one single time zone to another """
        nontime_cols = [col for col in df.columns if col != time_col]
        df = df.select(
            F.from_utc_timestamp(
                F.to_utc_timestamp(F.col(time_col), from_tz), to_tz
            ).alias(time_col), *nontime_cols
        )
        return df

    def convert_dataframe(self, df=None, project_time_dim=None):
        if project_time_dim is None:
            return df

        spark = SparkSession.builder.appName("dgrid").getOrCreate()
        session_tz = spark.conf.get("spark.sql.session.timeZone")
        project_tz = project_time_dim.model.timezone.tz_name

        time_col = self.get_timestamp_load_data_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        # I think whether project_time_dim is the same as dataset_time_dim, or different
        # or dataset_time_dim is in local time, we only need to do conversion once to project
        # I think time_zone is required only for validation at registration...
        df = self._convert_time_zone(df, time_col, session_tz, project_tz)

        # QC
        project_time_df = project_time_dim.get_time_dataframe()
        df_check = df.groupBy(time_col).count().sort(time_col)
        freq_count = df_check.select("count").distinct().collect()
        assert len(freq_count) == 1, freq_count

        project_ts = set(project_time_df.select(time_col).collect())
        df_ts = set(df_check.select(time_col).collect())
        assert df_ts == project_ts, df_ts.symmetric_difference(project_ts)

        return df

    def get_frequency(self):
        return self.model.frequency

    def get_time_ranges(self):
        ranges = []
        tz = self.get_tzinfo()
        for time_range in self.model.ranges:
            start = datetime.strptime(time_range.start, self.model.str_format)
            start = pd.Timestamp(start, tz=tz)
            end = datetime.strptime(time_range.end, self.model.str_format)
            end = pd.Timestamp(end, tz=tz)
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    leap_day_adjustment=self.model.leap_day_adjustment,
                )
            )

        return ranges

    def get_timestamp_load_data_columns(self):
        return list(DatetimeTimestampType._fields)

    def get_tzinfo(self):
        assert self.model.timezone is not TimeZone.LOCAL, self.model.timezone
        return self.model.timezone.tz

    def list_expected_dataset_timestamps(self):
        # TODO: need to support validation of multiple time ranges: DSGRID-173
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        return [DatetimeTimestampType(x) for x in time_range.iter_timestamps()]
