import abc
import calendar
import logging
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType, StructField, IntegerType
    )
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pandas as pd

from dsgrid.dimension.time import (
    TimeZone,
    get_timezone,
    RepresentativePeriodFormat,
    DatetimeRange,
    LeapDayAdjustmentType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import OneWeekPerMonthByHourType
from dsgrid.utils.timing import track_timing, timer_stats_collector
from .dimensions import RepresentativePeriodTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class RepresentativePeriodTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an RepresentativePeriodTimeDimensionModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # As of now there is only one format. We expect this to grow.
        # It's possible that one function (or set of functions) can handle all permutations
        # of parameters. We can make that determination once we have requirements for more
        # formats.
        if self.model.format == RepresentativePeriodFormat.ONE_WEEK_PER_MONTH_BY_HOUR:
            self._format_handler = OneWeekPerMonthByHourHandler()
        else:
            assert False, f"Unsupported {self.model.format}"

    @staticmethod
    def model_class():
        return RepresentativePeriodTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df):
        self._format_handler.check_dataset_time_consistency(
            self._format_handler.list_expected_dataset_timestamps(self.model.ranges), load_data_df
        )

    def get_time_dataframe(self):
        time_cols = self.get_timestamp_load_data_columns()
        schema = StructType([
                StructField(time_col, IntegerType(), False) for
                time_col in time_cols
            ])

        model_time = self.list_expected_dataset_timestamps()
        spark = SparkSession.builder.appName("dgrid").getOrCreate()
        df_time = spark.createDataFrame(model_time, schema=schema)

        return df_time

    @staticmethod
    def _create_time_map(project_time_dim):
        pass


    def convert_dataframe(self, df=None, project_time_dim=None, dataset_geography_dim=None, df_meta=None):
        # TODO: Create a dataframe with timestamps covering project_time_dim.model.ranges
        # for all model_years in the df, then join the df to that dataframe on the time columns.
        # Account for time zone.

        # TODO: remove df_meta, add timezone to df before convert_dataframe

        if project_time_dim is None:
            return df
        if project_time_dim.get_time_ranges == self.get_time_ranges:
            return df

        time_cols = self.get_timestamp_load_data_columns()

        # get unique timezones
        geo_records = dataset_geography_dim.get_records_dataframe()
        geo_tz_values = [row.time_zone for row in geo_records.select("time_zone").distinct().collect()]
        geo_tz_names = [get_timezone(tz).tz_name for tz in geo_tz_values]

        # create timezone map
        tz_df = df_meta.select(F.col("id").alias("map_id"), "geography").join(
            geo_records.select(F.col("id").alias("geography"), "time_zone"),
            on = "geography",
            how = "left"
            ).drop("geography")

        # create time map
        project_time_df = project_time_dim.get_time_dataframe()
        project_tz = project_time_dim.model.timezone.tz_name
        idx = 0
        for tz_value, tz_name in zip(geo_tz_values, geo_tz_names):
            local_time_df = project_time_df.withColumn("time_zone", F.lit(tz_value))\
            .withColumn("local_time",
                F.from_utc_timestamp(
                    F.to_utc_timestamp(F.col("timestamp"), project_tz),
                    tz_name
                    ))
            select = ["timestamp", "time_zone"]
            for col in time_cols:
                func = col.replace("_","")
                expr = f"{func}(local_time) AS {col}"
                if func == "dayofweek":
                    expr = f"{func}(local_time)-1 AS {col}"
                select.append(expr)
            local_time_df = local_time_df.selectExpr(*select)
            if idx == 0:
                time_df = local_time_df
            else:
                time_df = time_df.union(local_time_df)
            idx += 1

        # join all
        df = df.join(tz_df, on=df["id"]==tz_df["map_id"], how="left").drop("map_id")
        join_keys = time_cols + ["time_zone"]
        select = [col if col not in time_cols else F.col(col).cast(IntegerType()) for col in df.columns]
        dfn = df.select(*select).join(time_df, on=join_keys, how="left").drop(*join_keys)

        # QC
        time_len = project_time_df.count()
        id_len = df.select("id").distinct().count()
        df_len = dfn.count()
        assert df_len == time_len * id_len, f"df with exploded time has len={df_len}, but expecting {time_len*id_len}, check join logic!"

        return dfn

    def get_frequency(self):
        return self._format_handler.get_frequency()

    def get_time_ranges(self):
        return self._format_handler.get_time_ranges(self.model.ranges, self.get_tzinfo())

    def get_timestamp_load_data_columns(self):
        return self._format_handler.get_timestamp_load_data_columns()

    def get_tzinfo(self):
        ### TBD
        return None

    def list_expected_dataset_timestamps(self):
        return self._format_handler.list_expected_dataset_timestamps(self.model.ranges)


class RepresentativeTimeFormatHandlerBase(abc.ABC):
    """Provides implementations for different representative time formats."""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, expected_timestamps, load_data_df):
        """Check consistency between time ranges from the time dimension and load data.

        Parameters
        ----------
        expected_timestamps : list
        load_data_df : pyspark.sql.DataFrame

        Raises
        ------
        DSGInvalidDataset

        """

    @abc.abstractmethod
    def get_frequency(self):
        """Return the frequency.

        Returns
        -------
        timedelta

        """

    @staticmethod
    @abc.abstractmethod
    def get_timestamp_load_data_columns():
        """Return the required timestamp columns in the load data table.

        Returns
        -------
        list

        """

    @abc.abstractmethod
    def get_time_ranges(self):
        """Return a list of DatetimeRange instances for the dataset.

        Returns
        -------
        list
            list of DatetimeRange

        """

    @abc.abstractmethod
    def list_expected_dataset_timestamps(self):
        """Return a list of the timestamps expected in the load_data table.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """


class OneWeekPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one week per month."""

    def check_dataset_time_consistency(self, expected_timestamps, load_data_df):
        logger.info("Check OneWeekPerMonthByHourHandler dataset time consistency.")
        time_cols = self.get_timestamp_load_data_columns()
        actual_timestamps = [
            OneWeekPerMonthByHourType(*x.asDict().values())
            for x in load_data_df.select(*time_cols).distinct().sort(*time_cols).collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )
        logger.info("Verified that expected_timestamps equal actual_timestamps")

        actual_len = len(actual_timestamps)
        expected_len = len(expected_timestamps)
        if actual_len != expected_len:
            raise DSGInvalidDataset(
                f"Length of time arrays is incorrect: actual={actual_len} expected={expected_len}"
            )
        logger.info("Verified that all time arrays have the same, correct length.")

    def get_frequency(self):
        return timedelta(hours=1)

    def get_time_ranges(self, ranges, _):
        # TODO: This method may have some problems but is currently unused.
        # How to handle year? Leap year?
        time_ranges = []
        for model in ranges:
            if model.end == 2:
                logger.warning("Last day of February may not be accurate.")
            last_day = calendar.monthrange(2021, model.end)[1]
            time_ranges.append(
                DatetimeRange(
                    start=pd.Timestamp(datetime(year=1970, month=model.start, day=1)),
                    end=pd.Timestamp(datetime(year=1970, month=model.end, day=last_day, hour=23)),
                    frequency=timedelta(hours=1),
                    leap_day_adjustment=LeapDayAdjustmentType.NONE,
                )
            )

        return time_ranges

    @staticmethod
    def get_timestamp_load_data_columns():
        return list(OneWeekPerMonthByHourType._fields)

    def list_expected_dataset_timestamps(self, ranges):
        timestamps = []
        for model in ranges:
            for month in range(model.start, model.end + 1):
                for day_of_week in range(7):
                    for hour in range(24):
                        ts = OneWeekPerMonthByHourType(
                            month=month, day_of_week=day_of_week, hour=hour
                        )
                        timestamps.append(ts)
        return timestamps
