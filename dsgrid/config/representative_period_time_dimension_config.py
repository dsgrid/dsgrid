import abc
import calendar
import logging
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as F

import pandas as pd

from dsgrid.dimension.time import (
    TimeZone,
    RepresentativePeriodFormat,
    DatetimeRange,
    LeapDayAdjustmentType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import OneWeekPerMonthByHourType
from dsgrid.utils.timing import track_timing, timer_stats_collector
from dsgrid.utils.spark import get_spark_session
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
    def check_dataset_time_consistency(self, load_data_df, time_columns):
        self._format_handler.check_dataset_time_consistency(
            self._format_handler.list_expected_dataset_timestamps(self.model.ranges),
            load_data_df,
            time_columns,
        )

    def build_time_dataframe(self):
        time_cols = self.get_timestamp_load_data_columns()
        schema = StructType(
            [StructField(time_col, IntegerType(), False) for time_col in time_cols]
        )

        model_time = self.list_expected_dataset_timestamps()
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     return self.build_time_dataframe()

    def convert_dataframe(self, df=None, project_time_dim=None):
        # in spark.dayofweek: 1=Sunday, 7=Saturday
        # dsgrid uses python standard library (same for pandas), which has day_of_week: 0=Monday, 6=Sunday
        # the mapping is: python.dt.day_of_week = [(i+7-2)%7 for i in spark.dayofweek]

        if project_time_dim is None:
            return df
        if (
            project_time_dim.list_expected_dataset_timestamps()
            == self.list_expected_dataset_timestamps()
        ):
            return df

        time_cols = self.get_timestamp_load_data_columns()
        ptime_col = project_time_dim.get_timestamp_load_data_columns()
        assert len(ptime_col) == 1, ptime_col
        ptime_col = ptime_col[0]

        assert "time_zone" in df.columns, df.columns
        geo_tz_values = [row.time_zone for row in df.select("time_zone").distinct().collect()]
        assert geo_tz_values
        geo_tz_names = [TimeZone(tz).tz_name for tz in geo_tz_values]
        assert geo_tz_names

        # create time map
        # temporarily set session time to UTC for timeinfo extraction
        # Note: timeinfo is extracted from local_time column exactly in DF.show(),
        # and only UTC seems to convert to local_time correctly for DF.show().
        # Even though UTC does not always lead to correct time output when DF.toPandas()
        # the underlying time data is correctly stored when saved to file
        spark = get_spark_session()
        session_tz_orig = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        session_tz = spark.conf.get("spark.sql.session.timeZone")

        time_df = None
        try:
            project_time_df = project_time_dim.build_time_dataframe()
            idx = 0
            for tz_value, tz_name in zip(geo_tz_values, geo_tz_names):
                local_time_df = project_time_df.withColumn(
                    "time_zone", F.lit(tz_value)
                ).withColumn(
                    "local_time",
                    F.from_utc_timestamp(
                        F.to_utc_timestamp(F.col(ptime_col), session_tz), tz_name
                    ),
                )
                select = [ptime_col, "time_zone"]
                for col in time_cols:
                    func = col.replace("_", "")
                    expr = f"{func}(local_time) AS {col}"
                    if col == "day_of_week":
                        expr = f"mod(dayofweek(local_time)+7-2, 7) AS {col}"
                    select.append(expr)
                local_time_df = local_time_df.selectExpr(*select)
                if idx == 0:
                    time_df = local_time_df
                else:
                    time_df = time_df.union(local_time_df)
                idx += 1
        finally:
            # reset session timezone
            spark.conf.set("spark.sql.session.timeZone", session_tz_orig)
            assert spark.conf.get("spark.sql.session.timeZone") == session_tz_orig

        # join all
        join_keys = time_cols + ["time_zone"]
        select = [
            col if col not in time_cols else F.col(col).cast(IntegerType()) for col in df.columns
        ]
        df = df.select(*select).join(time_df, on=join_keys).drop(*time_cols)

        return df

    def get_frequency(self):
        return self._format_handler.get_frequency()

    def get_time_ranges(self):
        return self._format_handler.get_time_ranges(self.model.ranges, self.get_tzinfo())

    def get_timestamp_load_data_columns(self):
        return self._format_handler.get_timestamp_load_data_columns()

    def get_tzinfo(self):
        # TBD
        return None

    def list_expected_dataset_timestamps(self):
        return self._format_handler.list_expected_dataset_timestamps(self.model.ranges)


class RepresentativeTimeFormatHandlerBase(abc.ABC):
    """Provides implementations for different representative time formats."""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, expected_timestamps, load_data_df, time_columns):
        """Check consistency between time ranges from the time dimension and load data.

        Parameters
        ----------
        expected_timestamps : list
        load_data_df : pyspark.sql.DataFrame
        time_columns : list[str]

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

    def check_dataset_time_consistency(self, expected_timestamps, load_data_df, time_columns):
        logger.info("Check OneWeekPerMonthByHourHandler dataset time consistency.")
        actual_timestamps = []
        for row in load_data_df.select(*time_columns).distinct().sort(*time_columns).collect():
            data = row.asDict()
            num_none = 0
            for val in data.values():
                if val is None:
                    num_none += 1
            if num_none > 0:
                if num_none != len(data):
                    raise DSGInvalidDataset(
                        f"If any time column is null then all columns must be null: {data}"
                    )
            else:
                actual_timestamps.append(OneWeekPerMonthByHourType(**data))

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
