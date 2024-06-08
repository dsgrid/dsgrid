import logging
from datetime import timedelta

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType

from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.time import (
    MeasurementType,
    AnnualTimeRange,
    TimeDimensionType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session, custom_spark_conf
from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class():
        return AnnualTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns):
        logger.info("Check AnnualTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            raise ValueError(
                "AnnualTimeDimensionConfig expects only one column from "
                f"get_load_data_time_columns, but has {time_columns}"
            )
        time_col = time_columns[0]
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            pd.Timestamp(str(x[time_col]), tz=self.get_tzinfo()).to_pydatetime()
            for x in load_data_df.select(time_col).distinct().sort(time_col).collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def build_time_dataframe(self):
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, IntegerType(), False)])

        model_time = self.list_expected_dataset_timestamps()
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)
        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     return self.build_time_dataframe()

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        value_columns: set[str],
        wrap_time_allowed=False,
        time_based_data_adjustment=None,
    ):
        assert value_columns is not None
        match self.model.measurement_type:
            case MeasurementType.MEASURED:
                df = self.map_annual_time_measured_to_datetime(df, project_time_dim)
            case MeasurementType.TOTAL:
                df = self.map_annual_total_to_datetime(df, project_time_dim, value_columns)
            case _:
                raise NotImplementedError(f"Unhandled: {self.model.measurement_type}")

        return df

    def map_annual_time_measured_to_datetime(
        self, annual_df: DataFrame, dt_dim: DateTimeDimensionConfig
    ):
        """Map a dataframe with MeasuredType.MEASURED to DateTime."""
        # Does this category even make sense? Why would someone report
        # something other than total for annual?
        dt_time_col = dt_dim.get_load_data_time_columns()[0]
        df = dt_dim.build_time_dataframe()
        with custom_spark_conf(
            {"spark.sql.session.timeZone": dt_dim.model.datetime_format.timezone.tz_name}
        ):
            years = df.withColumn("year", F.year(dt_time_col)).select("year").distinct().collect()
            if len(years) != 1:
                raise Exception("Bug: project time has more than one year: {years=}")
        return df.crossJoin(annual_df)

    def map_annual_total_to_datetime(
        self, annual_df: DataFrame, dt_dim: DateTimeDimensionConfig, value_columns: set[str]
    ):
        """Map a dataframe with MeasuredType.TOTAL to DateTime."""
        assert (
            dt_dim.model.time_type == TimeDimensionType.DATETIME
        ), "dt_dim must be datetime type."
        frequency = dt_dim.model.frequency
        df = dt_dim.build_time_dataframe()
        dt_time_col = dt_dim.get_load_data_time_columns()[0]
        with custom_spark_conf(
            {"spark.sql.session.timeZone": dt_dim.model.datetime_format.timezone.tz_name}
        ):
            years = df.withColumn("year", F.year(dt_time_col)).select("year").distinct().collect()
            if len(years) != 1:
                raise Exception("Bug: project time has more than one year: {years=}")
            year = years[0].year
            if self.model.include_leap_day and year % 4 == 0:
                measured_duration = timedelta(days=366)
            else:
                measured_duration = timedelta(days=365)
        df2 = df.crossJoin(annual_df)
        for column in value_columns:
            df2 = df2.withColumn(column, F.col(column) / (measured_duration / frequency))
        return df2

    def get_frequency(self):
        return timedelta(days=365)

    def get_time_ranges(self):
        ranges = []
        frequency = self.get_frequency()
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, tz=self.get_tzinfo()
        ):
            start = pd.Timestamp(start)
            end = pd.Timestamp(end)
            ranges.append(
                AnnualTimeRange(
                    start=start,
                    end=end,
                    frequency=frequency,
                )
            )

        return ranges

    def get_load_data_time_columns(self):
        return list(AnnualTimestampType._fields)

    def get_tzinfo(self):
        return None

    def get_time_interval_type(self):
        return None

    def list_expected_dataset_timestamps(self):
        timestamps = []
        for time_range in self.model.ranges:
            start, end = (int(time_range.start), int(time_range.end))
            timestamps += [AnnualTimestampType(x) for x in range(start, end + 1)]
        return timestamps
