import logging
from datetime import timedelta

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

from dsgrid.dimension.time import (
    MeasurementType,
    AnnualTimeRange,
    TimeDimensionType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session, custom_spark_conf, union
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

    def build_time_dataframe(self, model_years=None):
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, IntegerType(), False)])

        model_time = self.list_expected_dataset_timestamps(model_years=model_years)
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)
        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     return self.build_time_dataframe()

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        model_years=None,
        value_columns=None,
        wrap_time_allowed=False,
        time_based_data_adjustment=None,
    ):
        assert model_years is not None
        assert value_columns is not None
        match self.model.measurement_type:
            case MeasurementType.MEASURED:
                df = self.map_annual_time_measured_to_datetime(df, project_time_dim, model_years)
            case MeasurementType.TOTAL:
                df = self.map_annual_total_to_datetime(
                    df, project_time_dim, model_years, value_columns
                )
            case _:
                raise NotImplementedError(f"Unhandled: {self.model.measurement_type}")

        return df

    def map_annual_time_measured_to_datetime(self, annual_df, dt_dim, model_years):
        """Map a dataframe with MeasuredType.MEASURED to DateTime."""
        time_col = self.get_load_data_time_columns()[0]
        df = dt_dim.build_time_dataframe(model_years=model_years)
        dt_time_col = dt_dim.get_load_data_time_columns()[0]
        return df.join(annual_df, on=F.year(dt_time_col) == annual_df[time_col]).drop(time_col)

    def map_annual_total_to_datetime(self, annual_df, dt_dim, model_years, value_columns):
        """Map a dataframe with MeasuredType.TOTAL to DateTime."""
        assert (
            dt_dim.model.time_type == TimeDimensionType.DATETIME
        ), "dt_dim must be datetime type."
        frequency = dt_dim.model.frequency
        time_col = self.get_load_data_time_columns()[0]
        dfs = []
        for model_year in model_years:
            if self.model.include_leap_day and model_year % 4 == 0:
                measured_duration = timedelta(days=366)
            else:
                measured_duration = timedelta(days=365)
            df = dt_dim.build_time_dataframe(model_years=[model_year])
            dt_time_col = dt_dim.get_load_data_time_columns()[0]
            with custom_spark_conf({"spark.sql.session.timeZone": "UTC"}):
                tmp_col = f"{dt_time_col}_utc"
                df = df.withColumn(
                    tmp_col,
                    F.from_utc_timestamp(
                        dt_time_col, dt_dim.model.datetime_format.timezone.tz_name
                    ),
                )
                df = df.join(annual_df, on=F.year(df[tmp_col]) == annual_df[time_col]).drop(
                    time_col, tmp_col
                )
            for column in value_columns:
                df = df.withColumn(column, F.col(column) / (measured_duration / frequency))
            if not df.rdd.isEmpty():
                dfs.append(df)

        return union(dfs)

    def get_frequency(self):
        return timedelta(days=365)

    def get_time_ranges(self, model_years=None):
        ranges = []
        frequency = self.get_frequency()
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, model_years=model_years, tz=self.get_tzinfo()
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

    def list_expected_dataset_timestamps(self, model_years=None):
        if model_years is not None:
            # We do not expect to need this.
            raise NotImplementedError(
                f"No support for {model_years=} in {type(self)}.list_expected_dataset_timestamps"
            )

        timestamps = []
        for time_range in self.model.ranges:
            start, end = (int(time_range.start), int(time_range.end))
            timestamps += [AnnualTimestampType(x) for x in range(start, end + 1)]
        return timestamps
