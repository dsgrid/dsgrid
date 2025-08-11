import logging
from datetime import timedelta, datetime

import pandas as pd
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import AnnualTimeRange
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.dimension.time_utils import is_leap_year
from dsgrid.spark.functions import (
    cross_join,
    handle_column_spaces,
    select_expr,
)
from dsgrid.spark.types import (
    DataFrame,
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    F,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import (
    get_spark_session,
    set_session_time_zone,
)
from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class() -> AnnualTimeDimensionModel:
        return AnnualTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check AnnualTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            msg = (
                "AnnualTimeDimensionConfig expects only one column from "
                f"get_load_data_time_columns, but has {time_columns}"
            )
            raise ValueError(msg)
        time_col = time_columns[0]
        time_ranges = self.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        actual_timestamps = [
            pd.Timestamp(str(x[time_col]), tz=self.get_tzinfo()).to_pydatetime()
            for x in load_data_df.select(time_col)
            .distinct()
            .filter(f"{time_col} IS NOT NULL")
            .sort(time_col)
            .collect()
        ]
        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            msg = f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            raise DSGInvalidDataset(msg)

    def build_time_dataframe(self) -> DataFrame:
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        schema = StructType([StructField(time_col, IntegerType(), False)])

        model_time = self.list_expected_dataset_timestamps()
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)
        return df_time

    def get_frequency(self) -> timedelta:
        return timedelta(days=365)

    def get_time_ranges(self) -> list[AnnualTimeRange]:
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

    def get_start_times(self) -> list[pd.Timestamp]:
        tz = self.get_tzinfo()
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))

        return start_times

    def get_lengths(self) -> list[int]:
        lengths = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            end = datetime.strptime(trange.end, self.model.str_format)
            lengths.append(end.year - start.year + 1)
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        return list(AnnualTimestampType._fields)

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self) -> None:
        return None

    def list_expected_dataset_timestamps(self) -> list[AnnualTimestampType]:
        timestamps = []
        for time_range in self.model.ranges:
            start, end = (int(time_range.start), int(time_range.end))
            timestamps += [AnnualTimestampType(x) for x in range(start, end + 1)]
        return timestamps


def map_annual_time_to_date_time(
    df: DataFrame,
    annual_dim: AnnualTimeDimensionConfig,
    dt_dim: DateTimeDimensionConfig,
    value_columns: set[str],
) -> DataFrame:
    """Map a DataFrame with an annual time dimension to a DateTime time dimension."""
    annual_col = annual_dim.get_load_data_time_columns()[0]
    myear_column = DimensionType.MODEL_YEAR.value
    timestamps = make_time_range_generator(dt_dim.to_chronify()).list_timestamps()
    time_cols = dt_dim.get_load_data_time_columns()
    assert len(time_cols) == 1, time_cols
    time_col = time_cols[0]
    schema = StructType([StructField(time_col, TimestampType(), False)])
    dt_df = get_spark_session().createDataFrame(
        [(x.to_pydatetime(),) for x in timestamps], schema=schema
    )

    # Note that MeasurementType.TOTAL has already been verified.
    with set_session_time_zone(dt_dim.model.datetime_format.timezone.tz_name):
        years = (
            select_expr(dt_df, [f"YEAR({handle_column_spaces(time_col)}) AS year"])
            .distinct()
            .collect()
        )
        if len(years) != 1:
            msg = "DateTime dimension has more than one year: {years=}"
            raise NotImplementedError(msg)
        if annual_dim.model.include_leap_day and is_leap_year(years[0].year):
            measured_duration = timedelta(days=366)
        else:
            measured_duration = timedelta(days=365)

    df2 = (
        cross_join(df, dt_df)
        .withColumn(myear_column, F.col(annual_col).cast(StringType()))
        .drop(annual_col)
    )
    frequency = dt_dim.model.frequency
    for column in value_columns:
        df2 = df2.withColumn(column, F.col(column) / (measured_duration / frequency))
    return df2
