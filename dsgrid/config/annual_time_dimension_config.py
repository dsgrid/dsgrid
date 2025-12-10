import logging
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import AnnualTimeRange
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import AnnualTimestampType
from dsgrid.dimension.time_utils import is_leap_year, build_annual_ranges
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

    def get_frequency(self) -> relativedelta:
        freqs = [trange.frequency for trange in self.model.ranges]
        assert set(freqs) == {freqs[0]}, freqs
        return relativedelta(years=freqs[0])

    def get_time_ranges(self) -> list[AnnualTimeRange]:
        ranges = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            ranges.append(
                AnnualTimeRange(
                    start=start,
                    end=end,
                    frequency=freq,
                )
            )

        return ranges

    def get_start_times(self) -> list[pd.Timestamp]:
        start_times = []
        for start, _, _ in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            start_times.append(start)

        return start_times

    def get_lengths(self) -> list[int]:
        lengths = []
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            length = (end.year - start.year) // freq
            if (end.year - start.year) % freq == 0:
                length += 1
            lengths.append(length)
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
        for start, end, freq in build_annual_ranges(self.model.ranges, tz=self.get_tzinfo()):
            year = start.year
            while year <= end.year:
                timestamps.append(AnnualTimestampType(year))
                year += freq
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
    with set_session_time_zone(dt_dim.model.format.time_zone.tz_name):
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
    frequency = dt_dim.get_frequency()
    for column in value_columns:
        df2 = df2.withColumn(column, F.col(column) / (measured_duration / frequency))
    return df2
