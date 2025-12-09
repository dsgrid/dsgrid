import logging
from datetime import timedelta, datetime

import pandas as pd

from dsgrid.dimension.time import DailyTimeRange, TimeBasedDataAdjustmentModel
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DailyTimestampType
from dsgrid.spark.types import (
    DataFrame,
    StructType,
    StructField,
    IntegerType,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import (
    get_spark_session,
)
from .dimensions import DailyTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class DailyTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DailyTimeDimensionModel."""

    @staticmethod
    def model_class() -> DailyTimeDimensionModel:
        return DailyTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check DailyTimeDimensionConfig dataset time consistency.")
        expected_cols = set(self.get_load_data_time_columns())
        actual_cols = set(time_columns)
        if expected_cols != actual_cols:
            msg = (
                f"DailyTimeDimensionConfig expects columns {expected_cols}, "
                f"but got {actual_cols}"
            )
            raise ValueError(msg)

        # Get the year column name (time_year, weather_year, or model_year)
        year_col = self.get_year_column()
        month_col = "month"
        day_col = "day"

        # Get time ranges
        time_ranges = self.get_time_ranges()
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        # Build expected timestamps for all ranges
        expected_timestamps = []
        for time_range in time_ranges:
            expected_timestamps.extend(time_range.list_time_range())

        # Get actual timestamps from dataframe
        actual_timestamps = [
            pd.Timestamp(
                year=x[year_col], month=x[month_col], day=x[day_col], tz=self.get_tzinfo()
            ).to_pydatetime()
            for x in load_data_df.select(year_col, month_col, day_col)
            .distinct()
            .filter(
                f"{year_col} IS NOT NULL AND {month_col} IS NOT NULL AND {day_col} IS NOT NULL"
            )
            .sort(year_col, month_col, day_col)
            .collect()
        ]

        if set(expected_timestamps) != set(actual_timestamps):
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            msg = (
                f"load_data timestamps do not match expected times. "
                f"mismatch count={len(mismatch)}, first 10={mismatch[:10]}"
            )
            raise DSGInvalidDataset(msg)

    def build_time_dataframe(self) -> DataFrame:
        model_time = self.list_expected_dataset_timestamps()
        df_time = get_spark_session().createDataFrame(
            model_time,
            schema=StructType(
                [StructField(col, IntegerType(), False) for col in DailyTimestampType._fields]
            ),
        )
        df_time = df_time.withColumnRenamed("year", self.get_year_column())
        return df_time

    def get_frequency(self) -> timedelta:
        return timedelta(days=1)

    def get_time_ranges(self) -> list[DailyTimeRange]:
        ranges = []
        frequency = self.get_frequency()
        time_based_data_adjustment = TimeBasedDataAdjustmentModel(
            leap_day_adjustment=self.model.leap_day_adjustment
        )
        for start, end in self._build_time_ranges(
            self.model.ranges, self.model.str_format, tz=self.get_tzinfo()
        ):
            start = pd.Timestamp(start)
            end = pd.Timestamp(end)
            ranges.append(
                DailyTimeRange(
                    start=start,
                    end=end,
                    frequency=frequency,
                    time_based_data_adjustment=time_based_data_adjustment,
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
        for time_range in self.get_time_ranges():
            # Count the number of days after leap day adjustment
            lengths.append(len(time_range.list_time_range()))
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        """Return the time columns based on the year_column setting."""
        year_col = self.get_year_column()
        return [year_col, "month", "day"]

    def get_year_column(self) -> str:
        """Return the year column name (time_year, weather_year, or model_year)."""
        return self.model.year_column

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self):
        return self.model.time_interval_type

    def supports_chronify(self) -> bool:
        """Daily time dimension does not support chronify yet."""
        return False

    def list_expected_dataset_timestamps(self) -> list[DailyTimestampType]:
        """List all expected timestamps as DailyTimestampType tuples."""
        timestamps = []

        for time_range in self.get_time_ranges():
            for dt in time_range.list_time_range():
                timestamps.append(
                    DailyTimestampType(
                        year=dt.year,
                        month=dt.month,
                        day=dt.day,
                    )
                )

        return timestamps
