import logging
from datetime import datetime, timedelta
from typing import Union

import chronify
import pandas as pd

from dsgrid.dimension.time import (
    IndexTimeRange,
    DatetimeRange,
)

from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidParameter

from dsgrid.spark.types import (
    DataFrame,
    IntegerType,
    StructField,
    StructType,
)
from dsgrid.time.types import DatetimeTimestampType, IndexTimestampType
from dsgrid.utils.timing import timer_stats_collector, track_timing

from dsgrid.utils.spark import (
    get_spark_session,
)
from .dimensions import IndexTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.config.dimensions import TimeRangeModel
from dsgrid.dimension.time import TimeIntervalType


logger = logging.getLogger(__name__)


class IndexTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a IndexTimeDimensionModel."""

    @staticmethod
    def model_class() -> IndexTimeDimensionModel:
        return IndexTimeDimensionModel

    def supports_chronify(self) -> bool:
        return True

    def to_chronify(
        self,
    ) -> Union[
        chronify.IndexTimeRangeTZ, chronify.IndexTimeRangeNTZ, chronify.IndexTimeRangeLocalTime
    ]:
        time_cols = self.get_load_data_time_columns()
        assert len(self._model.ranges) == 1
        assert len(time_cols) == 1

        # IndexTimeDimensionModel does not map to IndexTimeRangeNTZ and TZ at the moment
        assert self.get_time_zone() is None
        config = chronify.IndexTimeRangeLocalTime(
            time_column=time_cols[0],
            start=self._model.ranges[0].start,
            length=self.get_lengths()[0],
            start_timestamp=pd.Timestamp(self.get_start_times()[0]),
            resolution=self._model.frequency,
            time_zone_column="time_zone",
            measurement_type=self._model.measurement_type,
            interval_type=self._model.time_interval_type,
        )
        return config

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check IndexTimeDimensionConfig dataset time consistency.")
        if len(time_columns) > 1:
            msg = f"IndexTimeDimensionConfig expects only one time column, but has {time_columns=}"
            raise DSGInvalidParameter(msg)
        time_col = time_columns[0]

        # check indices are consistent with ranges
        index_ranges = self.get_time_ranges()
        assert len(index_ranges) == 1, len(index_ranges)
        index_range = index_ranges[0]

        expected_indices = set(index_range.list_time_range())
        actual_indices = set(
            [
                x[time_col]
                for x in load_data_df.filter(f"{time_col} is not null")
                .select(time_col)
                .distinct()
                .collect()
            ]
        )

        if expected_indices != actual_indices:
            mismatch = sorted(expected_indices.symmetric_difference(actual_indices))
            raise DSGInvalidDataset(
                f"load_data {time_col}s do not match expected times. mismatch={mismatch}"
            )

    def build_time_dataframe(self) -> DataFrame:
        # shows time as indices
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        model_time = self.list_expected_dataset_timestamps()
        schema = StructType(
            [
                StructField(time_col, IntegerType(), False),
            ]
        )
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    def get_frequency(self) -> timedelta:
        return self.model.frequency

    def get_time_ranges(self) -> list[IndexTimeRange]:
        dt_ranges = self._create_represented_time_ranges()
        ranges = []
        time_ranges = self._build_time_ranges(
            dt_ranges, self.model.str_format, tz=self.get_tzinfo()
        )
        for index_range, time_range in zip(self.model.ranges, time_ranges):
            ranges.append(
                IndexTimeRange(
                    start=time_range[0],
                    end=time_range[1],
                    frequency=self.model.frequency,
                    start_index=index_range.start,
                )
            )

        return ranges

    def _get_represented_time_ranges(self) -> list[DatetimeRange]:
        dt_ranges = self._create_represented_time_ranges()
        ranges = []
        for start, end in self._build_time_ranges(
            dt_ranges, self.model.str_format, tz=self.get_tzinfo()
        ):
            ranges.append(
                DatetimeRange(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                )
            )

        return ranges

    def get_start_times(self) -> list[pd.Timestamp]:
        """get represented start times"""
        tz = self.get_tzinfo()
        start_times = []
        for ts in self.model.starting_timestamps:
            start = datetime.strptime(ts, self.model.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))
        return start_times

    def get_lengths(self) -> list[int]:
        return [trange.end - trange.start + 1 for trange in self.model.ranges]

    def get_load_data_time_columns(self) -> list[str]:
        return list(IndexTimestampType._fields)

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self) -> list[IndexTimestampType]:
        # list timestamps as indices
        indices = []
        for index_range in self.get_time_ranges():
            indices += [IndexTimestampType(x) for x in index_range.list_time_range()]
        return indices

    def _list_represented_dataset_timestamps(self) -> list[DatetimeTimestampType]:
        timestamps = []
        for time_range in self.get_time_ranges():
            timestamps += [DatetimeTimestampType(x) for x in time_range.list_time_range()]
        return timestamps

    def _create_represented_time_ranges(self) -> list[TimeRangeModel]:
        """create datetime range models from index ranges."""
        ranges = []
        for ts, range in zip(self.model.starting_timestamps, self.model.ranges):
            start = datetime.strptime(ts, self.model.str_format)
            steps = range.end - range.start
            end = start + self.model.frequency * steps
            ts_range = TimeRangeModel(start=str(start), end=str(end))
            ranges.append(ts_range)
        return ranges
