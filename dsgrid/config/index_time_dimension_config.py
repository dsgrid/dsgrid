import logging
from datetime import datetime, timedelta
from typing import Union

import chronify
import pandas as pd

from dsgrid.time.types import IndexTimestampType
from .dimensions import IndexTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig
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
        chronify.IndexTimeRangeTZ, chronify.IndexTimeRangeNTZ, chronify.IndexTimeRangeWithTZColumn
    ]:
        time_cols = self.get_load_data_time_columns()
        assert len(self._model.ranges) == 1
        assert len(time_cols) == 1

        # IndexTimeDimensionModel does not map to IndexTimeRangeNTZ and TZ at the moment
        assert self.get_time_zone() is None
        config = chronify.IndexTimeRangeWithTZColumn(
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

    def get_frequency(self) -> timedelta:
        return self.model.frequency

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
