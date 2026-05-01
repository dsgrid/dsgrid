import logging
from datetime import datetime, timedelta, tzinfo
from typing import Union
from zoneinfo import ZoneInfo

import chronify
import pandas as pd

from dsgrid.common import TIME_ZONE_COLUMN
from dsgrid.dimension.time import TimeZoneFormat, TimeIntervalType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.time.types import IndexTimestampType
from .dimensions import IndexTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


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
    ) -> Union[chronify.IndexTimeRange, chronify.IndexTimeRangeWithTZColumn]:
        time_cols = self.get_load_data_time_columns()
        assert len(self._model.ranges) == 1
        assert len(time_cols) == 1

        match self.model.time_zone_format.format_type:
            case TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME:
                return chronify.IndexTimeRange(
                    time_column=time_cols[0],
                    start=self._model.ranges[0].start,
                    length=self.get_lengths()[0],
                    start_timestamp=pd.Timestamp(self.get_start_times()[0]),
                    resolution=self.get_frequency(),
                    measurement_type=self._model.measurement_type,
                    interval_type=self._model.time_interval_type,
                )
            case TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME:
                return chronify.IndexTimeRangeWithTZColumn(
                    time_column=time_cols[0],
                    start=self._model.ranges[0].start,
                    length=self.get_lengths()[0],
                    start_timestamp=pd.Timestamp(self.get_start_times()[0]),
                    resolution=self.get_frequency(),
                    time_zone_column=TIME_ZONE_COLUMN,
                    measurement_type=self._model.measurement_type,
                    interval_type=self._model.time_interval_type,
                )
            case _:
                msg = f"Unsupported time zone format for chronify: {self.model.time_zone_format.format_type}"
                raise DSGInvalidParameter(msg)

    def get_frequency(self) -> timedelta:
        freqs = [trange.frequency for trange in self.model.ranges]
        if len(set(freqs)) > 1:
            msg = f"IndexTimeDimensionConfig.get_frequency found multiple frequencies: {freqs}"
            raise ValueError(msg)
        return freqs[0]

    def get_start_times(self) -> list[pd.Timestamp]:
        """get represented start times"""
        tz = self.get_tzinfo()
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.starting_timestamp, trange.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))
        return start_times

    def get_lengths(self) -> list[int]:
        return [trange.end - trange.start + 1 for trange in self.model.ranges]

    def get_load_data_time_columns(self) -> list[str]:
        return list(IndexTimestampType._fields)

    def get_time_zone(self) -> str | None:
        time_zones = self.get_time_zones()
        return time_zones[0] if len(time_zones) == 1 else None

    def get_time_zones(self) -> list[str]:
        return self.model.time_zone_format.get_time_zones()

    def get_tzinfo(self) -> tzinfo | None:
        time_zone = self.get_time_zone()
        if time_zone is None:
            return None
        return ZoneInfo(time_zone)

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type
