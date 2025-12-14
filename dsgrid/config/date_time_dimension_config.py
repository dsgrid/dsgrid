import logging
from datetime import datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo

import pandas as pd

import chronify

from dsgrid.dimension.time import TimeZoneFormat, TimeIntervalType
from .dimensions import DateTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig

logger = logging.getLogger(__name__)


class DateTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DateTimeDimensionModel."""

    @staticmethod
    def model_class() -> DateTimeDimensionModel:
        return DateTimeDimensionModel

    def supports_chronify(self) -> bool:
        return True

    def to_chronify(self) -> chronify.DatetimeRange:
        time_cols = self.get_load_data_time_columns()
        assert len(self._model.ranges) == 1
        assert len(time_cols) == 1
        # TODO: issue #341: this is actually tied to the weather_year problem #340
        # If there are no ranges, all of this must be dynamic.
        # The two issues should be solved together.
        datetime_type = self._get_datetime_type()

        if datetime_type == "tz_aware_datetime_single_tz":
            return chronify.DatetimeRange(
                time_column=time_cols[0],
                start=pd.Timestamp(self.get_start_times()[0]),
                length=self.get_lengths()[0],
                resolution=self.get_frequency(),
                measurement_type=self._model.measurement_type,
                interval_type=self._model.time_interval_type,
            )
        if datetime_type == "tz_naive_datetime_single_tz":
            # localize to time zones, may do this outside of Chronify
            msg = "dsgrid does not support NTZ datetime in dataframe yet"
            raise NotImplementedError(msg)
        if datetime_type == "tz_aware_datetime_multiple_tz":
            return chronify.DatetimeRangeWithTZColumn(
                time_column=time_cols[0],
                start=pd.Timestamp(self.get_start_times()[0]),
                length=self.get_lengths()[0],
                resolution=self.get_frequency(),
                time_zone_column="time_zone",
                time_zones=self.get_time_zones(),
                measurement_type=self._model.measurement_type,
                interval_type=self._model.time_interval_type,
            )
        if datetime_type == "tz_naive_datetime_multiple_tz":
            # localize to time zones, may do this outside of Chronify
            msg = "dsgrid does not support NTZ datetime in dataframe yet"
            raise NotImplementedError(msg)

    def get_frequency(self) -> timedelta:
        freqs = [trange.frequency for trange in self.model.ranges]
        if len(set(freqs)) > 1:
            msg = f"DateTimeDimensionConfig.get_frequency found multiple frequencies: {freqs}"
            raise ValueError(msg)
        return freqs[0]

    def get_start_times(self) -> list[pd.Timestamp]:
        tz = self.get_tzinfo()
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, trange.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))
        return start_times

    def get_lengths(self) -> list[int]:
        tz = self.get_tzinfo()
        lengths = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, trange.str_format)
            end = datetime.strptime(trange.end, trange.str_format)
            assert start.tzinfo is None
            assert end.tzinfo is None
            start_utc = start.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            end_utc = end.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            freq = trange.frequency
            length = (end_utc - start_utc) / freq + 1
            assert length % 1 == 0, f"{length=} is not a whole number"
            lengths.append(int(length))
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        return [self.model.time_column]

    def get_time_zone(self) -> str | None:
        if self.model.time_zone_format.format_type == TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME:
            return self.model.time_zone_format.time_zone
        return None

    def get_time_zones(self) -> list[str]:
        if self.model.time_zone_format.format_type == TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME:
            return [self.model.time_zone_format.time_zone]
        if self.model.time_zone_format.format_type == TimeZoneFormat.ALIGNED_IN_CLOCK_TIME:
            return self.model.time_zone_format.time_zones
        return []

    def get_tzinfo(self) -> tzinfo | None:
        time_zone = self.get_time_zone()
        if time_zone is None:
            return None
        return ZoneInfo(time_zone)

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type

    def _get_datetime_type(self) -> str:
        """Return a string representing the datetime type for this dimension."""
        match (self.model.time_zone_format.format_type, self.model.localize_to_time_zone):
            case (TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME, True):
                return "tz_aware_datetime_single_tz"
            case (TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME, False):
                return "tz_naive_datetime_single_tz"
            case (TimeZoneFormat.ALIGNED_IN_CLOCK_TIME, True):
                return "tz_aware_datetime_multiple_tz"
            case (TimeZoneFormat.ALIGNED_IN_CLOCK_TIME, False):
                return "tz_naive_datetime_multiple_tz"
            case _:
                msg = (
                    f"Unsupported combination of format_type {self.model.time_zone_format.format_type} "
                    f"and localize_to_time_zone {self.model.localize_to_time_zone}"
                )
                raise ValueError(msg)
