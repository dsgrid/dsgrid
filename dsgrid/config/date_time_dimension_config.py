import logging
from datetime import datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo

import pandas as pd

import chronify

from dsgrid.dimension.time import TimeZone
from dsgrid.spark.types import DataFrame, F
from dsgrid.dimension.time import DatetimeFormat, TimeIntervalType
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
        return chronify.DatetimeRange(
            time_column=time_cols[0],
            start=pd.Timestamp(self.get_start_times()[0]),
            length=self.get_lengths()[0],
            resolution=self._model.frequency,
            measurement_type=self._model.measurement_type,
            interval_type=self._model.time_interval_type,
        )

    def get_frequency(self) -> timedelta:
        return self.model.frequency

    def get_start_times(self) -> list[pd.Timestamp]:
        tz = self.get_tzinfo()
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            assert start.tzinfo is None
            start_times.append(start.replace(tzinfo=tz))
        return start_times

    def get_lengths(self) -> list[int]:
        tz = self.get_tzinfo()
        lengths = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            end = datetime.strptime(trange.end, self.model.str_format)
            assert start.tzinfo is None
            assert end.tzinfo is None
            start_utc = start.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            end_utc = end.replace(tzinfo=tz).astimezone(tz=ZoneInfo("UTC"))
            freq = self.get_frequency()
            length = (end_utc - start_utc) / freq + 1
            assert length % 1 == 0, f"{length=} is not a whole number"
            lengths.append(int(length))
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        return [self.model.time_column]

    def get_time_zone(self) -> TimeZone | None:
        if self.model.datetime_format.format_type == DatetimeFormat.ALIGNED:
            return self.model.datetime_format.timezone
        if self.model.datetime_format.format_type in [DatetimeFormat.LOCAL_AS_STRINGS]:
            return None
        msg = f"Undefined time zone for {self.model.datetime_format.format_type=}"
        raise NotImplementedError(msg)

    def get_tzinfo(self) -> tzinfo | None:
        time_zone = self.get_time_zone()
        if time_zone is None:
            return None
        return time_zone.tz

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type

    def convert_time_format(self, df: DataFrame, update_model: bool = False) -> DataFrame:
        if self.model.datetime_format.format_type != DatetimeFormat.LOCAL_AS_STRINGS:
            return df
        time_col = self.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        df = df.withColumn(
            time_col,
            F.to_timestamp(time_col, self.model.datetime_format.data_str_format),
        )
        if update_model:
            # TODO: The code doesn't support DatetimeFormat.LOCAL.
            # self.model.datetime_format.format_type = DatetimeFormat.LOCAL
            msg = "convert_time_format DatetimeFormat.LOCAL_AS_STRINGS update_model=True"
            raise NotImplementedError(msg)
        return df
