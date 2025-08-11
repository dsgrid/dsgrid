import abc
import logging
from datetime import timedelta
from typing import Type, Any, Union

import chronify

from dsgrid.dimension.time import RepresentativePeriodFormat, TimeIntervalType
from dsgrid.time.types import (
    OneWeekPerMonthByHourType,
    OneWeekdayDayAndOneWeekendDayPerMonthByHourType,
)
from .dimensions import RepresentativePeriodTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class RepresentativePeriodTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an RepresentativePeriodTimeDimensionModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We expect the list of required formats to grow.
        # It's possible that one function (or set of functions) can handle all permutations
        # of parameters. We can make that determination once we have requirements for more
        # formats.
        match self.model.format:
            case RepresentativePeriodFormat.ONE_WEEK_PER_MONTH_BY_HOUR:
                self._format_handler = OneWeekPerMonthByHourHandler()
            case RepresentativePeriodFormat.ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR:
                self._format_handler = OneWeekdayDayAndWeekendDayPerMonthByHourHandler()
            case _:
                msg = self.model.format.value
                raise NotImplementedError(msg)

    def supports_chronify(self) -> bool:
        return True

    def to_chronify(
        self,
    ) -> Union[chronify.RepresentativePeriodTimeTZ, chronify.RepresentativePeriodTimeNTZ]:
        if len(self._model.ranges) != 1:
            msg = (
                "Mapping RepresentativePeriodTime with chronify is only supported with one range: "
                f"{self._model.ranges}"
            )
            raise NotImplementedError(msg)
        range_ = self._model.ranges[0]
        if range_.start != 1 or range_.end != 12:
            msg = (
                "Mapping RepresentativePeriodTime with chronify is only supported with a full year: "
                f"{range_}"
            )
            raise NotImplementedError(msg)
        # RepresentativePeriodTimeDimensionModel does not map to NTZ at the moment
        if isinstance(self._format_handler, OneWeekPerMonthByHourHandler) or isinstance(
            self._format_handler, OneWeekdayDayAndWeekendDayPerMonthByHourHandler
        ):
            return chronify.RepresentativePeriodTimeTZ(
                measurement_type=self._model.measurement_type,
                interval_type=self._model.time_interval_type,
                time_format=chronify.RepresentativePeriodFormat(self._model.format.value),
                time_zone_column="time_zone",
            )

        msg = f"Cannot chronify time_config for {self._format_handler}"
        raise NotImplementedError(msg)

    @staticmethod
    def model_class() -> RepresentativePeriodTimeDimensionModel:
        return RepresentativePeriodTimeDimensionModel

    def get_frequency(self) -> timedelta:
        return self._format_handler.get_frequency()

    def get_start_times(self) -> list[Any]:
        return self._format_handler.get_start_times(self.model.ranges)

    def get_lengths(self) -> list[int]:
        return self._format_handler.get_lengths(self.model.ranges)

    def get_load_data_time_columns(self) -> list[str]:
        return self._format_handler.get_load_data_time_columns()

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self) -> TimeIntervalType:
        return self.model.time_interval_type


class RepresentativeTimeFormatHandlerBase(abc.ABC):
    """Provides implementations for different representative time formats."""

    @staticmethod
    @abc.abstractmethod
    def get_representative_time_type() -> Type:
        """Return the time type representing the data."""

    @abc.abstractmethod
    def get_frequency(self):
        """Return the frequency.

        Returns
        -------
        timedelta

        """

    @staticmethod
    @abc.abstractmethod
    def get_load_data_time_columns():
        """Return the required timestamp columns in the load data table.

        Returns
        -------
        list

        """


class OneWeekPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one week per month."""

    @staticmethod
    def get_representative_time_type() -> OneWeekPerMonthByHourType:
        return OneWeekPerMonthByHourType

    def get_frequency(self) -> timedelta:
        return timedelta(hours=1)

    @staticmethod
    def get_start_times(ranges) -> list[OneWeekPerMonthByHourType]:
        """Get the starting combination of (month, day_of_week, hour) based on sorted order"""
        start_times = []
        for model in ranges:
            start_times.append(OneWeekPerMonthByHourType(month=model.start, day_of_week=0, hour=0))
        return start_times

    @staticmethod
    def get_lengths(ranges) -> list[int]:
        """Get the number of unique combinations of (month, day_of_week, hour)"""
        lengths = []
        for model in ranges:
            n_months = model.end - model.start + 1
            lengths.append(n_months * 7 * 24)
        return lengths

    @staticmethod
    def get_load_data_time_columns() -> list[str]:
        return list(OneWeekPerMonthByHourType._fields)


class OneWeekdayDayAndWeekendDayPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one weekday day and one weekend day
    per month.
    """

    @staticmethod
    def get_representative_time_type() -> OneWeekdayDayAndOneWeekendDayPerMonthByHourType:
        return OneWeekdayDayAndOneWeekendDayPerMonthByHourType

    def get_frequency(self) -> timedelta:
        return timedelta(hours=1)

    @staticmethod
    def get_start_times(ranges) -> list[OneWeekdayDayAndOneWeekendDayPerMonthByHourType]:
        """Get the starting combination of (month, hour, is_weekday) based on sorted order"""
        start_times = []
        for model in ranges:
            start_times.append(
                OneWeekdayDayAndOneWeekendDayPerMonthByHourType(
                    month=model.start, hour=0, is_weekday=False
                )
            )
        return start_times

    @staticmethod
    def get_lengths(ranges) -> list[int]:
        """Get the number of unique combinations of (month, hour, is_weekday)"""
        lengths = []
        for model in ranges:
            n_months = model.end - model.start + 1
            lengths.append(n_months * 24 * 2)
        return lengths

    @staticmethod
    def get_load_data_time_columns() -> list[str]:
        return list(OneWeekdayDayAndOneWeekendDayPerMonthByHourType._fields)
