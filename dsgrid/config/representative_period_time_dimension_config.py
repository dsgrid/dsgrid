import abc
import calendar
import logging
from datetime import datetime, timedelta
from typing import Type, Any, Union

import chronify
import pandas as pd

from dsgrid.dimension.time import RepresentativePeriodFormat, DatetimeRange, TimeIntervalType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.spark.types import (
    DataFrame,
    StructType,
    StructField,
    IntegerType,
)
from dsgrid.time.types import (
    OneWeekPerMonthByHourType,
    OneWeekdayDayAndOneWeekendDayPerMonthByHourType,
)
from dsgrid.utils.timing import track_timing, timer_stats_collector
from dsgrid.utils.spark import (
    get_spark_session,
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

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:  # DND
        self._format_handler.check_dataset_time_consistency(
            self._format_handler.list_expected_dataset_timestamps(self.model.ranges),
            load_data_df,
            time_columns,
        )

    def build_time_dataframe(self) -> DataFrame:
        time_cols = self.get_load_data_time_columns()
        schema = StructType(
            [StructField(time_col, IntegerType(), False) for time_col in time_cols]
        )
        model_time = self.list_expected_dataset_timestamps()
        df_time = get_spark_session().createDataFrame(model_time, schema=schema)

        return df_time

    # def build_time_dataframe_with_time_zone(self):
    #     return self.build_time_dataframe()

    # def convert_dataframe(self, *args, **kwargs):
    #     msg = f"{self.__class__.__name__}.convert_dataframe is implemented through chronify"
    #     raise NotImplementedError(msg)

    def get_frequency(self) -> timedelta:
        return self._format_handler.get_frequency()

    def get_time_ranges(
        self,
    ) -> list[OneWeekPerMonthByHourType] | list[OneWeekdayDayAndOneWeekendDayPerMonthByHourType]:
        return self._format_handler.get_time_ranges(
            self.model.ranges,
            self.model.time_interval_type,
            self.get_tzinfo(),
        )

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

    def list_expected_dataset_timestamps(
        self,
    ) -> list[OneWeekPerMonthByHourType] | list[OneWeekdayDayAndOneWeekendDayPerMonthByHourType]:
        return self._format_handler.list_expected_dataset_timestamps(self.model.ranges)


class RepresentativeTimeFormatHandlerBase(abc.ABC):
    """Provides implementations for different representative time formats."""

    @staticmethod
    @abc.abstractmethod
    def get_representative_time_type() -> Type:
        """Return the time type representing the data."""

    def check_dataset_time_consistency(
        self, expected_timestamps, load_data_df, time_columns
    ):  # DND
        """Check consistency between time ranges from the time dimension and load data.

        Parameters
        ----------
        expected_timestamps : list
        load_data_df : pyspark.sql.DataFrame
        time_columns : list[str]

        Raises
        ------
        DSGInvalidDataset

        """
        logger.info("Check %s dataset time consistency.", self.__class__.__name__)
        actual_timestamps = []
        for row in load_data_df.select(*time_columns).distinct().sort(*time_columns).collect():
            data = row.asDict()
            num_none = 0
            for val in data.values():
                if val is None:
                    num_none += 1
            if num_none > 0:
                if num_none != len(data):
                    raise DSGInvalidDataset(
                        f"If any time column is null then all columns must be null: {data}"
                    )
            else:
                actual_timestamps.append(self.get_representative_time_type()(**data))

        if expected_timestamps != actual_timestamps:
            mismatch = sorted(
                set(expected_timestamps).symmetric_difference(set(actual_timestamps))
            )
            raise DSGInvalidDataset(
                f"load_data timestamps do not match expected times. mismatch={mismatch}"
            )
        logger.info("Verified that expected_timestamps equal actual_timestamps")

        actual_len = len(actual_timestamps)
        expected_len = len(expected_timestamps)
        if actual_len != expected_len:
            raise DSGInvalidDataset(
                f"Length of time arrays is incorrect: actual={actual_len} expected={expected_len}"
            )
        logger.info("Verified that all time arrays have the same, correct length.")

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

    @abc.abstractmethod
    def get_time_ranges(self):
        """Return a list of DatetimeRange instances for the dataset.

        Returns
        -------
        list
            list of DatetimeRange

        """

    @abc.abstractmethod
    def list_expected_dataset_timestamps(self):  # DND
        """Return a list of the timestamps expected in the load_data table.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """


class OneWeekPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one week per month."""

    @staticmethod
    def get_representative_time_type() -> OneWeekPerMonthByHourType:
        return OneWeekPerMonthByHourType

    def get_frequency(self) -> timedelta:
        return timedelta(hours=1)

    def get_time_ranges(self, ranges, time_interval_type, _) -> list[DatetimeRange]:
        # TODO: This method may have some problems but is currently unused.
        # How to handle year? Leap year?
        time_ranges = []
        for model in ranges:
            if model.end == 2:
                logger.warning("Last day of February may not be accurate.")
            last_day = calendar.monthrange(2021, model.end)[1]
            time_ranges.append(
                DatetimeRange(
                    start=pd.Timestamp(datetime(year=1970, month=model.start, day=1)),
                    end=pd.Timestamp(datetime(year=1970, month=model.end, day=last_day, hour=23)),
                    frequency=timedelta(hours=1),
                )
            )

        return time_ranges

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

    def list_expected_dataset_timestamps(self, ranges) -> list[OneWeekPerMonthByHourType]:  # DND
        timestamps = []
        for model in ranges:
            for month in range(model.start, model.end + 1):
                for day_of_week in range(7):
                    for hour in range(24):
                        ts = OneWeekPerMonthByHourType(
                            month=month, day_of_week=day_of_week, hour=hour
                        )
                        timestamps.append(ts)
        return timestamps


class OneWeekdayDayAndWeekendDayPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one weekday day and one weekend day
    per month.
    """

    @staticmethod
    def get_representative_time_type() -> OneWeekdayDayAndOneWeekendDayPerMonthByHourType:
        return OneWeekdayDayAndOneWeekendDayPerMonthByHourType

    def get_frequency(self) -> timedelta:
        return timedelta(hours=1)

    def get_time_ranges(self, ranges, time_interval_type, _):
        raise NotImplementedError("get_time_ranges")

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

    def list_expected_dataset_timestamps(
        self, ranges
    ) -> list[OneWeekdayDayAndOneWeekendDayPerMonthByHourType]:  # DND
        timestamps = []
        for model in ranges:
            for month in range(model.start, model.end + 1):
                # This is sorted because we sort time columns in the load data.
                for is_weekday in (False, True):
                    for hour in range(24):
                        ts = OneWeekdayDayAndOneWeekendDayPerMonthByHourType(
                            month=month, hour=hour, is_weekday=is_weekday
                        )
                        timestamps.append(ts)
        return timestamps
