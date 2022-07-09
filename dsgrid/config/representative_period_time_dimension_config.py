import abc
import calendar
import logging
from datetime import datetime, timedelta

import pandas as pd

from dsgrid.dimension.time import (
    RepresentativePeriodFormat,
    DatetimeRange,
    LeapDayAdjustmentType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import OneWeekPerMonthByHourType
from dsgrid.utils.timing import track_timing, timer_stats_collector
from .dimensions import RepresentativePeriodTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class RepresentativePeriodTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an RepresentativePeriodTimeDimensionModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # As of now there is only one format. We expect this to grow.
        # It's possible that one function (or set of functions) can handle all permutations
        # of parameters. We can make that determination once we have requirements for more
        # formats.
        if self.model.format == RepresentativePeriodFormat.ONE_WEEK_PER_MONTH_BY_HOUR:
            self._format_handler = OneWeekPerMonthByHourHandler()
        else:
            assert False, f"Unsupported {self.model.format}"

    @staticmethod
    def model_class():
        return RepresentativePeriodTimeDimensionModel

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df):
        self._format_handler.check_dataset_time_consistency(
            self._format_handler.list_expected_dataset_timestamps(self.model.ranges), load_data_df
        )

    def convert_dataframe(self, df, project_time_dim):
        # TODO: Create a dataframe with timestamps covering project_time_dim.model.ranges
        # for all model_years in the df, then join the df to that dataframe on the time columns.
        # Account for time zone.
        return df

    def get_frequency(self):
        return self._format_handler.get_frequency()

    def get_time_ranges(self):
        return self._format_handler.get_time_ranges(self.model.ranges, self.get_tzinfo())

    def get_timestamp_load_data_columns(self):
        return self._format_handler.get_timestamp_load_data_columns()

    def get_tzinfo(self):
        return None

    def list_expected_dataset_timestamps(self):
        return self._format_handler.list_expected_dataset_timestamps(self.model.ranges)


class RepresentativeTimeFormatHandlerBase(abc.ABC):
    """Provides implementations for different representative time formats."""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, expected_timestamps, load_data_df):
        """Check consistency between time ranges from the time dimension and load data.

        Parameters
        ----------
        expected_timestamps : list
        load_data_df : pyspark.sql.DataFrame

        Raises
        ------
        DSGInvalidDataset

        """

    @abc.abstractmethod
    def get_frequency(self):
        """Return the frequency.

        Returns
        -------
        timedelta

        """

    @staticmethod
    @abc.abstractmethod
    def get_timestamp_load_data_columns():
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
    def list_expected_dataset_timestamps(self):
        """Return a list of the timestamps expected in the load_data table.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """


class OneWeekPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one week per month."""

    def check_dataset_time_consistency(self, expected_timestamps, load_data_df):
        logger.info("Check OneWeekPerMonthByHourHandler dataset time consistency.")
        actual_timestamps = [
            OneWeekPerMonthByHourType(month=x.month, day_of_week=x.day_of_week, hour=x.hour)
            for x in load_data_df.select("month", "day_of_week", "hour")
            .distinct()
            .sort("month", "day_of_week", "hour")
            .collect()
        ]
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

    def get_frequency(self):
        return timedelta(hours=1)

    def get_time_ranges(self, ranges, _):
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
                    leap_day_adjustment=LeapDayAdjustmentType.NONE,
                )
            )

        return time_ranges

    @staticmethod
    def get_timestamp_load_data_columns():
        return ["month", "day_of_week", "hour"]

    def list_expected_dataset_timestamps(self, ranges):
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
