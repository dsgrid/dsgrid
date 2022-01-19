import abc
from collections import namedtuple
from datetime import timedelta

import pyspark.sql.functions as F

from dsgrid.dimension.time import RepresentativePeriodFormat, TimeZone
from dsgrid.exceptions import DSGInvalidDataset
from .dimensions import RepresentativePeriodTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


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

    def check_dataset_time_consistency(self, load_data_df):
        self._format_handler.check_dataset_time_consistency(
            self._format_handler.get_expected_timestamps(self.model.ranges), load_data_df
        )

    def convert_dataframe(self, df):
        # TODO: broken
        return df

    def get_frequency(self):
        return self._format_handler.get_frequency()

    def get_time_ranges(self):
        return self._format_handler.get_time_ranges(self.model.ranges)

    def get_timestamp_load_data_columns(self):
        return self._format_handler.get_timestamp_load_data_columns()

    def get_tzinfo(self):
        return None


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
    def get_expected_timestamps(self, ranges):
        """Return a list of times required to be in the data.

        Parameters
        ----------
        ranges : list

        Returns
        -------
        list
            list of datetime

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

    def get_tzinfo(self):
        """Return a tzinfo instance for this dimension.

        Returns
        -------
        tzinfo

        """
        assert self.model.timezone is not TimeZone.LOCAL
        return self.model.timezone.tz


OneWeekPerMonthByHourType = namedtuple(
    "OneWeekPerMonthByHourType", ["month", "day_of_week", "hour"]
)


class OneWeekPerMonthByHourHandler(RepresentativeTimeFormatHandlerBase):
    """Handler for format with hourly data that includes one week per month."""

    def check_dataset_time_consistency(self, expected_timestamps, load_data_df):
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

        timestamps_by_id = (
            load_data_df.select("month", "day_of_week", "hour", "id")
            .groupby("id")
            .agg(F.countDistinct("month", "day_of_week", "hour").alias("distinct_timestamps"))
        )
        lengths = timestamps_by_id.select("distinct_timestamps").distinct()
        count = lengths.count()
        if count != 1:
            raise DSGInvalidDataset(
                f"All time arrays must have the same times: unique times={count}"
            )

        expected_len = len(expected_timestamps)
        actual = lengths.collect()[0].distinct_timestamps
        if actual != expected_len:
            raise DSGInvalidDataset(
                f"Length of time arrays is incorrect: actual={actual} expected={expected_len}"
            )

    def get_frequency(self):
        return timedelta(hours=1)

    def get_expected_timestamps(self, ranges):
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

    @staticmethod
    def get_timestamp_load_data_columns():
        return ["month", "day_of_week", "hour"]
