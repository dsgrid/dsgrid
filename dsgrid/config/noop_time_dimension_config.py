from datetime import timedelta

from dsgrid.dimension.time import TimeZone, make_time_range
from .dimensions import NoOpTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class NoOpTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an NoOpTimeDimensionModel."""

    @staticmethod
    def model_class():
        return NoOpTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_for_time_check):
        return load_data_for_time_check

    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return timedelta(days=0)

    def get_time_ranges(self):
        frequency = self.get_frequency()
        ranges = [
            make_time_range(
                start=None,
                end=None,
                frequency=frequency,
                leap_day_adjustment=None,
            )
        ]

        return ranges

    def get_timestamp_load_data_columns(self):
        return []

    def get_tzinfo(self):
        return TimeZone.NONE

    def get_time_ranges_tzinfo(self):
        return TimeZone.NONE
