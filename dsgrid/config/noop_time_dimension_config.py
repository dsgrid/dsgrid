from datetime import timedelta

from dsgrid.dimension.time import make_time_range
from .dimensions import NoOpTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class NoOpTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an NoOpTimeDimensionModel."""

    @staticmethod
    def model_class():
        return NoOpTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df):
        pass

    def convert_dataframe(self, df, project_time_dim):
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
        return None

    def list_expected_dataset_timestamps(self):
        return []
