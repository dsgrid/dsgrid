from datetime import timedelta

from .dimensions import TrivialTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class TrivialTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an TrivialTimeDimensionModel."""

    @staticmethod
    def model_class():
        return TrivialTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df):
        pass

    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return timedelta(years=0)

    def get_time_ranges(self):
        return []

    def get_timestamp_load_data_columns(self):
        return []

    def get_tzinfo(self):
        return None
