from datetime import timedelta

from .dimensions import AnnualTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class():
        return AnnualTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df):
        assert False, "not supported yet"

    def convert_dataframe(self, df):
        return df

    def get_frequency(self):
        return timedelta(days=365)

    def get_time_ranges(self):
        assert False, "not supported yet"

    def get_timestamp_load_data_columns(self):
        return ["year"]

    def get_tzinfo(self):
        return None
