from datetime import timedelta

from dsgrid.dimension.time import make_time_range
from .dimensions import NoOpTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class NoOpTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an NoOpTimeDimensionModel."""

    @staticmethod
    def model_class():
        return NoOpTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df, time_columns):
        pass

    def build_time_dataframe(self):
        raise NotImplementedError(f"Cannot build a time dataframe for a {type(self)}")

    # def build_time_dataframe_with_time_zone(self):
    #     pass

    def convert_dataframe(self, df, project_time_dim, model_years=None, value_columns=None):
        return df

    def get_frequency(self):
        return timedelta(days=0)

    def get_time_ranges(self, model_years=None):
        frequency = self.get_frequency()
        ranges = [
            make_time_range(
                start=None,
                end=None,
                frequency=frequency,
                leap_day_adjustment=None,
                time_interval_type=None,
            )
        ]

        return ranges

    def get_timestamp_load_data_columns(self):
        return []

    def get_tzinfo(self):
        return None

    def get_time_interval_type(self):
        return None

    def list_expected_dataset_timestamps(self, model_years=None):
        return []
