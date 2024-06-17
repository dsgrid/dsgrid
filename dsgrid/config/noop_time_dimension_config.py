from datetime import timedelta

from .dimensions import NoOpTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class NoOpTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an NoOpTimeDimensionModel."""

    @staticmethod
    def model_class():
        return NoOpTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df, time_columns):
        pass

    def build_time_dataframe(self, model_years=None):
        raise NotImplementedError(f"Cannot build a time dataframe for a {type(self)}")

    # def build_time_dataframe_with_time_zone(self):
    #     pass

    def convert_dataframe(
        self,
        df,
        project_time_dim,
        model_years=None,
        value_columns=None,
        wrap_time_allowed=False,
        time_based_data_adjustment=None,
    ):
        return df

    def get_frequency(self):
        return timedelta(days=0)

    def get_time_ranges(self, model_years=None):
        return []

    def get_load_data_time_columns(self):
        return []

    def get_tzinfo(self):
        return None

    def get_time_interval_type(self):
        return None

    def list_expected_dataset_timestamps(self, model_years=None):
        return []
