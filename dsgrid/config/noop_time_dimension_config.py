from datetime import timedelta

from dsgrid.spark.types import DataFrame
from .dimensions import NoOpTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


class NoOpTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an NoOpTimeDimensionModel."""

    @staticmethod
    def model_class() -> NoOpTimeDimensionModel:
        return NoOpTimeDimensionModel

    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        pass

    def build_time_dataframe(self):
        raise NotImplementedError(f"Cannot build a time dataframe for a {type(self)}")

    # def build_time_dataframe_with_time_zone(self):
    #     pass

    def convert_dataframe(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def get_frequency(self) -> timedelta:
        return timedelta(days=0)

    def get_time_ranges(self) -> list:
        return []

    def get_start_times(self) -> list:
        return []

    def get_lengths(self) -> list:
        return []

    def get_load_data_time_columns(self) -> list:
        return []

    def get_time_zone(self) -> None:
        return None

    def get_tzinfo(self) -> None:
        return None

    def get_time_interval_type(self) -> None:
        return None

    def list_expected_dataset_timestamps(self) -> list:
        return []
