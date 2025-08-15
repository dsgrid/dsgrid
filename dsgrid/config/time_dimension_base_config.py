import abc
import logging
from datetime import timedelta, tzinfo
from typing import Any

import chronify

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import (
    TimeZone,
    TimeIntervalType,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    build_time_ranges,
)
from dsgrid.config.dimensions import TimeRangeModel

from dsgrid.spark.types import (
    DataFrame,
)


logger = logging.getLogger(__name__)


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles, abc.ABC):
    """Base class for all time dimension configs"""

    def supports_chronify(self) -> bool:
        """Return True if the config can be converted to chronify."""
        return False

    # @abc.abstractmethod
    def to_chronify(self) -> chronify.TimeBaseModel:
        """Return the chronify version of the time model."""
        # This is likely temporary until we can use chronify models directly.
        msg = f"{type(self)}.to_chronify"
        raise NotImplementedError(msg)

    def check_dataset_time_consistency(self, load_data_df, time_columns: list[str]) -> None:
        """Check consistency of the load data with the time dimension.

        Parameters
        ----------
        load_data_df : pyspark.sql.DataFrame
        time_columns : list[str]

        Raises
        ------
        DSGInvalidDataset
            Raised if the dataset is inconsistent with the time dimension.
        """
        msg = f"{type(self)}.check_dataset_time_consistency is not implemented"
        raise NotImplementedError(msg)

    def build_time_dataframe(self) -> DataFrame:
        """Build time dimension as specified in config in a spark dataframe.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        msg = f"{self.__class__.__name__}.build_time_dataframe is not implemented"
        raise NotImplementedError(msg)

    @abc.abstractmethod
    def get_frequency(self) -> timedelta:
        """Return the frequency.

        Returns
        -------
        timedelta
        """

    @abc.abstractmethod
    def get_load_data_time_columns(self) -> list[str]:
        """Return the required timestamp columns in the load data table.

        Returns
        -------
        list
        """

    def list_load_data_columns_for_query_name(self) -> list[str]:
        """Return the time columns expected in the load data table for this dimension's query name.

        Returns
        -------
        list[str]
        """
        # This may need to be re-implemented by child classes.
        return [self.model.name]

    def map_timestamp_load_data_columns_for_query_name(self, df) -> DataFrame:
        """Map the timestamp columns in the load data table to those specified by the query name.

        Parameters
        ----------
        df : pyspark.sql.DataFrame

        Returns
        -------
        pyspark.sql.DataFrame
        """
        time_cols = self.get_load_data_time_columns()
        if len(time_cols) > 1:
            msg = (
                "Handling of multiple time columns needs to be implemented in the child class: "
                f"{type(self)}: {time_cols=}"
            )
            raise NotImplementedError(msg)

        time_col = time_cols[0]
        if time_col not in df.columns:
            return df
        return df.withColumnRenamed(time_col, self.model.name)

    def get_time_ranges(self) -> list[Any]:
        """Return time ranges with timezone applied.

        Returns
        -------
        list
            list of DatetimeRange
        """
        msg = f"{type(self)}.get_time_ranges is not implemented"
        raise NotImplementedError(msg)

    @abc.abstractmethod
    def get_start_times(self) -> list[Any]:
        """Return the list of starting timestamp (with tzinfo) for this dimension.
        One per time range.

        Returns
        -------
        list[Any]
        """

    @abc.abstractmethod
    def get_lengths(self) -> list[int]:
        """Return the list of time range length (number of time steps) for this dimension.
        One per time range.

        Returns
        -------
        list[Any]
        """

    @abc.abstractmethod
    def get_time_zone(self) -> TimeZone | None:
        """Return a TimeZone instance for this dimension."""

    @abc.abstractmethod
    def get_tzinfo(self) -> tzinfo | None:
        """Return a tzinfo instance for this dimension.

        Returns
        -------
        tzinfo | None
        """

    @abc.abstractmethod
    def get_time_interval_type(self) -> TimeIntervalType:
        """Return the time interval type for this dimension.

        Returns
        -------
        TimeIntervalType
        """

    def list_expected_dataset_timestamps(
        self,
        time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
    ) -> list[tuple]:
        """Return a list of the timestamps expected in the load_data table.
        Parameters
        ----------
        time_based_data_adjustmen : TimeBasedDataAdjustmentModel | None

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """
        msg = f"{type(self)}.list_expected_dataset_timestamps is not implemented"
        raise NotImplementedError(msg)

    def convert_time_format(self, df: DataFrame, update_model: bool = False) -> DataFrame:
        """Convert time from str format to datetime if exists."""
        return df

    def _build_time_ranges(
        self,
        time_ranges: TimeRangeModel,
        str_format: str,
        tz: TimeZone | None = None,
    ):
        return build_time_ranges(time_ranges, str_format, tz=tz)
