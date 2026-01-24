import abc
import logging
from datetime import tzinfo
from typing import Any
import ibis.expr.types as ir
import pandas as pd

import chronify

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import (
    TimeIntervalType,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    build_time_ranges,
)
from dsgrid.config.dimensions import TimeRangeModel


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
        load_data_df : ir.Table
        time_columns : list[str]

        Raises
        ------
        DSGInvalidDataset
            Raised if the dataset is inconsistent with the time dimension.
        """
        msg = f"{type(self)}.check_dataset_time_consistency is not implemented"
        raise NotImplementedError(msg)

    def build_time_dataframe(self) -> ir.Table:
        """Build time dimension as specified in config in a table.

        Returns
        -------
        ir.Table
        """
        msg = f"{self.__class__.__name__}.build_time_dataframe is not implemented"
        raise NotImplementedError(msg)

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

    def map_timestamp_load_data_columns_for_query_name(self, df) -> ir.Table:
        """Map the timestamp columns in the load data table to those specified by the query name.

        Parameters
        ----------
        df : ir.Table

        Returns
        -------
        ir.Table
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
        return df.rename(**{self.model.name: time_col})

    def get_time_ranges(self) -> list[Any]:
        """Return time ranges with time_zone applied.

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
    def get_time_zone(self) -> str | None:
        """Return a time zone instance for this dimension."""

    def get_time_zones(self) -> list[str]:
        """Return a list of time zones for this dimension."""
        if self.get_time_zone():
            return [self.get_time_zone()]
        return []

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

    def _build_time_ranges(
        self,
        time_ranges: list[TimeRangeModel],
        tz: str | None = None,
    ) -> list[tuple[pd.Timestamp, pd.Timestamp, pd.Timedelta]]:
        return build_time_ranges(time_ranges, tz=tz)
