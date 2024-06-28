import abc
import logging
from datetime import timedelta
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql import DataFrame

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import (
    TimeZone,
    TimeIntervalType,
    LeapDayAdjustmentType,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.dimension.time_utils import (
    build_time_ranges,
    filter_to_project_timestamps,
    shift_time_interval,
    time_difference,
    apply_time_wrap,
)
from dsgrid.config.dimensions import TimeRangeModel
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import persist_intermediate_table


logger = logging.getLogger(__name__)


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles, abc.ABC):
    """Base class for all time dimension configs"""

    @abc.abstractmethod
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

    @abc.abstractmethod
    def build_time_dataframe(self) -> DataFrame:
        """Build time dimension as specified in config in a spark dataframe.

        Returns
        -------
        pyspark.sql.DataFrame
        """

    # @abc.abstractmethod
    # def build_time_dataframe_with_time_zone(self):
    #     """Build time dataframe so that relative to spark.sql.session.timeZone, it
    #     appears as expected in config time zone.
    #     Notes: the converted time will need to be converted back to session.timeZone
    #        so spark can intepret it correctly when saving to file in UTC.
    #     Returns
    #     -------
    #     pyspark.sql.DataFrame

    #     """

    @abc.abstractmethod
    def convert_dataframe(
        self,
        df,
        project_time_dim,
        value_columns: set[str],
        scratch_dir_context: ScratchDirContext,
        wrap_time_allowed: bool = False,
        time_based_data_adjustment: Optional[TimeBasedDataAdjustmentModel] = None,
    ) -> DataFrame:
        """Convert input df to use project's time format and time zone.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        project_time_dim : TimeDimensionBaseConfig
        value_columns : set[str]
            Columns in the dataframe that represent load values.
        scratch_dir_context
            Used to persist intermediate tables.
        wrapped_time_allowed : bool
            Whether to allow time-wrapping to align time zone
        time_based_data_adjustment : None | TimeBasedDataAdjustmentModel
            Leap day and daylight saving adjustments to make to both time and data.

        Returns
        -------
        pyspark.sql.DataFrame
        """

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
        return [self.model.dimension_query_name]

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
            raise NotImplementedError(
                "Handling of multiple time columns needs to be implemented in the child class: "
                f"{type(self)}: {time_cols=}"
            )

        return df.withColumnRenamed(time_cols[0], self.model.dimension_query_name)

    @abc.abstractmethod
    def get_time_ranges(self):
        """Return time ranges with timezone applied.

        Returns
        -------
        list
            list of DatetimeRange
        """

    @abc.abstractmethod
    def get_tzinfo(self):
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

    @abc.abstractmethod
    def list_expected_dataset_timestamps(
        self,
        time_based_data_adjustment: Optional[TimeBasedDataAdjustmentModel] = None,
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

    def convert_time_format(self, df: DataFrame) -> DataFrame:
        """Convert time from str format to datetime if exists."""
        return df

    def _convert_time_to_project_time(
        self,
        df: DataFrame,
        project_time_dim: "TimeDimensionBaseConfig",
        context: ScratchDirContext,
        wrap_time: bool = False,
        time_based_data_adjustment: Optional[TimeBasedDataAdjustmentModel] = None,
    ) -> DataFrame:
        """
        Shift time to match project time based on TimeIntervalType, time zone,
        and other attributes.
        - Time-wrapping is applied automatically when aligning time_interval_type.
        - wrap_time_allowed from InputDatasetModel is used to align time due to
        time_zone differences
        """
        if time_based_data_adjustment is None:
            time_based_data_adjustment = TimeBasedDataAdjustmentModel()

        # At least in the case of DECARB-industrial, this is required to get through
        # dataset-to-project mapping on one Kestrel node.
        # That might not be true for other datasets, and so more customization might be
        # required.
        df = persist_intermediate_table(df, context)

        time_col = project_time_dim.get_load_data_time_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]
        time_map = (
            df.select(time_col).distinct().select(time_col, F.col(time_col).alias("orig_ts"))
        ).cache()
        time_map.count()

        time_map = self._align_time_interval_type(time_map, project_time_dim)
        if time_based_data_adjustment.leap_day_adjustment != LeapDayAdjustmentType.NONE:
            time_map = filter_to_project_timestamps(time_map, project_time_dim)

        if wrap_time:
            diff = time_difference(time_map, project_time_dim, difference="left")
            if diff:
                time_map = apply_time_wrap(time_map, project_time_dim, diff)
            else:
                logger.warning("wrap_time is not required, no time misalignment found.")

        time_map_diff = (
            time_map.select((F.col(time_col) - F.col("orig_ts")).alias("diff"))
            .distinct()
            .collect()
        )
        if time_map_diff != [Row(diff=timedelta(0))]:
            other_cols = [x for x in df.columns if x != time_col]
            df = (
                df.select(F.col(time_col).alias("orig_ts"), *other_cols)
                .join(time_map, "orig_ts", "inner")
                .drop("orig_ts")
            )

        time_map.unpersist()
        return df

    def _align_time_interval_type(self, df, project_time_dim):
        """Align time interval type between df and project_time_dim.
        If time range spills over into another year after time interval alignment,
        the time range will be wrapped around so it's bounded within the year.
        Returns
            df: Pyspark dataframe
        """
        dtime_interval = self.get_time_interval_type()
        ptime_interval = project_time_dim.get_time_interval_type()
        time_col = project_time_dim.get_load_data_time_columns()

        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        if dtime_interval == ptime_interval:
            return df

        df = shift_time_interval(
            df, time_col, dtime_interval, ptime_interval, self.get_frequency()
        )

        # Apply time wrap to one timestamp
        if self.model.ranges == project_time_dim.model.ranges:
            project_time = project_time_dim.list_expected_dataset_timestamps()
            time_delta = (
                project_time[-1][0] - project_time[0][0] + project_time_dim.get_frequency()
            ).total_seconds()

            if dtime_interval == TimeIntervalType.PERIOD_BEGINNING:
                df_change = df.join(df.agg(F.max(time_col).alias(time_col)), time_col)
                df = df_change.withColumn(
                    time_col,
                    F.from_unixtime(F.unix_timestamp(time_col) - time_delta).cast("timestamp"),
                ).union(df.exceptAll(df_change))
            elif dtime_interval == TimeIntervalType.PERIOD_ENDING:
                df_change = df.join(df.agg(F.min(time_col).alias(time_col)), time_col).select(
                    *df.columns
                )
                df = df_change.withColumn(
                    time_col,
                    F.from_unixtime(F.unix_timestamp(time_col) + time_delta).cast("timestamp"),
                ).union(df.exceptAll(df_change))
        return df

    def _build_time_ranges(
        self,
        time_ranges: TimeRangeModel,
        str_format: str,
        tz: Optional[TimeZone] = None,
    ):
        return build_time_ranges(time_ranges, str_format, tz=tz)
