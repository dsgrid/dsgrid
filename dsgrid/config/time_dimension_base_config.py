import abc
import pyspark.sql.functions as F

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import TimeIntervalType


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles, abc.ABC):
    """Base class for all time dimension configs"""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, load_data_df, time_columns):
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
    def build_time_dataframe(self):
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
    def convert_dataframe(self, df, project_time_dim):
        """Convert input df to use project's time format and time zone.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        project_time_dim : TimeDimensionBaseConfig

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @abc.abstractmethod
    def get_frequency(self):
        """Return the frequency.

        Returns
        -------
        timedelta

        """

    @abc.abstractmethod
    def get_timestamp_load_data_columns(self):
        """Return the required timestamp columns in the load data table.

        Returns
        -------
        list

        """

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
    def get_time_interval_type(self):
        """Return the time interval type for this dimension.

        Returns
        -------
        TimeIntervalType | None

        """

    @abc.abstractmethod
    def list_expected_dataset_timestamps(self):
        """Return a list of the timestamps expected in the load_data table.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """

    def _convert_time_to_project_time_interval(self, df=None, project_time_dim=None):
        """Shift time to match project time based on TimeIntervalType"""
        if project_time_dim is None:
            return df

        dtime_interval = self.get_time_interval_type()
        ptime_interval = project_time_dim.get_time_interval_type()
        time_col = project_time_dim.get_timestamp_load_data_columns()

        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        print()
        print("=============================")
        print(dtime_interval, ptime_interval)
        print("=============================")
        print()
        if dtime_interval != ptime_interval:
            match (dtime_interval, ptime_interval):
                case (TimeIntervalType.PERIOD_BEGINNING, TimeIntervalType.PERIOD_ENDING):
                    df = df.withColumn(
                        time_col,
                        F.col(time_col)
                        + F.expr(f"INTERVAL {self.get_frequency().seconds} SECONDS"),
                    )
                case (TimeIntervalType.PERIOD_ENDING, TimeIntervalType.PERIOD_BEGINNING):
                    df = df.withColumn(
                        time_col,
                        F.col(time_col)
                        - F.expr(f"INTERVAL {self.get_frequency().seconds} SECONDS"),
                    )

        return df
