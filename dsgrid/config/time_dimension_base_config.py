import abc
import pyspark.sql.functions as F

from .dimension_config import DimensionBaseConfigWithoutFiles
from dsgrid.dimension.time import TimeIntervalType
from dsgrid.exceptions import DSGInvalidOperation


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

    def _convert_time_to_project_time_interval(
        self, df=None, project_time_dim=None, wrap_time=True
    ):
        """Shift time to match project time based on TimeIntervalType
        If time range spills over into another year after time interval alignment,
        the time range will be wrapped around so it's bounded within the year
        """
        if project_time_dim is None:
            return df

        dtime_interval = self.get_time_interval_type()
        ptime_interval = project_time_dim.get_time_interval_type()
        time_col = project_time_dim.get_timestamp_load_data_columns()

        assert len(time_col) == 1, time_col
        time_col = time_col[0]

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

        if wrap_time:
            df = self._wrap_time_around_year(df, project_time_dim)

        return df

    @staticmethod
    def _wrap_time_around_year(df, project_time_dim):
        """If dataset_time does not match project_time, apply time-wrapping"""

        time_col = project_time_dim.get_timestamp_load_data_columns()
        assert len(time_col) == 1, time_col
        time_col = time_col[0]

        project_time = set(
            project_time_dim.build_time_dataframe().agg(F.collect_set(time_col)).collect()[0][0]
        )
        dataset_time = set(df.agg(F.collect_set(time_col)).collect()[0][0])
        diff = dataset_time.difference(project_time)

        if not diff:
            return df

        # extract time_delta based on if diff is to the left or right of project_time
        time_delta = (
            max(project_time) - min(project_time) + project_time_dim.get_frequency()
        ).total_seconds()
        if min(diff) > max(project_time):
            time_delta *= -1

        # time-wrap by "changing" the year with time_delta
        df = (
            df.filter(F.col(time_col).isin(diff))
            .withColumn(
                time_col,
                F.from_unixtime(F.unix_timestamp(time_col) + time_delta).cast("timestamp"),
            )
            .union(df.filter(~F.col(time_col).isin(diff)))
        )

        dataset_time = set(df.agg(F.collect_set(time_col)).collect()[0][0])
        if not dataset_time.issubset(project_time):
            raise DSGInvalidOperation(
                "Dataset time column after self._wrap_time_around_year() does not match project_time"
            )

        return df
