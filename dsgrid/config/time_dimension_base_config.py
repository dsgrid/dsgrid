import abc

from .dimension_config import DimensionBaseConfigWithoutFiles


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles, abc.ABC):
    """Base class for all time dimension configs"""

    @abc.abstractmethod
    def check_dataset_time_consistency(self, load_data_df):
        """Check consistency of the load data with the time dimension.

        Parameters
        ----------
        load_data_df : pyspark.sql.DataFrame

        Raises
        ------
        DSGInvalidDataset
            Raised if the dataset is inconsistent with the time dimension.

        """

    @abc.abstractmethod
    def convert_dataframe(self, df, project_time_dim):
        """Convert a dataframe to use project's time format and time zone.

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
    def list_expected_dataset_timestamps(self):
        """Return a list of the timestamps expected in the load_data table.

        Returns
        -------
        list
            List of tuples of columns representing time in the load_data table.

        """
