import abc


class DatasetSchemaHandlerBase(abc.ABC):
    """ define interface/required behaviors per dataset schema """

    @abc.abstractmethod
    def check_consistency(self):
        """
        check all data consistencies, including data columns, dataset to dimension records, and time
        """

    @abc.abstractmethod
    def get_pivot_dimension_columns(self):
        """
        get cols for the dimension that is pivoted in load_data.

        return:

        list of column names

        """
