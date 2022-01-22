import abc


class DatasetSchemaHandlerBase(abc.ABC):
    """ define interface/required behaviors per dataset schema """

    @abc.abstractmethod
    def check_consistency(self):
        """
        check all data consistencies, including data columns, dataset to dimension records, and time
        """
