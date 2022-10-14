from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from dsgrid.exceptions import DSGInvalidDimension


class DimensionRecords:
    """Stores dimension records by type."""

    def __init__(self):
        self._store = {}  # {type of DimensionRecordBaseModel: pyspark.sql.dataframe.DataFrame}
        self._spark = SparkSession.getActiveSession()

    def add_dataframe(self, dimension):
        """Add a dataframe to the store.

        Parameters
        ----------
        dimension : DimensionRecordBaseModel

        """
        df = self._spark.createDataFrame((Row(**x) for x in dimension.records))
        # This should not overwhelm memory and should speed-up queries.
        # df.cache()
        assert dimension.cls not in self._store, dimension.cls.__name__
        self._store[dimension.cls] = df
        # This is now duplicate information.
        dimension.records.clear()

    def get_dataframe(self, dimension_class):
        """Return a dataframe from the store.

        Parameters
        ----------
        dimension_class : DimensionRecordBaseModel

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        """
        self._raise_if_df_not_stored(dimension_class)
        return self._store[dimension_class]

    def get_record(self, dimension_class, record_id):
        """Get a record from the store.

        Parameters
        ----------
        dimension_class : DimensionRecordBaseModel
        record_id : id

        """
        self._raise_if_df_not_stored(dimension_class)
        df = self._get_record_by_id(dimension_class, record_id)
        if df.rdd.isEmpty():
            raise DSGInvalidDimension(f"{dimension_class.__name__} {record_id} is not stored")

        return deserialize_row(dimension_class, df.first())

    def has_record(self, dimension_class, record_id):
        """Return true if the record is stored.

        Parameters
        ----------
        dimension_class : DimensionRecordBaseModel
        record_id : id

        """
        if dimension_class not in self._store:
            return False

        return not self._get_record_by_id(dimension_class, record_id).rdd.isEmpty()

    def iter_dimension_classes(self, base_class=None):
        """Return an iterator over the stored dimension classes.

        Parameters
        ----------
        base_class : type | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        iterator
            model classes representing each dimension

        """
        if base_class is None:
            return self._store.keys()
        return (x for x in self._store if issubclass(x, base_class))

    def iter_records(self, dimension_class):
        """Return an iterator over the records for dimension_class.

        Parameters
        ----------
        dimension_class : DimensionRecordBaseModel
            dataclass

        Returns
        -------
        iterator

        """
        self._raise_if_df_not_stored(dimension_class)
        for row in self._store[dimension_class].rdd.toLocalIterator():
            yield deserialize_row(dimension_class, row)

    def list_records(self, dimension_class):
        """Return the records for the dimension_class.

        Returns
        -------
        list
            list of dataclass instances

        """
        return sorted(list(self.iter_records(dimension_class)), key=lambda x: x.id)

    def _get_record_by_id(self, dimension_class, record_id):
        return self._store[dimension_class].filter(F.col("id") == record_id)

    def _raise_if_df_not_stored(self, dimension_class):
        if dimension_class not in self._store:
            raise DSGInvalidDimension(f"{dimension_class} is not stored")


def deserialize_row(dimension_class, row):
    """Return an instance of the dimension_class deserialized from a Row.

    Parameters
    ----------
    dimension_class : DimensionRecordBaseModel
    row : pyspark.sql.types.Row
        row read from spark DataFrame

    Returns
    -------
    dataclass

    """
    return dimension_class(**row.asDict())
