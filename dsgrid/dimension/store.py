
from dataclasses import fields
import logging
import sqlite3

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

from dsgrid.dimension.base import DSGBaseDimension
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimension, DSGInvalidDimensionMapping
from dsgrid.utils.files import load_data
from dsgrid.utils.spark import init_spark
from dsgrid.utils.timing import timed_debug, timed_info


logger = logging.getLogger(__name__)


class DimensionStore:
    """Provides mapping functionality for project-defined dimensions."""

    REQUIRED_FIELDS = ("id", "name")

    def __init__(self, spark, dimension_mappings):
        self._store = {}  # {DimensionType: pyspark.sql.dataframe.DataFrame}
        self._spark = spark  # SparkSession
        self._dimension_mappings = {
            (x["from"], x["to"]): x["key"] for x in dimension_mappings
        }

    @classmethod
    @timed_debug
    def load(cls, model_mappings, dimension_mappings=None, spark=None):
        """Load a project's dimension dataset records.

        Parameters
        ----------
        model_mappings : dict
            Maps a dataclass to a JSON file defining records for that dataclass

        Returns
        -------
        DimensionStore

        """
        if spark is None:
            spark = init_spark("store")

        # TODO: develop data structure to accept all inputs
        store = cls(spark, dimension_mappings)

        for dimension_type, filename in model_mappings.items():
            if not issubclass(dimension_type, DSGBaseDimension):
                raise DSGInvalidDimension(
                    f"{dimension_type.__name__} is not derived from DSGBaseDimension"
                )

            # Consider handling multiLine
            df = spark.read.json(filename)
            # This should not overwhelm memory and should speed-up queries.
            df.cache()
            store.check_dataframe(dimension_type, df)
            store.add_dataframe(dimension_type, df)

        return store

    @staticmethod
    @timed_debug
    def check_dataframe(dimension_type, df):
        """Validates the dataframe.

        Raises
        ------
        DSGInvalidField
            Raised if a field is invalid.

        """
        cls_fields = sorted(list(dimension_type.__dataclass_fields__.keys()))
        columns = sorted(df.columns)
        if cls_fields != columns:
            raise DSGInvalidField(f"columns don't match: {cls_fields} {columns}")
        for field in DimensionStore.REQUIRED_FIELDS:
            if field not in columns:
                raise DSGInvalidField(f"{dimension_type} must define {field}")
        for field in df.schema.fields:
            if field.name == "id" and not isinstance(field.dataType, StringType):
                raise DSGInvalidField(f"id must be a string: {dimension_type}")

        rows = df.collect()
        num_unique_ids = len(set((x.id for x in rows)))
        if num_unique_ids != len(rows):
            raise DSGInvalidField(f"{dimension_type} does not have unique 'id' values")

    def add_dataframe(self, dimension_type, df):
        """Add a dataframe to the store.

        Parameters
        ----------
        dimension_type : class
        df : pyspark.sql.dataframe.DataFrame

        """
        assert dimension_type not in self._store
        self._store[dimension_type] = df

    def get_dataframe(self, dimension_type):
        """Return a dataframe from the store.

        Parameters
        ----------
        dimension_type : class

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        """
        self._raise_if_table_not_stored(dimension_type)
        return self._store[dimension_type]

    def get_dimension_mapping_key(self, from_type, to_type):
        """Return the key to perform a join from `from_type` to `to_type`.

        Parameters
        ----------
        from_type : class
        to_type : class

        Returns
        -------
        str

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if the mapping is not stored.

        """
        key = (from_type, to_type)
        self._raise_if_mapping_not_stored(key)
        return self._dimension_mappings[key]

    def get_record(self, dimension_type, record_id):
        """Get a record from the store.

        Parameters
        ----------
        dimension_type : class
        record_id : id

        """
        self._raise_if_table_not_stored(dimension_type)
        df = self._get_record_by_id(dimension_type, record_id)
        if df.rdd.isEmpty():
            raise DSGInvalidDimension(f"{dimension_type.__name__} {record_id} is not stored")

        # TODO: consider whether the user wants Spark record or dataclass.
        # Both have the same dot access.
        return df.first()

    def has_record(self, dimension_type, record_id):
        """Return true if the record is stored.

        Parameters
        ----------
        dimension_type : class
        record_id : id

        """
        if dimension_type not in self._store:
            return False

        return not self._get_record_by_id(dimension_type, record_id).rdd.isEmpty()

    def iter_dimension_types(self, base_class=None):
        """Return an iterator over the stored dimension types.

        Parameters
        ----------
        base_class : class | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        iterator
            dataclasses representing each dimension type

        """
        if base_class is None:
            return self._store.keys()
        return (x for x in self._store if issubclass(x, base_class))

    def iter_records(self, dimension_type):
        """Return an iterator over the records for dimension_type.

        Parameters
        ----------
        dimension_type : class
            dataclass

        Returns
        -------
        iterator

        """
        self._raise_if_table_not_stored(dimension_type)
        return self._store[dimension_type].rdd.toLocalIterator()

    def list_dimension_types(self, base_class=None):
        """Return the stored dimension types.

        Parameters
        ----------
        base_class : class | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        list
            list of dataclasses representing each dimension type

        """
        return sorted(list(self.iter_dimension_types(base_class=base_class)),
                      key=lambda x: x.__name__)

    def list_records(self, dimension_type):
        """Return the records for the dimension_type.

        Returns
        -------
        list
            list of dataclass instances

        """
        return sorted(list(self.iter_records(dimension_type)), key=lambda x: x.id)

    @property
    def spark(self):
        """Return the SparkSession instance."""
        return self._spark

    def _get_record_by_id(self, dimension_type, record_id):
        return self._store[dimension_type].filter(F.col("id") == record_id)

    def _raise_if_table_not_stored(self, dimension_type):
        if dimension_type not in self._store:
            raise DSGInvalidDimension(f"{dimension_type} is not stored")

    def _raise_if_mapping_not_stored(self, key):
        if key not in self._dimension_mappings:
            raise DSGInvalidDimensionMapping(f"{key} is not stored")


def deserialize_row(dimension_type, row):
    """Return an instance of the dimension_type deserialized from a tuple.

    Parameters
    ----------
    dimension_type : class
    row : pyspark.sql.types.Row
        row read from spark DataFrame

    Returns
    -------
    dataclass

    """
    return dimension_type(**row.asDict())
