"""TBD"""

import logging
import os

import pyspark
from pyspark.sql import functions as F

from dsgrid.utils.files import load_data
from dsgrid.utils.spark import init_spark


logger = logging.getLogger(__name__)


class Dataset:
    """Contains metadata and data for a sector."""

    DATA_FILENAME = "load_data.parquet"
    LOOKUP_FILENAME = "load_data_lookup.parquet"

    def __init__(self, input_dataset, load_data_lookup, data, store):
        self._spark = store.spark  # SparkSession
        self._load_data_lookup = load_data_lookup  # DataFrame of dimension elements
        self._load_data = data  # DataFrame containing load data
        self._store = store  # DimensionStore: contains dimension mappings
        self._record_store = store.record_store  # DimensionRecords
        self._input_dataset = input_dataset  # Pydantic model
        self._id = input_dataset.dataset_id

    @classmethod
    def load(cls, input_dataset, store):
        """Load a dataset from a store.

        Parameters
        ----------
        path : str

        Returns
        -------
        Dataset

        """
        spark = store.spark
        path = input_dataset.path

        load_data_lookup = spark.read.parquet(os.path.join(path, cls.LOOKUP_FILENAME))
        data = spark.read.parquet(os.path.join(path, cls.DATA_FILENAME))
        logger.debug("Loaded Dataset from %s", path)
        dataset = cls(input_dataset, load_data_lookup, data, store)
        dataset.make_tables()
        return dataset

    @staticmethod
    def make_table_name(dataset_id, name):
        """Make a table name specific to the dataset ID"""
        return f"{dataset_id}__{name}"

    def make_tables(self):
        # TODO: should we create these in a separate database?
        self._load_data_lookup.createOrReplaceTempView(
            self.make_table_name(self._id, "load_data_lookup")
        )
        self._load_data.createOrReplaceTempView(
            self.make_table_name(self._id, "load_data")
        )

    @property
    def spark(self):
        """Return the SparkSession instance."""
        return self._spark

    # TODO: this is likely throwaway code

    #def compute_sum_by_sector_id(self):
    #    """Compute the sum for each sector.

    #    Returns
    #    -------
    #    pyspark.sql.dataframe.DataFrame

    #    """
    #    sums = self._load_data.groupby("id") \
    #        .sum() \
    #        .drop("sum(id)") \
    #        .withColumnRenamed("id", "data_id")
    #    expr = [F.col(x) * F.col("scale_factor") for x in sums.columns]
    #    expr += ["id", "sector_id"]
    #    return self._load_data_lookup.join(sums, "data_id").select(expr)

    #def aggregate_sector_sums_by_dimension(self, from_dimension, to_dimension):
    #    """Aggregate the sums for each sector for a dimension.

    #    Parameters
    #    ----------
    #    from_dimension : class
    #    to_dimension : class

    #    Returns
    #    -------
    #    pyspark.sql.dataframe.DataFrame

    #    Examples
    #    --------
    #    >>> df = dataset.aggregate_sum_by_dimension(County, State)

    #    """
    #    #self_store.get_scale_factor(from_dimension, to_dimension)
    #    from_df = self._store.get_dataframe(from_dimension)
    #    data_by_sector_id = self.compute_sum_by_sector_id()
    #    key = self._store.get_dimension_mapping_key(from_dimension, to_dimension)
    #    df = data_by_sector_id.join(from_df.select("id", key), "id")
    #    return df.groupby("sector_id", key).sum()
