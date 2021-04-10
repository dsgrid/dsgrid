"""TBD"""

import logging
import os
from pathlib import Path

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from dsgrid.utils.files import load_data


logger = logging.getLogger(__name__)


class Dataset:
    """Contains metadata and data for a sector."""

    DATA_FILENAME = "load_data.parquet"
    LOOKUP_FILENAME = "load_data_lookup.parquet"
    VIEW_NAMES = ("load_data_lookup", "load_data")

    def __init__(self, config, load_data_lookup, data):
        self._spark = SparkSession.getActiveSession()
        self._load_data_lookup = load_data_lookup  # DataFrame of dimension elements
        self._load_data = data  # DataFrame containing load data
        self._id = config.model.dataset_id
        # TODO: do we need a DimensionStore here?

    @classmethod
    def load(cls, config, version=None):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig

        Returns
        -------
        Dataset

        """
        spark = SparkSession.getActiveSession()
        path = Path(config.model.path)
        load_data_lookup = spark.read.parquet(str(path / cls.LOOKUP_FILENAME))
        data = spark.read.parquet(str(path / cls.DATA_FILENAME))
        logger.debug("Loaded Dataset from %s", path)
        dataset = cls(config, load_data_lookup, data)
        return dataset

    def _make_view_name(self, name):
        return f"{self._id}__{name}"

    def _make_view_names(self):
        return (f"{self._id}__{name}" for name in self.VIEW_NAMES)

    def create_views(self):
        """Create views for each of the tables in this dataset."""
        # TODO: should we create these in a separate database?
        self._load_data_lookup.createOrReplaceTempView(self._make_view_name("load_data_lookup"))
        self._load_data.createOrReplaceTempView(self._make_view_name("load_data"))

    def delete_views(self):
        """Delete views of the tables in this dataset."""
        for view in self._make_view_names():
            self._spark.catalog.dropTempView(view)

    # TODO: this is likely throwaway code

    # def compute_sum_by_sector_id(self):
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

    # def aggregate_sector_sums_by_dimension(self, from_dimension, to_dimension):
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
