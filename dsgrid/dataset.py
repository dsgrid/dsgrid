"""TBD"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.utils.spark import read_dataframe


logger = logging.getLogger(__name__)


class Dataset:
    """Contains metadata and data for a sector."""

    VIEW_NAMES = ("load_data_lookup", "load_data")

    def __init__(self, config, load_data_lookup, data):
        self._spark = SparkSession.getActiveSession()
        self._load_data_lookup = load_data_lookup  # DataFrame of dimension elements
        self._load_data = data  # DataFrame containing load data
        self._id = config.model.dataset_id
        # Can't use dashes in view names. This will need to be handled when we implement
        # queries based on dataset ID.
        # TODO: do we need a DimensionStore here?

    @property
    def dataset_id(self):
        return self._id

    @classmethod
    def load(cls, config):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig

        Returns
        -------
        Dataset

        """
        load_data = read_dataframe(config.load_data_path)
        load_data_lookup = read_dataframe(config.load_data_lookup_path, cache=True)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        dataset = cls(config, load_data_lookup, load_data)
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

    @property
    def load_data(self):
        return self._load_data

    @property
    def load_data_lookup(self):
        return self._load_data_lookup

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
