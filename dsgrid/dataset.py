"""Provides access to a dataset."""

from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
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

    def __init__(self, schema_handler):
        self._handler = schema_handler
        self._id = schema_handler.config.model.dataset_id
        # Can't use dashes in view names. This will need to be handled when we implement
        # queries based on dataset ID.
        # TODO: do we need a DimensionStore here?

    @property
    def dataset_id(self):
        return self._id

    @classmethod
    def load(cls, config, dimension_mgr, dimension_mapping_mgr):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager
        dimension_mapping_mgr : DimensionMappingRegistryManager

        Returns
        -------
        Dataset

        """
        return cls(make_dataset_schema_handler(config, dimension_mgr, dimension_mapping_mgr))

    def _make_view_name(self, name):
        return f"{self._id}__{name}"

    def _make_view_names(self):
        return (f"{self._id}__{name}" for name in self.VIEW_NAMES)

    def create_views(self):
        """Create views for each of the tables in this dataset."""
        # TODO: should we create these in a separate database?
        self.load_data_lookup.createOrReplaceTempView(self._make_view_name("load_data_lookup"))
        self.load_data.createOrReplaceTempView(self._make_view_name("load_data"))

    def delete_views(self):
        """Delete views of the tables in this dataset."""
        spark = SparkSession.getActiveSession()
        for view in self._make_view_names():
            spark.catalog.dropTempView(view)

    # TODO: the following two methods need to abstract load_data_lookup
    # They can only be used with Standard dataset schema.

    @property
    def load_data(self):
        return self._handler._load_data

    @property
    def load_data_lookup(self):
        return self._handler._load_data_lookup

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
