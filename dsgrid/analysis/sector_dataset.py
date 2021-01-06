"""Sector dataset functionality"""

import logging
import os

import pyspark
from pyspark.sql import functions as F

from dsgrid.utils.files import load_data
from dsgrid.utils.spark import init_spark


logger = logging.getLogger(__name__)


class SectorDataset:
    """Contains metadata and data for a sector."""

    COMMON_FILENAME = "common.json"
    DATA_FILENAME = "data.parquet"
    METADATA_FILENAME = "load_data_table.parquet"
    RECORDS_FILENAME = "load_records.parquet"

    def __init__(self, spark, common, records, metadata, data, store):
        self._spark = spark
        self._common = common
        self._records = records
        self._metadata = metadata
        self._data = data
        self._store = store

    @classmethod
    def load(cls, path, store):
        """Load a sector dataset from a path.

        Parameters
        ----------
        path : str

        Returns
        -------
        SectorDataset

        """
        spark = store.spark
        common = load_data(os.path.join(path, cls.COMMON_FILENAME))
        metadata = spark.read.parquet(os.path.join(path, cls.METADATA_FILENAME))
        records = spark.read.parquet(os.path.join(path, cls.RECORDS_FILENAME))
        data = spark.read.parquet(os.path.join(path, cls.DATA_FILENAME))
        logger.debug("Loaded SectorDataset from %s", path)
        return cls(spark, common, records, metadata, data, store)

    def compute_sum_by_sector_id(self, time_range=None):
        """Compute the sum for each sector.

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        """
        assert time_range is None, "subsetting time range is not currently supported"
        sums = self._data.groupby("id").sum().drop("sum(id)").withColumnRenamed("id", "data_id")
        expr = [F.col(x) * F.col("scale_factor") for x in sums.columns if x != "id"]
        expr += ["id", "sector_id"]
        return self._records.join(sums, self._records.data == sums.data_id).select(expr)

    def aggregate_sector_sums_by_dimension(self, from_dimension, to_dimension):
        """Aggregate the sums for each sector for a dimension.

        Parameters
        ----------
        from_dimension : class
        to_dimension : class

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        Examples
        --------
        >>> df = dataset.aggregate_sum_by_dimension(County, State)

        """
        from_df = self._store.get_dataframe(from_dimension)
        data_by_sector_id = self.compute_sum_by_sector_id()
        key = self._store.get_dimension_mapping_key(from_dimension, to_dimension)
        df = data_by_sector_id.join(from_df.select("id", key), "id")
        return df.groupby("sector_id", key).sum()
