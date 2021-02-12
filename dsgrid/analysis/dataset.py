"""Sector dataset functionality"""

import logging
import os

import pyspark
from pyspark.sql import functions as F

from dsgrid.utils.files import load_data
from dsgrid.utils.spark import init_spark


logger = logging.getLogger(__name__)


class Dataset:
    """Contains metadata and data for a sector."""

    DATA_FILENAME = "data.parquet"
    METADATA_FILENAME = "load_data_table.parquet"
    RECORDS_FILENAME = "load_records.parquet"

    def __init__(self, spark, load_data_lookup, load_metadata, data, store):
        self._spark = spark  # SparkSession
        self._load_data_lookup = load_data_lookup  # DataFrame of dimension elements
        self._load_metadata = load_metadata  # DataFrame containing metadata
        self._load_data = data  # DataFrame containing load data
        self._store = store  # DimensionStore: contains dimension mappings

    @classmethod
    def load(cls, path, store):
        """Load a sector dataset from a path.

        Parameters
        ----------
        path : str

        Returns
        -------
        Dataset

        """
        spark = store.spark
        load_metadata = spark.read.parquet(os.path.join(path, cls.METADATA_FILENAME))
        load_data_lookup = spark.read.parquet(os.path.join(path, cls.RECORDS_FILENAME))
        data = spark.read.parquet(os.path.join(path, cls.DATA_FILENAME))
        logger.debug("Loaded Dataset from %s", path)
        return cls(spark, load_data_lookup, load_metadata, data, store)

    @property
    def spark(self):
        """Return the SparkSession instance."""
        return self._spark

    def compute_sum_by_sector_id(self):
        """Compute the sum for each sector.

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        """
        sums = self._load_data.groupby("id") \
            .sum() \
            .drop("sum(id)") \
            .withColumnRenamed("id", "data_id")
        expr = [F.col(x) * F.col("scale_factor") for x in sums.columns]
        expr += ["id", "sector_id"]
        return self._load_data_lookup.join(sums, "data_id").select(expr)

    def compute_sum_by_sector_id_sql(self):
        """Compute the sum for each sector.

        Returns
        -------
        pyspark.sql.dataframe.DataFrame

        """
        end_uses = (
            "fans", "pumps", "heating", "cooling", "interior_lights",
            "exterior_lights", "water_systems", "interior_equipment",
            "heat_rejection",
        )
        columns = ", ".join(f"sum({x}) as sum_{x}" for x in end_uses)
        cols_sum_end_uses = ", ".join(f"sum_{x}" for x in end_uses)
        sums = self._spark.sql(f"select id, {columns} from load_data group by id")
        sums.createOrReplaceTempView("sums")
        expr = [F.col(x) * F.col("scale_factor") for x in sums.columns if x != "id"]
        expr += ["id", "sector_id"]
        return self._spark.sql(f"""
            select sums.id, load_data_lookup.scale_factor, load_data_lookup.sector_id, {cols_sum_end_uses}
            from load_data_lookup
            join sums on sums.id=load_data_lookup.data_id""") \
            .select(expr)

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
        #self_store.get_scale_factor(from_dimension, to_dimension)
        from_df = self._store.get_dataframe(from_dimension)
        data_by_sector_id = self.compute_sum_by_sector_id()
        key = self._store.get_dimension_mapping_key(from_dimension, to_dimension)
        df = data_by_sector_id.join(from_df.select("id", key), "id")
        return df.groupby("sector_id", key).sum()

    def aggregate_sector_sums_by_dimension_sql(self, from_dimension, to_dimension):
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
        self.make_tables()
        #self_store.get_scale_factor(from_dimension, to_dimension)
        from_df = self._spark.sql(f"select * from {from_dimension.__name__}")
        from_df.createOrReplaceTempView("from_df")
        data_by_sector_id = self.compute_sum_by_sector_id_sql()
        data_by_sector_id.createOrReplaceTempView("data_by_sector_id")
        breakpoint()
        key = self._store.get_dimension_mapping_key(from_dimension, to_dimension)
        df = self._spark.sql("""
            select *
            from data_by_sector_id
            join from_df on data_by_sector_id.id=from_df.id""")
        df.createOrReplaceTempView("df")
        return self._spark.sql(f"select * from df group by {key}")
        #return df.groupby("sector_id", key).sum()

    def make_tables(self):
        self._load_data_lookup.createOrReplaceTempView("load_data_lookup")
        self._load_data.createOrReplaceTempView("load_data")
        for dim_type in self._store.iter_dimension_types():
            df = self._store.get_dataframe(dim_type)
            df.createOrReplaceTempView(dim_type.__name__)
        self._spark.sql("show tables").show()
