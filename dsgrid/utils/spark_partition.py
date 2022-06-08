import logging
import math

from dsgrid.utils.timing import timed_info

logger = logging.getLogger(__name__)


class SparkPartition:
    def __init__(self):
        return

    def get_data_size(self, df, bytes_per_cell=8):
        """approximate dataset size

        Parameters
        ----------
        df : DataFrame
        bytes_per_cell : [float, int]
            Estimated number of bytes per cell in a dataframe.
            * 4-bytes = 32-bit = Single-precision Float = pyspark.sql.types.FloatType,
            * 8-bytes = 64-bit = Double-precision float = pyspark.sql.types.DoubleType,

        Returns
        -------
        n_rows : int
            Number of rows in df
        n_cols : int
            Number of columns in df
        data_MB : float
            Estimated size of df in memory in MB

        """
        n_rows = df.count()
        n_cols = len(df.columns)
        data_MB = n_rows * n_cols * bytes_per_cell / 1e6  # MB
        return n_rows, n_cols, data_MB

    @timed_info
    def get_optimal_number_of_files(self, df, MB_per_cmp_file=128, cmp_ratio=0.18):
        """calculate *optimal* number of files
        Parameters
        ----------
        df : DataFrame
        MB_per_cmp_file : float
            Desired size of compressed file on disk in MB
        cmp_ratio : float
            Ratio of file size after and before compression

        Returns
        -------
        n_files : int
            Number of files
        """
        _, _, data_MB = self.get_data_size(df)
        MB_per_file = MB_per_cmp_file / cmp_ratio
        n_files = math.ceil(data_MB / MB_per_file)

        logger.info(
            f"Dataframe is approximately {data_MB:.02f} MB in size, "
            f"ideal to split into {n_files} file(s) at {MB_per_file:.1f} MB compressed on disk. "
            f"({MB_per_file:.1f} MB uncompressed in memory, {cmp_ratio} compression ratio)."
        )
        return n_files

    @timed_info
    def file_size_if_partition_by(self, df, key):
        """calculate sharded file size based on paritionBy key"""
        n_rows, n_cols, data_MB = self.get_data_size(df)
        n_partitions = df.select(key).distinct().count()
        avg_MB = round(data_MB / n_partitions, 2)

        n_rows_largest_part = df.groupBy(key).count().orderBy("count", ascending=False).first()[1]
        n_rows_smallest_part = df.groupBy(key).count().orderBy("count", ascending=True).first()[1]

        largest_MB = round(data_MB / n_rows * n_rows_largest_part, 2)
        smallest_MB = round(data_MB / n_rows * n_rows_smallest_part, 2)

        report = (
            f'Partitioning by "{key}" will yield: \n'
            + f"  - # of partitions: {n_partitions} \n"
            + f"  - avg partition size: {avg_MB} MB \n"
            + f"  - largest partition: {largest_MB} MB \n"
            + f"  - smallest partition: {smallest_MB} MB \n"
        )

        logger.info(report)

        output = {
            key: {
                "n_partitions": n_partitions,
                "avg_partition_MB": avg_MB,
                "max_partition_MB": largest_MB,
                "min_partition_MB": smallest_MB,
            }
        }

        return output
