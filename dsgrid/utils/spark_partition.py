import logging
import pandas as pd
import math
from dsgrid.utils.timing import timed_info

logger = logging.getLogger(__name__)


class Spark_Partition:
    def __init__(self):
        super().__init__()

    def get_data_size(self, df, bytes_per_cell=64):
        """ approximate dataset size """
        n_rows = df.count()
        n_cols = len(df.columns)
        data_MB = n_rows * n_cols * bytes_per_cell / 1e6  # MB
        return n_rows, n_cols, data_MB

    @timed_info
    def get_optimal_number_of_files(self, df, MB_per_file=128):
        """ calculate *optimal* number of files """
        _, _, data_MB = self.get_data_size(df)
        n_files = math.ceil(data_MB / MB_per_file)

        logger.info(
            f"load_data_lookup is approximately {data_MB:.02f} MB in size, ideal to split into {n_files} file(s) at {MB_per_file} MB each."
        )
        return n_files

    @timed_info
    def file_size_if_partition_by(self, df, key):
        """ calculate sharded file size based on paritionBy key """
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

        output = pd.DataFrame(
            {key: [n_partitions, avg_MB, largest_MB, smallest_MB]},
            index=["n_partitions", "avg_partition_MB", "max_partition_MB", "min_partition_MB"],
        )
        return output
