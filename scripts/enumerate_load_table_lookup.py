import errno
import sys
import os
import shutil
import logging

from dsgrid.loggers import setup_logging
from dsgrid.spark.functions import cross_join, read_parquet
from dsgrid.spark.types import use_duckdb
from dsgrid.utils.timing import timed_info
from dsgrid.utils.spark_partition import SparkPartition

logger = logging.getLogger(__name__)

####################################################################
# MODEL FUNCS


class EnumerateTable:
    def __init__(self):
        self.partition = SparkPartition()

    def copydirectory(self, src, dst, override=False):
        """
        Copy directory from src to dst, with option to override.
        override=True will clear dst before copying
        """
        if not override and os.path.exists(dst):
            logger.info(f'"{dst}" already exists, passing...')

        else:
            if os.path.exists(dst):
                shutil.rmtree(dst)
            try:
                shutil.copytree(src, dst)
            except OSError as exc:
                if exc.errno in (errno.ENOTDIR, errno.EINVAL):
                    shutil.copy(src, dst)
                else:
                    raise
            logger.info(f'"{src}"\n copied to: "{dst}"')

    @timed_info
    def relocate_original_file(self, lookup_file):
        """copy load_data_lookup to new location"""
        # define path for relocation
        file_rename = list(os.path.splitext(os.path.basename(lookup_file)))
        file_rename[0] = file_rename[0] + "_orig"
        file_rename = "".join(file_rename)
        relocated_file = os.path.join(os.path.dirname(lookup_file), file_rename)

        # execute
        self.copydirectory(lookup_file, relocated_file, override=False)
        return relocated_file

    @timed_info
    def enumerate_lookup_by_keys(self, df_lookup, keys):
        if len(keys) > 1:
            df_lookup_full = df_lookup.select(keys[0]).distinct()

            for key in keys[1:]:
                df_lookup_full = cross_join(df_lookup_full, df_lookup.select(key).distinct())

            df_lookup_full = df_lookup_full.join(df_lookup, keys, "left").sort(["id"] + keys)
        else:
            df_lookup_full = df_lookup
        return df_lookup_full

    @timed_info
    def enumeration_report(self, df_lookup_full, df_lookup):
        N_df_lookup = df_lookup.count()
        N_df_lookup_full = df_lookup_full.count()
        N_df_lookup_null = N_df_lookup_full - N_df_lookup
        logger.info(f"  # rows in df_lookup: {N_df_lookup}")
        logger.info(f"  # rows in df_lookup (fully enumerated): {N_df_lookup_full}")
        logger.info(
            f"  # of rows without data: {N_df_lookup_null} ({(N_df_lookup_null / N_df_lookup_full * 100):.02f}%)"
        )

    @timed_info
    def assertion_checks(self, df_lookup_full, df_lookup, keys):
        # 1) set of (data) id is the same before and after enumeration
        df_lookup_ids = df_lookup.select("id").distinct().collect()[0]
        df_lookup_full_ids = df_lookup_full.select("id").distinct().collect()[0]

        assert set(df_lookup_ids) == set(df_lookup_full_ids) - set([None])

        # 2) make sure N_df_lookup_full is the product of the nunique of each key
        N_df_lookup_full = df_lookup_full.count()
        N_enumerations = 1
        for key in keys:
            N_enumerations *= df_lookup.select(key).distinct().count()

        assert N_enumerations == N_df_lookup_full

    @timed_info
    def save_file(self, df, filepath, n_files=None, repartition_by=None):
        """
        n_files: number of target sharded files
        repartition_by: col to repartition by

        Note:
            - Not available for load_data_lookup: df.write.partitionBy().bucketBy()
            - df.coalesce(n): combine without shuffling, will not go larger than current_n_parts
            - df.repartition(n): shuffle and try to evenly distribute, if n > df.nunique rows, some partitions will be empty
            - df.repartition(col): shuffle and create partitions by col.nunique + 1 empty/very small partition
            - df.repartition(n, col): shufffle, number partitions = min(n, col.nunique)
        """
        if use_duckdb():
            logger.warning("save_file is not optimized for DuckDB")
            df.write.parquet(filepath)
            return

        current_n_parts = df.rdd.getNumPartitions()

        if n_files is not None and repartition_by is not None:
            df_out = df.repartition(n_files, repartition_by)
        elif n_files is None and repartition_by is not None:
            df_out = df.repartition(repartition_by)
        elif n_files is not None and repartition_by is None:
            df_out = df.repartition(n_files)
        else:
            df_out = df

        # for reporting out:
        if repartition_by is not None:
            n_out_files = df.select(repartition_by).distinct().count() + 1
            ext = f", repartitioned by {repartition_by}"
        else:
            n_out_files = current_n_parts
            ext = ""
        n_out_files = min(n_out_files, df_out.rdd.getNumPartitions())
        logger.info(f"Saving {current_n_parts} partitions --> {n_out_files} file(s){ext}...")

        df_out.write.mode("overwrite").option("path", filepath).saveAsTable(
            "load_data_lookup", format="parquet"
        )

    @timed_info
    def run(self, relocated_file, lookup_file):
        """read from relocated_file, replace lookup_file with new output"""
        # 1. load data
        df_lookup = read_parquet(relocated_file)

        # 2. get keys to enumerte on
        keys_to_exclude = ["scale_factor", "id"]
        keys = [x for x in df_lookup.columns if x not in keys_to_exclude]
        logger.info(f"keys in load_data_lookup: {keys}")

        # 3. enumerate keys
        df_lookup_full = self.enumerate_lookup_by_keys(df_lookup, keys)

        # 4. data quality check
        self.enumeration_report(df_lookup_full, df_lookup)
        self.assertion_checks(df_lookup_full, df_lookup, keys)

        # 5. save
        # save to file by controling n_files
        n_files = self.partition.get_optimal_number_of_files(df_lookup_full)
        self.save_file(df_lookup_full, lookup_file, n_files, repartition_by=None)


####################################################################
# MAIN FUNCS


def main(lookup_file):
    global logger
    """ copy lookup_file to new loc, replace lookup_file with enumerated file """
    base_dir = os.path.dirname(lookup_file)

    log_file = os.path.join(base_dir, "enumerate_load_table_output.log")
    logger = setup_logging(__name__, log_file)
    logger.info("CLI args: %s", " ".join(sys.argv))

    enumeration = EnumerateTable()
    relocated_file = enumeration.relocate_original_file(lookup_file)
    logger.info("\n")
    enumeration.run(relocated_file, lookup_file)


if __name__ == "__main__":
    """
    Usaage:
    python enumerate_load_table_lookup.py path_to_lookup_file
    """
    __name__ = "DSG"  # rename for logger

    if len(sys.argv) != 2:
        logger.info(f"Usage: {sys.argv[0]} path_to_lookup_file")
        sys.exit(1)

    main(sys.argv[1])
