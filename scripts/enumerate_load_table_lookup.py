import multiprocessing
import sys
import os
import shutil, errno
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.window as W
from pyspark.sql.utils import AnalysisException
import math
import logging
import functools
import time

logger = logging.getLogger(__name__)

####################################################################
### [[ SET UP SPARK SESSION ]] ###


def init_spark1(name, mem="5gb", num_cpus=None):
    """ Initialize a SparkSession. """
    if num_cpus is None:
        num_cpus = multiprocessing.cpu_count()

    cluster = os.environ.get("SPARK_CLUSTER")
    if cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf = SparkConf().setAppName(name).setMaster(cluster)
        sc = SparkContext(conf=conf)
        spark = (
            SparkSession.builder.config(conf=conf)
            .config("spark.executor.memory", mem)
            .config("spark.cores.max", str(num_cpus))
            .getOrCreate()
        )
    else:
        logger.info("Create SparkSession %s in new cluster", name)
        spark = (
            SparkSession.Builder()
            .master("local")
            .appName(name)
            .config("spark.executor.memory", mem)
            .config("spark.cores.max", str(num_cpus))
            .config("spark.logConf", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    return spark


def init_spark2(
    name,
    # mem="10gb",
    num_cpus=None,
):
    """Initialize a SparkSession."""
    cluster = "spark://r103u21.ib0.cm.hpc.nrel.gov:7077"
    conf = SparkConf().setAppName(name).setMaster(cluster)
    sc = SparkContext(conf=conf)
    spark = (
        SparkSession.builder.config(conf=conf)
        # .config("spark.executor.memory", mem)
        # .config("spark.cores.max", str(num_cpus))
        .getOrCreate()
    )
    return spark


def init_spark_main(name):
    """ choose which version of init_spark to use here """
    spark = init_spark1(name, mem="5gb", num_cpus=None)
    # spark = init_spark2(name, num_cpus=None)

    return spark


####################################################################


def timed_info(func):
    """Decorator to measure and logger.info a function's execution time."""

    @functools.wraps(func)
    def timed_(*args, **kwargs):
        return _timed(func, logger.info, *args, **kwargs)

    return timed_


def timed_debug(func):
    """Decorator to measure and logger.debug a function's execution time."""

    @functools.wraps(func)
    def timed_(*args, **kwargs):
        return _timed(func, logger.debug, *args, **kwargs)

    return timed_


def _timed(func, log_func, *args, **kwargs):
    start = time.time()
    result = func(*args, **kwargs)
    total = time.time() - start
    log_func("execution-time=%s func=%s", get_time_duration_string(total), func.__name__)
    return result


def get_time_duration_string(seconds):
    """Returns a string with the time converted to reasonable units."""
    if seconds >= 1:
        val = "{:.3f} s".format(seconds)
    elif seconds >= 0.001:
        val = "{:.3f} ms".format(seconds * 1000)
    elif seconds >= 0.000001:
        val = "{:.3f} us".format(seconds * 1000000)
    elif seconds == 0:
        val = "0 s"
    else:
        val = "{:.3f} ns".format(seconds * 1000000000)

    return val


def setup_logging(filename, file_level=logging.INFO, console_level=logging.INFO):
    global logger
    logger = logging.getLogger("DSG")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(filename)
    fh.setLevel(file_level)
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


def copydirectory(src, dst, override=False):
    """
    Copy directory from src to dst, with option to override.
    override=True will clear dst before copying
    """
    if (override == False) & (os.path.exists(dst)):
        logger.info(f'"{dst}" already exists, passing...')
        pass

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
def relocate_original_file(lookup_file):
    """ copy load_data_lookup to new location """
    # define path for relocation
    file_rename = list(os.path.splitext(os.path.basename(lookup_file)))
    file_rename[0] = file_rename[0] + "_orig"
    file_rename = "".join(file_rename)
    relocated_file = os.path.join(os.path.dirname(lookup_file), file_rename)

    # execute
    copydirectory(lookup_file, relocated_file, override=False)

    return relocated_file


@timed_info
def enumerate_lookup_by_keys(df_lookup, keys):
    if len(keys) > 1:
        df_lookup_full = df_lookup.select(keys[0]).distinct()

        for key in keys[1:]:
            df_lookup_full = df_lookup_full.crossJoin(df_lookup.select(key).distinct())

        df_lookup_full = df_lookup_full.join(df_lookup, keys, "left").sort(["id"] + keys)
    else:
        df_lookup_full = df_lookup

    return df_lookup_full


@timed_info
def enumeration_report(df_lookup_full, df_lookup):
    N_df_lookup = df_lookup.count()
    N_df_lookup_full = df_lookup_full.count()
    N_df_lookup_null = N_df_lookup_full - N_df_lookup
    logger.info(f"  # rows in df_lookup: {N_df_lookup}")
    logger.info(f"  # rows in df_lookup (fully enumerated): {N_df_lookup_full}")
    logger.info(
        f"  # of rows without data: {N_df_lookup_null} ({(N_df_lookup_null/N_df_lookup_full*100):.02f}%)"
    )


@timed_info
def assertion_checks(df_lookup_full, df_lookup, keys):

    # 1) number of (data) id list is the same before and after enumeration
    df_lookup_ids = df_lookup.select("id").distinct().toPandas().iloc[:, 0].values
    df_lookup_full_ids = df_lookup_full.select("id").distinct().toPandas().iloc[:, 0].values

    assert len(set(df_lookup_ids).difference(df_lookup_full_ids)) == 0

    # 2) make sure N_df_lookup_full is the product of the nunique of each key
    N_df_lookup_full = df_lookup_full.count()
    N_enumerations = 1
    for key in keys:
        N_enumerations *= df_lookup.select(key).distinct().count()

    assert N_enumerations == N_df_lookup_full


@timed_info
def get_number_of_buckets(df_lookup_full):
    """ approximate dataset size and calculate n_buckets """
    fraction = 0.001
    sample = df_lookup_full.sample(fraction=fraction).toPandas()
    data_MB = sample.memory_usage(index=False, deep=False).sum() / 1e6 / fraction  # MiB
    n_buckets = math.ceil(data_MB / 128)

    logger.info(
        f"load_data_lookup is approximately {data_MB:.02f} MB in size, expecting {n_buckets} bucket(s) at 128 MB each."
    )

    return n_buckets


@timed_info
def save_file(df, n_buckets, output_file):
    df.repartition(n_buckets, "id").write.bucketBy(n_buckets, "id").mode("overwrite").option(
        "path", output_file
    ).saveAsTable("load_data_lookup", format="parquet")


@timed_info
def run(relocated_file, lookup_file):
    """ read from relocated_file, replace lookup_file with new output """

    spark = init_spark_main("dsgrid-load")

    # 1. load data
    df_lookup = spark.read.parquet(relocated_file)

    # 2. get keys to enumerte on
    keys_to_exclude = ["scale_factor", "data_id", "id"]
    keys = [x for x in df_lookup.columns if x not in keys_to_exclude]
    logger.info(f"keys in load_data_lookup: {keys}")

    # 3. enumerate keys
    df_lookup_full = enumerate_lookup_by_keys(df_lookup, keys)

    # 4. data quality check
    enumeration_report(df_lookup_full, df_lookup)
    assertion_checks(df_lookup_full, df_lookup, keys)

    # 5. save
    n_buckets = get_number_of_buckets(df_lookup_full)
    save_file(df_lookup_full, n_buckets, lookup_file)


def main(lookup_file):
    """ copy lookup_file to new loc, replace lookup_file with enumerated file """
    base_dir = os.path.dirname(lookup_file)

    log_file = os.path.join(base_dir, "enumerate_load_table_output.log")
    setup_logging(log_file)
    logger.info("CLI args: %s", " ".join(sys.argv))

    relocated_file = relocate_original_file(lookup_file)
    logger.info("\n")
    run(relocated_file, lookup_file)


if __name__ == "__main__":
    """
    Usaage:
    python enumerate_load_table_lookup path_to_lookup_file
    """

    if len(sys.argv) != 2:
        logger.info(f"Usage: {sys.argv[0]} path_to_lookup_file ")
        sys.exit(1)

    main(sys.argv[1])
