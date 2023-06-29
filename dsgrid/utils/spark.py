"""Spark helper functions"""

import enum
import itertools
import logging
import math
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import AnyStr, Union

import pandas as pd
import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, BooleanType
from pyspark import SparkConf

from dsgrid.exceptions import DSGInvalidField, DSGInvalidParameter, DSGInvalidFile
from dsgrid.loggers import disable_console_logging
from dsgrid.utils.files import load_data
from dsgrid.utils.timing import Timer, track_timing, timer_stats_collector


logger = logging.getLogger(__name__)

# Consider using our own database. Would need to manage creation with
# spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
# Doing so has caused conflicts in tests with the Derby db.
DSGRID_DB_NAME = "default"

PYTHON_TO_SPARK_TYPES = {
    int: IntegerType,
    float: DoubleType,
    str: StringType,
    bool: BooleanType,
}


def init_spark(name="dsgrid", check_env=True, spark_conf=None):
    """Initialize a SparkSession.

    Parameters
    ----------
    name : str
    check_env : bool
        If True, which is default, check for the SPARK_CLUSTER environment variable and attach to
        it. Otherwise, create a local-mode cluster or attach to the SparkSession that was created
        by pyspark/spark-submit prior to starting the current process.
    spark_conf : dict | None, defaults to None
        If set, Spark configuration parameters

    """
    cluster = os.environ.get("SPARK_CLUSTER")
    conf = SparkConf().setAppName(name)
    if spark_conf is not None:
        for key, val in spark_conf.items():
            conf.set(key, val)
    if check_env and cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf.setMaster(cluster)
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    with disable_console_logging():
        log_spark_conf(spark)
        logger.info("Custom configuration settings: %s", spark_conf)

    return spark


def restart_spark(*args, force=False, **kwargs):
    """Restart a SparkSession with new config parameters. Refer to init_spark for parameters.

    Parameters
    ----------
    force : bool
        If True, restart the session even if the config parameters haven't changed.
        You might want to do this in order to clear cached tables or start Spark fresh.

    Returns
    -------
    pyspark.sql.SparkSession

    """
    spark = SparkSession.getActiveSession()
    needs_restart = force
    if not force:
        conf = kwargs.get("spark_conf", {})
        for key, val in conf.items():
            current = spark.conf.get(key, None)
            if current is not None and current != val:
                logger.info("SparkSession needs restart because of %s = %s", key, val)
                needs_restart = True
                break

    if needs_restart:
        spark.stop()
        logger.info("Stopped the SparkSession so that it can be restarted with a new config.")
        spark = init_spark(*args, **kwargs)
    else:
        logger.info("No restart of Spark is needed.")

    return spark


def log_spark_conf(spark: SparkSession):
    """Log the Spark configuration details."""
    conf = spark.sparkContext.getConf().getAll()
    conf.sort(key=lambda x: x[0])
    logger.info("Spark conf: %s", "\n".join([f"{x} = {y}" for x, y in conf]))


@track_timing(timer_stats_collector)
def create_dataframe(records, table_name=None, require_unique=None):
    """Create a spark DataFrame from a list of records.

    Parameters
    ----------
    records : list
        list of spark.sql.Row
    table_name : str | None
        If set, cache the DataFrame in memory with this name. Must be unique.
    require_unique : list
        list of column names (str) to check for uniqueness

    Returns
    -------
    spark.sql.DataFrame

    """
    df = get_spark_session().createDataFrame(records)
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


def create_dataframe_from_pandas(df):
    """Create a spark DataFrame from a pandas DataFrame."""
    return get_spark_session().createDataFrame(df)


def try_read_dataframe(filename: Path, delete_if_invalid=True, **kwargs):
    """Try to read the dataframe.

    Parameters
    ----------
    filename : Path
    delete_if_invalid : bool
        Delete the file if it cannot be read, defaults to true.
    kwargs
        Forwarded to read_dataframe.

    Returns
    -------
    pyspark.sql.DataFrame | None
        Returns None if the file does not exist or is invalid.

    """
    if not filename.exists():
        return None

    try:
        return read_dataframe(filename, **kwargs)
    except DSGInvalidFile:
        if delete_if_invalid:
            if filename.is_dir():
                shutil.rmtree(filename)
            else:
                filename.unlink()
        return None


@track_timing(timer_stats_collector)
def read_dataframe(filename, table_name=None, require_unique=None, read_with_spark=True):
    """Create a spark DataFrame from a file.

    Supported formats when read_with_spark=True: .csv, .json, .parquet
    Supported formats when read_with_spark=False: .csv, .json

    When reading CSV files on AWS read_with_spark should be set to False because the
    files would need to be present on local storage for all workers. The master node
    will sync the config files from S3, read them with standard filesystem system calls,
    and then convert the data to Spark dataframes. This could change if we ever decide
    to read CSV files with Spark directly from S3.

    Parameters
    ----------
    filename : str | Path
        path to file
    table_name : str | None
        If set, cache the DataFrame in memory. Must be unique.
    require_unique : list
        list of column names (str) to check for uniqueness
    read_with_spark : bool
        If True, read the file with pyspark.read. Otherwise, read the file into
        a list of dicts, convert to pyspark Rows, and then to a DataFrame.

    Returns
    -------
    spark.sql.DataFrame

    Raises
    ------
    ValueError
        Raised if a require_unique column has duplicate values.
    DSGInvalidFile
        Raised if the file cannot be read. This can happen if a Parquet write operation fails.

    """
    func = _read_with_spark if read_with_spark else _read_natively
    df = func(str(filename))
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


def _read_with_spark(filename):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"{filename} does not exist")
    spark = get_spark_session()
    suffix = Path(filename).suffix
    if suffix == ".csv":
        df = spark.read.csv(filename, inferSchema=True, header=True)
    elif suffix == ".parquet":
        try:
            df = spark.read.parquet(filename)
        except pyspark.sql.utils.AnalysisException as exc:
            if "Unable to infer schema for Parquet. It must be specified manually." in str(exc):
                logger.exception("Failed to read Parquet file=%s. File may be invalid", filename)
                raise DSGInvalidFile(f"Cannot read {filename=}")
            raise
    elif suffix == ".json":
        df = spark.read.json(filename, mode="FAILFAST")
    else:
        assert False, f"Unsupported file extension: {filename}"
    return df


def _read_natively(filename):
    suffix = Path(filename).suffix
    if suffix == ".csv":
        # Reading the file is faster with pandas. Converting a list of Row to spark df
        # is a tiny bit faster. Pandas is likely scales better with bigger files.
        # Keep the code in case we ever want to revert.
        # with open(filename, encoding="utf-8-sig") as f_in:
        #     rows = [Row(**x) for x in csv.DictReader(f_in)]
        obj = pd.read_csv(filename)
    elif suffix == ".json":
        obj = load_data(filename)
    else:
        assert False, f"Unsupported file extension: {filename}"
    return get_spark_session().createDataFrame(obj)


def _post_process_dataframe(df, table_name=None, require_unique=None):
    if table_name is not None:
        df.createOrReplaceTempView(table_name)
        df.cache()

    if require_unique is not None:
        with Timer(timer_stats_collector, "check_unique"):
            for column in require_unique:
                unique = df.select(column).distinct()
                if unique.count() != df.count():
                    raise DSGInvalidField(f"DataFrame has duplicate entries for {column}")


def get_unique_values(df, columns: Union[AnyStr, list]):
    """Return the unique values of a dataframe in one column or a list of columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    column : str or list of str

    Returns
    -------
    set

    """
    dfc = df.select(columns).distinct().collect()
    if isinstance(columns, list):
        values = {tuple(getattr(row, col) for col in columns) for row in dfc}
    else:
        values = {getattr(x, columns) for x in dfc}

    return values


@track_timing(timer_stats_collector)
def models_to_dataframe(models, table_name=None):
    """Converts a list of Pydantic models to a Spark DataFrame.

    Parameters
    ----------
    models : list
    table_name : str | None
        If set, a unique ID to use as the cached table name. Return from cache if already stored.

    Returns
    -------
    pyspark.sql.DataFrame

    """
    spark = get_spark_session()
    if (
        table_name is not None
        and spark.catalog.tableExists(table_name)
        and spark.catalog.isCached(table_name)
    ):
        return spark.table(table_name)

    assert models
    cls = type(models[0])
    rows = []
    schema = StructType()
    for i, model in enumerate(models):
        dct = {}
        for f in cls.__fields__:
            val = getattr(model, f)
            if isinstance(val, enum.Enum):
                val = val.value
            if i == 0:
                if val is None:
                    python_type = cls.__fields__[f].type_
                    if issubclass(python_type, enum.Enum):
                        python_type = type(next(iter(python_type)).value)
                else:
                    python_type = type(val)
                spark_type = PYTHON_TO_SPARK_TYPES[python_type]()
                schema.add(f, spark_type, nullable=True)
            dct[f] = val
        rows.append(Row(**dct))

    df = spark.createDataFrame(rows, schema=schema)

    if table_name is not None:
        df.createOrReplaceTempView(table_name)
        df.cache()

    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_dimension_ids(records, *dimension_types, cache=True):
    """Return a DataFrame created from the IDs of dimension_types.

    Parameters
    ----------
    records : sequence
        Iterable of lists of record IDs
    dimension_types : tuple
    cache : If True, cache the DataFrame.

    Returns
    -------
    pyspark.sql.DataFrame

    """
    schema = StructType()
    for dimension_type in dimension_types:
        schema.add(dimension_type.value, StringType(), nullable=False)
    df = get_spark_session().createDataFrame(records, schema=schema)
    if cache:
        df.cache()
    return df


@track_timing(timer_stats_collector)
def check_for_nulls(df, exclude_columns=None):
    """Check if a DataFrame has null values.

    Parameters
    ----------
    df : spark.sql.DataFrame
    exclude_columns : None or Set

    Raises
    ------
    DSGInvalidField
        Raised if null exists in any column.

    """
    if exclude_columns is None:
        exclude_columns = set()
    cols_to_check = set(df.columns).difference(exclude_columns)
    cols_str = ", ".join(cols_to_check)
    filter_str = " OR ".join((f"{x} is NULL" for x in cols_to_check))
    df.createOrReplaceTempView("tmp_table")

    try:
        # Avoid iterating with many checks unless we know there is at least one failure.
        nulls = sql(f"SELECT {cols_str} FROM tmp_table WHERE {filter_str}")
        if not nulls.rdd.isEmpty():
            cols_with_null = set()
            for col in cols_to_check:
                if not nulls.select(col).filter(f"{col} is NULL").rdd.isEmpty():
                    cols_with_null.add(col)
            assert cols_with_null, "Did not find any columns with NULL values"

            raise DSGInvalidField(
                f"DataFrame contains NULL value(s) for column(s): {cols_with_null}"
            )
    finally:
        sql("DROP VIEW tmp_table")


@track_timing(timer_stats_collector)
def overwrite_dataframe_file(filename, df):
    """Perform an in-place overwrite of a Spark DataFrame, accounting for different file types
    and symlinks.

    Do not attempt to access the original dataframe unless it was fully cached.

    Parameters
    ----------
    filename : str
    df : pyspark.sql.DataFrame

    Returns
    -------
    pyspark.sql.DataFrame

    """
    spark = get_spark_session()
    suffix = Path(filename).suffix
    tmp = str(filename) + ".tmp"
    if suffix == ".parquet":
        df.write.parquet(tmp)
        read_method = spark.read.parquet
        kwargs = {}
    elif suffix == ".csv":
        df.write.csv(str(tmp), header=True)
        read_method = spark.read.csv
        kwargs = {"header": True, "inferSchema": True}
    elif suffix == ".json":
        df.write.json(str(tmp))
        read_method = spark.read.json
        kwargs = {}
    if os.path.isfile(filename) or os.path.islink(filename):
        os.unlink(filename)
    else:
        shutil.rmtree(filename)
    os.rename(tmp, str(filename))
    return read_method(str(filename), **kwargs)


@track_timing(timer_stats_collector)
def write_dataframe_and_auto_partition(
    df, filename, partition_size_mb=128, columns=None, rtol_pct=50
):
    """Write a dataframe to a file and then automatically coalesce or repartition it if needed.
    If the file already exists, it will be overwritten.

    Only Parquet files are supported.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    filename : Path
    partition_size_mb : int
        Target size in MB for each partition
    columns : None, list
        If not None and repartitioning is needed, partition on these columns.
    rtol_pct : int
        Don't repartition or coalesce if the relative difference between desired and actual
        partitions is within this tolerance as a percentage.

    Returns
    -------
    pyspark.sql.DataFrame

    Raises
    ------
    DSGInvalidParameter
        Raised if a non-Parquet file is passed

    """
    if filename.suffix != ".parquet":
        raise DSGInvalidParameter(f"Only parquet files are supported: {filename}")
    spark = get_spark_session()
    if filename.exists():
        df = overwrite_dataframe_file(filename, df)
    else:
        df.write.parquet(str(filename))
        df = spark.read.parquet(str(filename))
    partition_size_bytes = partition_size_mb * 1024 * 1024
    total_size = sum((x.stat().st_size for x in filename.glob("*.parquet")))
    desired = math.ceil(total_size / partition_size_bytes)
    actual = df.rdd.getNumPartitions()
    if abs(actual - desired) / desired * 100 < rtol_pct:
        logger.debug("No change in number of partitions is needed for %s.", filename)
    elif actual > desired:
        df = df.coalesce(desired)
        df = overwrite_dataframe_file(filename, df)
        logger.debug("Coalesced %s to partition count %s", filename, desired)
    else:
        if columns is None:
            df = df.repartition(desired)
        else:
            df = df.repartition(desired, *columns)
        df = overwrite_dataframe_file(filename, df)
        logger.debug("Repartitioned %s to partition count", filename, desired)

    logger.info("Wrote dataframe to %s", filename)
    return df


@track_timing(timer_stats_collector)
def write_dataframe(df, filename):
    """Write a Spark DataFrame, accounting for different file types.

    Parameters
    ----------
    filename : str
    df : pyspark.sql.DataFrame

    Returns
    -------
    pyspark.sql.DataFrame

    """
    suffix = Path(filename).suffix
    name = str(filename)
    if suffix == ".parquet":
        df.write.parquet(name)
    elif suffix == ".csv":
        df.write.csv(name, header=True)
    elif suffix == ".json":
        df.write.json(name)


def sql(query):
    """Run a SQL query with Spark.

    Parameters
    ----------
    query : str

    Returns
    -------
    pyspark.sql.DataFrame

    """
    logger.debug("Run SQL query [%s]", query)
    return get_spark_session().sql(query)


def sql_from_sqlalchemy(query):
    """Run a SQL query with Spark where the query was generated by sqlalchemy.

    Parameters
    ----------
    query : sqlalchemy.orm.query.Query

    Returns
    -------
    pyspark.sql.DataFrame

    """
    logger.debug("sqlchemy query = %s", query)
    return sql(str(query).replace('"', ""))


def cross_join_dfs(dfs):
    """Perform a cross join of all dataframes in dfs.

    Parameters
    ----------
    dfs : list[pyspark.sql.DataFrame]

    Returns
    pyspark.sql.DataFrame

    """
    if len(dfs) == 1:
        return dfs[0]

    df = dfs[0]
    for other in dfs[1:]:
        df = df.crossJoin(other)
    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_product(data: dict):
    """Create a dataframe by taking a product of values/columns in a dict. This is significantly
    faster than creating a dataframe from each column and cross-joining them with Spark.

    Parameters
    ----------
    data : dict
        Columns on which to perform a cross product.
        {"sector": [com], "subsector": ["SmallOffice", "LargeOffice"]}

    Returns
    -------
    pyspark.sql.DataFrame

    """
    spark = get_spark_session()
    columns = list(data.keys())
    df = spark.createDataFrame(
        spark.sparkContext.parallelize(itertools.product(*(data.values()))), columns
    )
    return df


def get_spark_session() -> SparkSession:
    """Return the active SparkSession or create a new one is none is active."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.warning("Could not find a SparkSession. Create a new one.")
        spark = SparkSession.builder.getOrCreate()
        log_spark_conf(spark)
    return spark


@contextmanager
def custom_spark_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.

    """
    spark = get_spark_session()
    orig_settings = {}

    try:
        for key, val in conf.items():
            orig_settings[key] = spark.conf.get(key)
            spark.conf.set(key, val)
            logger.info("Set %s=%s temporarily", key, val)
        yield
    finally:
        for key, val in orig_settings.items():
            spark.conf.set(key, val)


@contextmanager
def restart_spark_with_custom_conf(conf: dict, force=False):
    """Restart the SparkSession with a custom configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.
    force : bool
        If True, restart the session even if the config parameters haven't changed.
        You might want to do this in order to clear cached tables or start Spark fresh.

    """
    spark = get_spark_session()
    app_name = spark.conf.get("spark.app.name")
    orig_settings = {}

    try:
        for name in conf:
            current = spark.conf.get(name, None)
            if current is not None:
                orig_settings[name] = current
        restart_spark(name=app_name, spark_conf=conf, force=force)
        yield
    finally:
        restart_spark(name=app_name, spark_conf=orig_settings, force=force)


def load_stored_table(table_name):
    """Return a table stored in the Spark warehouse.

    Parameters
    ----------
    table_name : str

    Returns
    -------
    pyspark.sql.DataFrame

    """
    spark = get_spark_session()
    return spark.table(table_name)


def try_load_stored_table(table_name, database=DSGRID_DB_NAME):
    """Return a table if it is stored in the Spark warehouse.

    Parameters
    ----------
    table_name : str
    database : str, optional

    Returns
    -------
    pyspark.sql.DataFrame | None

    """
    spark = get_spark_session()
    if spark.catalog.tableExists(table_name, dbName=database):
        return spark.table(table_name)
    return None


def union(dfs) -> pyspark.sql.DataFrame:
    """Return a union of the dataframes, ensuring that the columns match."""
    df = dfs[0]
    if len(dfs) > 1:
        for dft in dfs[1:]:
            if df.columns != dft.columns:
                raise Exception(f"columns don't match: {df.columns=} {dft.columns=}")
            df = df.union(dft)
    return df


def is_table_stored(table_name, database=DSGRID_DB_NAME):
    spark = get_spark_session()
    return spark.catalog.tableExists(table_name, dbName=database)


def save_table(table, table_name, overwrite=True, database=DSGRID_DB_NAME):
    if overwrite:
        table.write.mode("overwrite").saveAsTable(table_name)
    else:
        table.write.saveAsTable(table_name)


def list_tables(database=DSGRID_DB_NAME):
    spark = get_spark_session()
    return [x.name for x in spark.catalog.listTables(dbName=database)]


def drop_table(table_name, database=DSGRID_DB_NAME):
    spark = get_spark_session()
    if is_table_stored(table_name, database=database):
        spark.sql(f"DROP TABLE {table_name}")
        logger.info("Dropped table %s", table_name)
