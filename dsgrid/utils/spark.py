"""Spark helper functions"""

import logging
import os
from pathlib import Path
from typing import AnyStr, List, Union

import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark import SparkConf

from dsgrid.exceptions import DSGInvalidField
from dsgrid.utils.files import load_data
from dsgrid.utils.timing import Timer, track_timing, timer_stats_collector


logger = logging.getLogger(__name__)


def init_spark(name="dsgrid"):
    """Initialize a SparkSession."""
    cluster = os.environ.get("SPARK_CLUSTER")
    if cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf = SparkConf().setAppName(name).setMaster(cluster)
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    else:
        logger.info("Create SparkSession %s in local-mode cluster", name)
        spark = SparkSession.builder.master("local").appName(name).getOrCreate()

    logger.info("Spark conf: %s", str(spark.sparkContext.getConf().getAll()))
    return spark


@track_timing(timer_stats_collector)
def create_dataframe(records, cache=False, require_unique=None):
    """Create a spark DataFrame from a list of records.

    Parameters
    ----------
    records : list
        list of spark.sql.Row
    cache : bool
        If True, cache the DataFrame in memory.
    require_unique : list
        list of column names (str) to check for uniqueness

    Returns
    -------
    spark.sql.DataFrame

    """
    df = SparkSession.getActiveSession().createDataFrame(records)
    _post_process_dataframe(df, cache=cache, require_unique=require_unique)
    return df


@track_timing(timer_stats_collector)
def read_dataframe(filename, cache=False, require_unique=None, read_with_spark=True):
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
    cache : bool
        If True, cache the DataFrame in memory.
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

    """
    func = _read_with_spark if read_with_spark else _read_natively
    df = func(str(filename))
    _post_process_dataframe(df, cache=cache, require_unique=require_unique)
    return df


def _read_with_spark(filename):
    spark = SparkSession.getActiveSession()
    if filename.endswith(".csv"):
        df = spark.read.csv(filename, inferSchema=True, header=True)
    elif Path(filename).suffix == ".parquet":
        df = spark.read.parquet(filename)
    elif Path(filename).suffix == ".json":
        df = spark.read.json(filename, mode="FAILFAST")
    else:
        assert False, f"Unsupported file extension: {filename}"
    return df


def _read_natively(filename):
    if Path(filename).suffix == ".csv":
        # Reading the file is faster with pandas. Converting a list of Row to spark df
        # is a tiny bit faster. Pandas is likely scales better with bigger files.
        # Keep the code in case we ever want to revert.
        # with open(filename, encoding="utf-8-sig") as f_in:
        #     rows = [Row(**x) for x in csv.DictReader(f_in)]
        obj = pd.read_csv(filename)
    elif Path(filename).suffix == ".json":
        obj = load_data(filename)
    else:
        assert False, f"Unsupported file extension: {filename}"
    return SparkSession.getActiveSession().createDataFrame(obj)


def _post_process_dataframe(df, cache=False, require_unique=None):
    # TODO This is causing Spark warning messages. Disable until we know why.
    # if cache:
    #     df = df.cache()

    if require_unique is not None:
        with Timer(timer_stats_collector, "check_unique"):
            for column in require_unique:
                unique = df.select(column).distinct()
                if unique.count() != df.count():
                    raise DSGInvalidField(f"DataFrame has duplicate entries for {column}")


def get_unique_values(df, columns: Union[AnyStr, List]):
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
def models_to_dataframe(models, cache=False):
    """Converts a list of Pydantic models to a Spark DataFrame.

    Parameters
    ----------
    models : list
    cache : If True, cache the DataFrame.

    Returns
    -------
    pyspark.sql.DataFrame

    """
    assert models
    cls = type(models[0])
    rows = []
    for model in models:
        row = Row(**{f: getattr(model, f) for f in cls.__fields__})
        rows.append(row)

    df = SparkSession.getActiveSession().createDataFrame(rows)
    if cache:
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
    df = SparkSession.getActiveSession().createDataFrame(records, schema=schema)
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
    return SparkSession.getActiveSession().sql(query)


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
