"""Spark helper functions"""

import enum
import itertools
import logging
import math
import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from types import UnionType
from typing import Any, Generator, Iterable, Sequence, Type, Union, get_origin, get_args

import duckdb
import pandas as pd

from dsgrid.data_models import DSGBaseModel
from dsgrid.exceptions import (
    DSGInvalidField,
    DSGInvalidFile,
    DSGInvalidOperation,
    DSGInvalidParameter,
)
from dsgrid.utils.files import delete_if_exists, load_data
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.spark.functions import (
    cross_join,
    get_spark_session,
    get_duckdb_spark_session,
    get_current_time_zone,
    set_current_time_zone,
    init_spark,
    is_dataframe_empty,
    read_csv,
    read_json,
    read_parquet,
)
from dsgrid.spark.types import (
    AnalysisException,
    BooleanType,
    DataFrame,
    DoubleType,
    IntegerType,
    SparkSession,
    StringType,
    StructField,
    StructType,
    use_duckdb,
)
from dsgrid.utils.timing import Timer, track_timing, timer_stats_collector


logger = logging.getLogger(__name__)

# Consider using our own database. Would need to manage creation with
# spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
# Doing so has caused conflicts in tests with the Derby db.
DSGRID_DB_NAME = "default"

MAX_PARTITION_SIZE_MB = 128

PYTHON_TO_SPARK_TYPES = {
    int: IntegerType,
    float: DoubleType,
    str: StringType,
    bool: BooleanType,
}


def get_active_session(*args) -> SparkSession:
    """Return the active Spark Session."""
    return get_duckdb_spark_session() or init_spark(*args)


def restart_spark(*args, force=False, **kwargs) -> SparkSession:
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
    spark = get_duckdb_spark_session()
    if spark is not None:
        return spark

    spark = SparkSession.getActiveSession()
    needs_restart = force
    orig_time_zone = spark.conf.get("spark.sql.session.timeZone")
    conf = kwargs.get("spark_conf", {})
    new_time_zone = conf.get("spark.sql.session.timeZone", orig_time_zone)

    if not force:
        for key, val in conf.items():
            current = spark.conf.get(key, None)
            if isinstance(current, str):
                match current.lower():
                    case "true":
                        current = True
                    case "false":
                        current = False
            if current is not None and current != val:
                logger.info("SparkSession needs restart because of %s = %s", key, val)
                needs_restart = True
                break

    if needs_restart:
        spark.stop()
        logger.info("Stopped the SparkSession so that it can be restarted with a new config.")
        spark = init_spark(*args, **kwargs)
        if spark.conf.get("spark.sql.session.timeZone") != new_time_zone:
            # We set this value in query_submitter.py and that change will get lost
            # when the session is restarted.
            spark.conf.set("spark.sql.session.timeZone", new_time_zone)
    else:
        logger.info("No restart of Spark is needed.")

    return spark


@track_timing(timer_stats_collector)
def create_dataframe(records, table_name=None, require_unique=None) -> DataFrame:
    """Create a spark DataFrame from a list of records.

    Parameters
    ----------
    records : list
        list of spark.sql.Row
    table_name : str | None
        If set, cache the DataFrame in memory with this name. Must be unique.
    require_unique : list
        list of column names (str) to check for uniqueness
    """
    df = get_spark_session().createDataFrame(records)
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_ids(ids: Iterable[str], column: str) -> DataFrame:
    """Create a spark DataFrame from a list of dimension IDs."""
    schema = StructType([StructField(column, StringType())])
    return get_spark_session().createDataFrame([[x] for x in ids], schema)


def create_dataframe_from_pandas(df):
    """Create a spark DataFrame from a pandas DataFrame."""
    return get_spark_session().createDataFrame(df)


def create_dataframe_from_dicts(records: list[dict[str, Any]]) -> DataFrame:
    """Create a spark DataFrame from a list of dictionaries.

    The only purpose is to avoid pyright complaints about the type of the input to
    spark.createDataFrame. This can be removed if pyspark fixes the type annotations.
    """
    if not records:
        msg = "records cannot be empty in create_dataframe_from_dicts"
        raise DSGInvalidParameter(msg)

    data = [tuple(row.values()) for row in records]
    columns = list(records[0].keys())
    return get_spark_session().createDataFrame(data, columns)


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
def read_dataframe(
    filename: str | Path,
    table_name: str | None = None,
    require_unique: None | bool = None,
    read_with_spark: bool = True,
) -> DataFrame:
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
        msg = f"{filename} does not exist"
        raise FileNotFoundError(msg)
    suffix = Path(filename).suffix
    if suffix == ".csv":
        df = read_csv(filename)
    elif suffix == ".parquet":
        try:
            df = read_parquet(filename)
        except AnalysisException as exc:
            if "Unable to infer schema for Parquet. It must be specified manually." in str(exc):
                logger.exception("Failed to read Parquet file=%s. File may be invalid", filename)
                msg = f"Cannot read {filename=}"
                raise DSGInvalidFile(msg)
            raise
        except duckdb.duckdb.IOException:
            logger.exception("Failed to read Parquet file=%s. File may be invalid", filename)
            msg = f"Cannot read {filename=}"
            raise DSGInvalidFile(msg)

    elif suffix == ".json":
        df = read_json(filename)
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
        msg = f"Unsupported file extension: {filename}"
        raise NotImplementedError(msg)
    return get_spark_session().createDataFrame(obj)


def _post_process_dataframe(df, table_name=None, require_unique=None):
    if not use_duckdb() and table_name is not None:
        df.createOrReplaceTempView(table_name)
        df.cache()

    if require_unique is not None:
        with Timer(timer_stats_collector, "check_unique"):
            for column in require_unique:
                unique = df.select(column).distinct()
                if unique.count() != df.count():
                    msg = f"DataFrame has duplicate entries for {column}"
                    raise DSGInvalidField(msg)


def cross_join_dfs(dfs: list[DataFrame]) -> DataFrame:
    """Perform a cross join of all dataframes in dfs."""
    if len(dfs) == 1:
        return dfs[0]

    df = dfs[0]
    for other in dfs[1:]:
        df = cross_join(df, other)
    return df


def get_unique_values(df: DataFrame, columns: Sequence[str]) -> set[str]:
    """Return the unique values of a dataframe in one column or a list of columns."""
    dfc = df.select(columns).distinct().collect()
    if isinstance(columns, list):
        values = {tuple(getattr(row, col) for col in columns) for row in dfc}
    else:
        values = {getattr(x, columns) for x in dfc}

    return values


@track_timing(timer_stats_collector)
def models_to_dataframe(models: list[DSGBaseModel], table_name: str | None = None) -> DataFrame:
    """Converts a list of Pydantic models to a Spark DataFrame.

    Parameters
    ----------
    models : list
    table_name : str | None
        If set, a unique ID to use as the cached table name. Return from cache if already stored.
    """
    spark = get_spark_session()
    if not use_duckdb():
        if (
            table_name is not None
            and spark.catalog.tableExists(table_name)
            and spark.catalog.isCached(table_name)
        ):
            return spark.table(table_name)

    assert models
    cls = type(models[0])
    rows = []
    struct_fields = []
    for i, model in enumerate(models):
        dct = {}
        for f in cls.model_fields:
            val = getattr(model, f)
            if isinstance(val, enum.Enum):
                val = val.value
            if i == 0:
                if val is None:
                    python_type = cls.model_fields[f].annotation
                    origin = get_origin(python_type)
                    if origin is Union or origin is UnionType:
                        python_type = get_type_from_union(python_type)
                    # else: will likely fail below
                    # Need to add more logic to detect the actual type or add to
                    # PYTHON_TO_SPARK_TYPES.
                else:
                    python_type = type(val)
                spark_type = PYTHON_TO_SPARK_TYPES[python_type]()
                struct_fields.append(StructField(f, spark_type, nullable=True))
            dct[f] = val
        rows.append(tuple(dct.values()))

    schema = StructType(struct_fields)
    df = spark.createDataFrame(rows, schema=schema)

    if not use_duckdb() and table_name is not None:
        df.createOrReplaceTempView(table_name)
        df.cache()

    return df


def get_type_from_union(python_type) -> Type:
    """Return the Python type from a Union.

    Only works if it is Union of NoneType and something.

    Raises
    ------
    NotImplementedError
        Raised if the code does know how to determine the type.
    """
    args = get_args(python_type)
    if issubclass(args[0], enum.Enum):
        python_type = type(next(iter(args[0])).value)
    else:
        types = [x for x in args if not issubclass(x, type(None))]
        if not types:
            msg = f"Unhandled Union type: {python_type=} {args=}"
            raise NotImplementedError(msg)
        elif len(types) > 1:
            msg = f"Unhandled Union type: {types=}"
            raise NotImplementedError(msg)
        else:
            python_type = types[0]

    return python_type


@track_timing(timer_stats_collector)
def create_dataframe_from_dimension_ids(records, *dimension_types, cache=True) -> DataFrame:
    """Return a DataFrame created from the IDs of dimension_types.

    Parameters
    ----------
    records : sequence
        Iterable of lists of record IDs
    dimension_types : tuple
    cache : If True, cache the DataFrame.
    """
    schema = StructType()
    for dimension_type in dimension_types:
        schema.add(dimension_type.value, StringType(), nullable=False)
    df = get_spark_session().createDataFrame(records, schema=schema)
    if not use_duckdb() and cache:
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
    filter_str = " OR ".join((f"{x} IS NULL" for x in cols_to_check))
    df.createOrReplaceTempView("tmp_view")

    try:
        # Avoid iterating with many checks unless we know there is at least one failure.
        nulls = sql(f"SELECT {cols_str} FROM tmp_view WHERE {filter_str}")
        if not is_dataframe_empty(nulls):
            cols_with_null = set()
            for col in cols_to_check:
                if not is_dataframe_empty(nulls.select(col).filter(f"{col} is NULL")):
                    cols_with_null.add(col)
            assert cols_with_null, "Did not find any columns with NULL values"

            msg = f"DataFrame contains NULL value(s) for column(s): {cols_with_null}"
            raise DSGInvalidField(msg)
    finally:
        sql("DROP VIEW tmp_view")


@track_timing(timer_stats_collector)
def overwrite_dataframe_file(filename: Path | str, df: DataFrame) -> DataFrame:
    """Perform an in-place overwrite of a Spark DataFrame, accounting for different file types
    and symlinks.

    Do not attempt to access the original dataframe unless it was fully cached.
    """
    spark = get_spark_session()
    suffix = Path(filename).suffix
    tmp = str(filename) + ".tmp"
    if suffix == ".parquet":
        df.write.parquet(tmp)
        read_method = read_parquet
        kwargs = {}
    elif suffix == ".csv":
        df.write.csv(str(tmp), header=True)
        read_method = spark.read.csv
        kwargs = {"header": True, "schema": df.schema}
    elif suffix == ".json":
        df.write.json(str(tmp))
        read_method = spark.read.json
        kwargs = {}
    delete_if_exists(filename)
    os.rename(tmp, str(filename))
    return read_method(str(filename), **kwargs)


@track_timing(timer_stats_collector)
def persist_intermediate_query(
    df: DataFrame, scratch_dir_context: ScratchDirContext, auto_partition=False
) -> DataFrame:
    """Persist the current query to files and then read it back and return it.

    This is advised when the query has become too complex or when the query might be evaluated
    twice.

    Parameters
    ----------
    df : DataFrame
    scratch_dir_context : ScratchDirContext
    auto_partition : bool
        If True, call write_dataframe_and_auto_partition.

    Returns
    -------
    DataFrame
    """
    spark = get_spark_session()
    tmp_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    if auto_partition:
        return write_dataframe_and_auto_partition(df, tmp_file)
    df.write.parquet(str(tmp_file))
    return spark.read.parquet(str(tmp_file))


@track_timing(timer_stats_collector)
def write_dataframe_and_auto_partition(
    df: DataFrame,
    filename: Path,
    partition_size_mb: int = MAX_PARTITION_SIZE_MB,
    columns: list[str] | None = None,
    rtol_pct: float = 50,
    min_num_partitions: int = 36,
) -> DataFrame:
    """Write a dataframe to a Parquet file and then automatically coalesce or repartition it if
    needed. If the file already exists, it will be overwritten.

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
    min_num_partitions : int
        Minimum number of partitions to create. If the number of partitions is less than this,
        Do not coalesce/repartition because it will reduce parallelism.

    Raises
    ------
    DSGInvalidParameter
        Raised if a non-Parquet file is passed
    """
    suffix = Path(filename).suffix
    if suffix != ".parquet":
        msg = "write_dataframe_and_auto_partition only supports Parquet files: {filename=}"
        raise DSGInvalidParameter(msg)

    start_initial_write = time.time()
    if filename.exists():
        df = overwrite_dataframe_file(filename, df)
    else:
        df.write.parquet(str(filename))
        df = read_parquet(filename)

    end_initial_write = time.time()
    duration_first_write = end_initial_write - start_initial_write

    if use_duckdb():
        logger.debug("write_dataframe_and_auto_partition is not optimized for DuckDB")
        return df

    num_partitions = len(list(filename.parent.iterdir()))
    if num_partitions < min_num_partitions:
        logger.info(
            "Not coalescing %s because it has only %s partitions, "
            "which is less than the minimum of %s.",
            filename,
            num_partitions,
            min_num_partitions,
        )
        # TODO: consider repartitioning to increase the number of partitions.
        return df

    partition_size_bytes = partition_size_mb * 1024 * 1024
    total_size = sum((x.stat().st_size for x in filename.glob("*.parquet")))
    desired = math.ceil(total_size / partition_size_bytes)
    actual = len(list(filename.glob("*.parquet")))
    if abs(actual - desired) / desired * 100 < rtol_pct:
        logger.info("No change in number of partitions is needed for %s.", filename)
    elif actual > desired:
        df = df.coalesce(desired)
        df = overwrite_dataframe_file(filename, df)
        duration_second_write = time.time() - end_initial_write
        logger.info(
            "Coalesced %s from partition count %s to %s. "
            "duration_first_write=%s duration_second_write=%s",
            filename,
            actual,
            desired,
            duration_first_write,
            duration_second_write,
        )
    else:
        if columns is None:
            df = df.repartition(desired)
        else:
            df = df.repartition(desired, *columns)
        df = overwrite_dataframe_file(filename, df)
        duration_second_write = time.time() - end_initial_write
        logger.info(
            "Repartitioned %s from partition count %s to %s. "
            "duration_first_write=%s duration_second_write=%s",
            filename,
            actual,
            desired,
            duration_first_write,
            duration_second_write,
        )

    logger.info("Wrote dataframe to %s", filename)
    return df


@track_timing(timer_stats_collector)
def write_dataframe(df: DataFrame, filename: str | Path, overwrite: bool = False) -> None:
    """Write a Spark DataFrame, accounting for different file types.

    Parameters
    ----------
    filename : str
    df : pyspark.sql.DataFrame
    """
    path = Path(filename)
    if overwrite:
        delete_if_exists(path)

    suffix = path.suffix
    name = str(filename)
    if suffix == ".parquet":
        df.write.parquet(name)
    elif suffix == ".csv":
        df.write.csv(name, header=True)
    elif suffix == ".json":
        if use_duckdb():
            new_name = name.replace(".json", ".parquet")
            df.write.parquet(new_name)
        else:
            df.write.json(name)


@track_timing(timer_stats_collector)
def persist_table(df: DataFrame, context: ScratchDirContext, tag=None) -> Path:
    """Persist a table to the scratch directory. This can be helpful to avoid multiple
    evaluations of the same query.
    """
    # Note: This does not use the Spark warehouse because we are not properly configuring or
    # managing it across sessions. And, we are already using the scratch dir for our own files.
    path = context.get_temp_filename(suffix=".parquet")
    logger.info("Start persist_table %s %s", path, tag or "")
    write_dataframe(df, path)
    logger.info("Completed persist_table %s %s", path, tag or "")
    return path


@track_timing(timer_stats_collector)
def save_to_warehouse(df: DataFrame, table_name: str) -> DataFrame:
    """Save a table to the Spark warehouse. Not supported when using DuckDB."""
    if use_duckdb():
        msg = "save_to_warehouse is not supported when using DuckDB"
        raise DSGInvalidOperation(msg)

    logger.info("Start saveAsTable to warehouse %s", table_name)
    df.write.saveAsTable(table_name)
    logger.info("Completed saveAsTable %s", table_name)
    return df.sparkSession.sql(f"select * from {table_name}")


def sql(query: str) -> DataFrame:
    """Run a SQL query with Spark."""
    logger.debug("Run SQL query [%s]", query)
    return get_spark_session().sql(query)


def load_stored_table(table_name: str) -> DataFrame:
    """Return a table stored in the Spark warehouse."""
    spark = get_spark_session()
    return spark.table(table_name)


def try_load_stored_table(
    table_name: str, database: str | None = DSGRID_DB_NAME
) -> DataFrame | None:
    """Return a table if it is stored in the Spark warehouse."""
    spark = get_spark_session()
    full_name = f"{database}.{table_name}"
    if spark.catalog.tableExists(full_name):
        return spark.table(table_name)
    return None


def is_table_stored(table_name, database=DSGRID_DB_NAME):
    spark = get_spark_session()
    full_name = f"{database}.{table_name}"
    return spark.catalog.tableExists(full_name)


def save_table(table, table_name, overwrite=True, database=DSGRID_DB_NAME):
    full_name = f"{database}.{table_name}"
    if overwrite:
        table.write.mode("overwrite").saveAsTable(full_name)
    else:
        table.write.saveAsTable(full_name)


def list_tables(database=DSGRID_DB_NAME):
    spark = get_spark_session()
    return [x.name for x in spark.catalog.listTables(dbName=database)]


def drop_table(table_name, database=DSGRID_DB_NAME):
    spark = get_spark_session()
    if is_table_stored(table_name, database=database):
        spark.sql(f"DROP TABLE {table_name}")
        logger.info("Dropped table %s", table_name)


@track_timing(timer_stats_collector)
def create_dataframe_from_product(
    data: dict[str, list[str]],
    context: ScratchDirContext,
    max_partition_size_mb=MAX_PARTITION_SIZE_MB,
) -> DataFrame:
    """Create a dataframe by taking a product of values/columns in a dict.

    Parameters
    ----------
    data : dict
        Columns on which to perform a cross product.
        {"sector": [com], "subsector": ["SmallOffice", "LargeOffice"]}
    context : ScratchDirContext
        Manages temporary files.
    """
    # dthom: 1/29/2024
    # This implementation creates a product of all columns in Python, writes them to temporary
    # CSV files, and then loads that back into Spark.
    # This is the fastest way I've found to pass a large dataframe from the Spark driver (Python
    # app) to the Spark workers on compute nodes.
    # The total size of a table can be large depending on the numbers of dimensions. For example,
    # comstock_conus_2022_projected is 3108 counties * 41 model years * 21 end uses * 14 subsectors * 3 scenarios
    #   112_391_496 rows. The CSV files are ~7.7 GB.
    #   (Note that, due to compression, the same table in Parquet is 7 MB.)
    # This is not ideal because it writes temporary files to the filesystem.
    # Other solutions tried:
    # 1. spark.createDataFrame(spark.sparkContext.parallelize(itertools.product(*(data.values()))), list(data.keys))
    #    Reasonably fast until the data is larger than Spark's max RPC message size. Then it fails.
    # 2. Create an RDD and then call rdd.flatMap with the output of itertools.product. Very slow.
    # 3. Create one Spark DataFrame per column and then cross-join all of them. Extremely slow.
    # 4. Create one pyarrow Table, write to temp Parquet, read back in Spark. ~2x slower
    #    than CSV implementaion.
    # 5. Create the joined table via SQLite and then read the contents into Spark with a JDBC
    #    driver. Much slower.

    # Note: This location must be accessible on all compute nodes.
    csv_dir = context.get_temp_filename(suffix=".csv")
    columns = list(data.keys())
    schema = StructType([StructField(x, StringType()) for x in columns])

    with CsvPartitionWriter(csv_dir, max_partition_size_mb=max_partition_size_mb) as writer:
        for row in itertools.product(*(data.values())):
            writer.add_row(row)

    spark = get_spark_session()
    if use_duckdb():
        df = spark.read.csv(f"{csv_dir}/*.csv", header=False, schema=schema)
    else:
        df = spark.read.csv(str(csv_dir), header=False, schema=schema)
    return df


class CsvPartitionWriter:
    """Writes dataframe rows to partitioned CSV files."""

    def __init__(self, directory: Path, max_partition_size_mb: int = MAX_PARTITION_SIZE_MB):
        self._directory = directory
        self._directory.mkdir(exist_ok=True)
        self._max_size = max_partition_size_mb * 1024 * 1024
        self._size = 0
        self._index = 1
        self._fp = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self._fp is not None:
            self._fp.close()

    def add_row(self, row: tuple) -> None:
        """Add a row to the CSV files."""
        line = ",".join(row)
        if self._fp is None:
            filename = self._directory / f"part{self._index}.csv"
            self._fp = open(filename, "w", encoding="utf-8")
        self._size += self._fp.write(line)
        self._size += self._fp.write("\n")
        if self._size >= self._max_size:
            self._fp.close()
            self._fp = None
            self._size = 0
            self._index += 1


@contextmanager
def custom_spark_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.

    """
    spark = get_duckdb_spark_session()
    if spark is not None:
        yield
        return

    spark = get_spark_session()
    orig_settings = {}

    try:
        for key, val in conf.items():
            orig_settings[key] = spark.conf.get(key)
            spark.conf.set(key, val)
            logger.info("Set %s=%s temporarily", key, val)
        yield
    finally:
        # Note that the user code could have restarted the session.
        # Get the current one.
        spark = get_spark_session()
        for key, val in orig_settings.items():
            spark.conf.set(key, val)


@contextmanager
def custom_time_zone(time_zone: str):
    """Apply a custom Spark time zone for the duration of a code block."""
    orig_time_zone = get_current_time_zone()
    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        # Note that the user code could have restarted the session.
        # This will function will get the current one.
        set_current_time_zone(orig_time_zone)


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
    spark = get_duckdb_spark_session()
    if spark is not None:
        yield spark
        return

    spark = get_spark_session()
    app_name = spark.conf.get("spark.app.name")
    orig_settings = {}

    try:
        for name in conf:
            current = spark.conf.get(name, None)
            if current is not None:
                orig_settings[name] = current
        new_spark = restart_spark(name=app_name, spark_conf=conf, force=force)
        yield new_spark
    finally:
        restart_spark(name=app_name, spark_conf=orig_settings, force=force)


@contextmanager
def set_session_time_zone(time_zone: str) -> Generator[None, None, None]:
    """Set the session time zone for execution of a code block."""
    orig = get_current_time_zone()

    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        set_current_time_zone(orig)


def union(dfs: list[DataFrame]) -> DataFrame:
    """Return a union of the dataframes, ensuring that the columns match."""
    df = dfs[0]
    if len(dfs) > 1:
        for dft in dfs[1:]:
            if df.columns != dft.columns:
                msg = f"columns don't match: {df.columns=} {dft.columns=}"
                raise Exception(msg)
            df = df.union(dft)
    return df
