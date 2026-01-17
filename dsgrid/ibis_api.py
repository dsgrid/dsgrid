import logging
import os
import shutil
import json
import itertools
import enum
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterable, Sequence, Type, get_args
from uuid import uuid4

import ibis
from ibis import BaseBackend
import pandas as pd

from dsgrid.common import BackendEngine
import dsgrid
from dsgrid.data_models import DSGBaseModel
from dsgrid.exceptions import (
    DSGInvalidField,
    DSGInvalidFile,
    DSGInvalidParameter,
)
from dsgrid.utils.files import delete_if_exists
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.timing import track_timing, timer_stats_collector

logger = logging.getLogger(__name__)

_IBIS_CON: BaseBackend | None = None
TEMP_TABLE_PREFIX = "tmp_dsgrid"
MAX_PARTITION_SIZE_MB = 128
DSGRID_DB_NAME = "default"


def get_backend_engine() -> BackendEngine:
    return dsgrid.runtime_config.backend_engine


def get_ibis_connection() -> BaseBackend:
    """Return the active Ibis connection."""
    global _IBIS_CON
    if _IBIS_CON is not None:
        return _IBIS_CON

    backend = get_backend_engine()

    if backend == BackendEngine.DUCKDB:
        # Use an in-memory DuckDB connection
        _IBIS_CON = ibis.duckdb.connect()
    elif backend == BackendEngine.SPARK:
        session = get_spark_session()
        _IBIS_CON = ibis.pyspark.connect(session)
    else:
        msg = f"Unsupported backend: {backend}"
        raise ValueError(msg)

    return _IBIS_CON


def reset_ibis_connection():
    """Reset the global Ibis connection. Useful if Spark session restarts."""
    global _IBIS_CON
    _IBIS_CON = None


def get_spark_session():
    """Return the active SparkSession or create a new one is none is active.

    Returns None if using DuckDB backend.
    """
    if get_backend_engine() == BackendEngine.DUCKDB:
        return None

    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.warning("Could not find a SparkSession. Create a new one.")
        spark = SparkSession.builder.getOrCreate()
        log_spark_conf(spark)
    return spark


def init_spark_session(name="dsgrid", check_env=True, spark_conf=None):
    """Initialize a SparkSession."""
    if get_backend_engine() == BackendEngine.DUCKDB:
        logger.info("Using DuckDB as the backend engine.")
        # Ensure Ibis connection is initialized
        get_ibis_connection()
        return None

    from pyspark.sql import SparkSession
    from pyspark import SparkConf
    from dsgrid.loggers import disable_console_logging

    logger.info("Using Spark as the backend engine.")
    cluster = os.environ.get("SPARK_CLUSTER")
    conf = SparkConf().setAppName(name)
    if spark_conf is not None:
        for key, val in spark_conf.items():
            conf.set(key, val)

    out_ts_type = conf.get("spark.sql.parquet.outputTimestampType")
    if out_ts_type is None:
        conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    elif out_ts_type != "TIMESTAMP_MICROS":
        logger.warning(
            "spark.sql.parquet.outputTimestampType is set to %s. Writing parquet files may "
            "produced undesired results.",
            out_ts_type,
        )
    conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

    if check_env and cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf.setMaster(cluster)

    config = SparkSession.builder.config(conf=conf)
    if dsgrid.runtime_config.use_hive_metastore:
        config = config.enableHiveSupport()
    spark = config.getOrCreate()

    with disable_console_logging():
        log_spark_conf(spark)
        logger.info("Custom configuration settings: %s", spark_conf)

    # Reset ibis connection so it picks up the new spark session
    reset_ibis_connection()
    return spark


def restart_spark(*args, force=False, **kwargs):
    """Restart a SparkSession with new config parameters. Refer to init_spark for parameters."""
    if get_backend_engine() == BackendEngine.DUCKDB:
        # DuckDB reset logic if needed, or no-op
        reset_ibis_connection()
        return None

    spark = get_spark_session()
    if spark is None:
        return init_spark_session(*args, **kwargs)

    # Logic for checking config changes and restarting
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
        reset_ibis_connection()
        logger.info("Stopped the SparkSession so that it can be restarted with a new config.")
        spark = init_spark_session(*args, **kwargs)
        if spark.conf.get("spark.sql.session.timeZone") != new_time_zone:
            spark.conf.set("spark.sql.session.timeZone", new_time_zone)
    else:
        logger.info("No restart of Spark is needed.")

    return spark


def log_spark_conf(spark):
    """Log the Spark configuration details."""
    if get_backend_engine() != BackendEngine.DUCKDB:
        conf = spark.sparkContext.getConf().getAll()
        conf.sort(key=lambda x: x[0])
        logger.info("Spark conf: %s", "\n".join([f"{x} = {y}" for x, y in conf]))


def get_current_time_zone() -> str:
    """Return the current time zone."""
    con = get_ibis_connection()
    if con.name == "duckdb":
        # Execute raw SQL on the underlying DuckDB connection
        res = con.con.execute("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone'").fetchone()
        return res[1]  # Value is in the second column

    spark = get_spark_session()
    tz = spark.conf.get("spark.sql.session.timeZone")
    assert tz is not None
    return tz


def set_current_time_zone(time_zone: str) -> None:
    """Set the current time zone."""
    con = get_ibis_connection()
    if con.name == "duckdb":
        con.con.execute(f"SET TimeZone='{time_zone}'")
        return

    spark = get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", time_zone)


@contextmanager
def custom_spark_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block."""
    if get_backend_engine() == BackendEngine.DUCKDB:
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
        set_current_time_zone(orig_time_zone)


@contextmanager
def restart_spark_with_custom_conf(conf: dict, force=False):
    """Restart the SparkSession with a custom configuration."""
    if get_backend_engine() == BackendEngine.DUCKDB:
        yield None
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


def make_temp_view_name() -> str:
    """Make a random name to be used as a view."""
    return f"{TEMP_TABLE_PREFIX}_{uuid4().hex}"


def create_temp_view(df: Any) -> str:
    """Create a temporary view with a random name and return the name."""
    view_name = make_temp_view_name()
    con = get_ibis_connection()
    con.create_view(view_name, df, overwrite=True)
    return view_name


def drop_temp_tables_and_views() -> None:
    """Drop all temporary views and tables."""
    drop_temp_views()
    drop_temp_tables()


def drop_temp_tables() -> None:
    """Drop all temporary tables."""
    con = get_ibis_connection()
    # TODO: Standardize listing tables/views in Ibis if possible.
    if con.name == "duckdb":
        query = f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%{TEMP_TABLE_PREFIX}%' AND table_type = 'BASE TABLE'"
        # execute via raw connection or ibis sql
        try:
            tables = con.sql(query).to_pyarrow().to_pylist()
        except Exception:
            # Fallback if sql method not available or fails
            return
        for row in tables:
            con.drop_table(row["table_name"], force=True)
    else:
        # Spark
        spark = get_spark_session()
        for row in spark.sql(f"SHOW TABLES LIKE '* {TEMP_TABLE_PREFIX}*' ").collect():
            spark.sql(f"DROP TABLE {row.tableName}")


def drop_temp_views() -> None:
    """Drop all temporary views."""
    con = get_ibis_connection()
    if con.name == "duckdb":
        query = f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%{TEMP_TABLE_PREFIX}%' AND table_type = 'VIEW'"
        try:
            views = con.sql(query).to_pyarrow().to_pylist()
        except Exception:
            return
        for row in views:
            con.drop_view(row["table_name"], force=True)
    else:
        spark = get_spark_session()
        for row in spark.sql(f"SHOW VIEWS LIKE '* {TEMP_TABLE_PREFIX}*' ").collect():
            spark.sql(f"DROP VIEW {row.viewName}")


def read_csv(path: Path | str, schema: dict[str, str] | None = None) -> ibis.Table:
    """Return an Ibis Table from a CSV file."""
    path = Path(path) if isinstance(path, str) else path
    con = get_ibis_connection()
    if path.is_file():
        path_str = str(path)
    elif get_backend_engine() == BackendEngine.SPARK:
        # Spark's CSV reader handles directories directly without glob patterns
        path_str = str(path)
    else:
        # DuckDB supports glob patterns for reading multiple CSV files
        path_str = f"{path}/**/*.csv"

    if get_backend_engine() == BackendEngine.SPARK:
        # Disable schema inference for Spark to preserve leading zeros in string columns.
        # When inferSchema=True (default), Spark converts "06073" to integer 6073 then to "6073".
        return con.read_csv(path_str, inferSchema=False, header=True)
    return con.read_csv(path_str)


def read_json(path: Path | str) -> ibis.Table:
    """Return an Ibis Table from a JSON file."""
    filename = str(path)
    con = get_ibis_connection()
    # DuckDB read_json might fail with BOM or if it's NDJSON with some issues.
    try:
        return con.read_json(filename)
    except Exception as e:
        try:
            with open(filename, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            return ibis.memtable(data)
        except Exception:
            # Try NDJSON
            try:
                with open(filename, "r", encoding="utf-8-sig") as f:
                    data = [json.loads(line) for line in f if line.strip()]
                return ibis.memtable(data)
            except Exception:
                pass
        raise e


def read_parquet(path: Path | str) -> ibis.Table:
    path = Path(path) if isinstance(path, str) else path
    con = get_ibis_connection()
    if path.is_file():
        path_str = str(path)
    elif get_backend_engine() == BackendEngine.SPARK:
        # Spark's parquet reader handles directories directly without glob patterns
        path_str = str(path)
    else:
        # DuckDB supports glob patterns for reading multiple parquet files
        path_str = f"{path}/**/*.parquet"
    return con.read_parquet(path_str)


@track_timing(timer_stats_collector)
def create_dataframe(records, table_name=None, require_unique=None, columns=None) -> ibis.Table:
    """Create a DataFrame from a list of records."""
    # Handle legacy signature: create_dataframe(data, columns)
    if isinstance(table_name, list) and columns is None:
        columns = table_name
        table_name = None

    # ibis.memtable expects list of dicts or tuples with schema.
    try:
        if columns:
            df = ibis.memtable(records, columns=columns)
        else:
            df = ibis.memtable(records)
    except Exception:
        # Fallback: try converting to pandas first if records is complex
        if columns:
            df = ibis.memtable(pd.DataFrame(records, columns=columns))
        else:
            df = ibis.memtable(pd.DataFrame(records))

    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_ids(ids: Iterable[str], column: str) -> ibis.Table:
    """Create a DataFrame from a list of dimension IDs."""
    return ibis.memtable({column: list(ids)})


def create_dataframe_from_pandas(df) -> ibis.Table:
    """Create a DataFrame from a pandas DataFrame."""
    return ibis.memtable(df)


def create_dataframe_from_dicts(records: list[dict[str, Any]]) -> ibis.Table:
    """Create a DataFrame from a list of dictionaries."""
    if not records:
        msg = "records cannot be empty in create_dataframe_from_dicts"
        raise DSGInvalidParameter(msg)

    return ibis.memtable(records)


def try_read_dataframe(filename: Path, delete_if_invalid=True, **kwargs):
    """Try to read the dataframe."""
    if not filename.exists():
        return None

    try:
        df = read_dataframe(filename, **kwargs)
        # Force execution to ensure file validity
        df.count().execute()
        return df
    except (DSGInvalidFile, Exception):
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
) -> ibis.Table:
    """Create a DataFrame from a file."""
    # Note: read_with_spark param is legacy naming, means "read with backend engine"
    filename = str(filename)
    if not os.path.exists(filename):
        msg = f"{filename} does not exist"
        raise FileNotFoundError(msg)

    suffix = Path(filename).suffix
    if suffix == ".csv":
        df = read_csv(filename)
    elif suffix == ".parquet":
        df = read_parquet(filename)
    elif suffix == ".json":
        df = read_json(filename)
    else:
        # Fallback to pandas for native read if requested?
        # Note: _read_natively is removed as it uses Spark DFs.
        # For DuckDB/Ibis, it's all native anyway.
        # If we reach here, it's an unsupported extension.
        assert False, f"Unsupported file extension: {filename}"

    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


def _post_process_dataframe(df, table_name=None, require_unique=None):
    if table_name is not None:
        con = get_ibis_connection()
        con.create_view(table_name, df, overwrite=True)

    if require_unique is not None:
        # Check unique
        # df is Ibis Table
        for column in require_unique:
            cnt = df.count().execute()
            distinct_cnt = df[column].nunique().execute()
            if cnt != distinct_cnt:
                msg = f"DataFrame has duplicate entries for {column}"
                raise DSGInvalidField(msg)


@track_timing(timer_stats_collector)
def models_to_dataframe(models: list[DSGBaseModel], table_name: str | None = None) -> ibis.Table:
    """Converts a list of Pydantic models to a DataFrame."""
    if not models:
        # Return empty dataframe? Needs schema.
        # For now assume models is not empty as per original assertion
        assert models

    # Logic to convert models to dicts
    records = [model.model_dump() for model in models]  # pydantic v2

    df = ibis.memtable(records)
    if table_name:
        con = get_ibis_connection()
        con.create_view(table_name, df, overwrite=True)
    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_dimension_ids(records, *dimension_types, cache=True) -> ibis.Table:
    """Return a DataFrame created from the IDs of dimension_types."""
    cols = [dt.value for dt in dimension_types]
    # records is iterable of lists
    # map to list of dicts for memtable
    data = [dict(zip(cols, row)) for row in records]
    df = ibis.memtable(data)
    # cache param ignored for memtable or could be used if we wrap it
    return df


@track_timing(timer_stats_collector)
def overwrite_dataframe_file(filename: Path | str, df: ibis.Table) -> ibis.Table:
    """Perform an in-place overwrite of a DataFrame."""
    filename = Path(filename)
    tmp = str(filename) + ".tmp"
    suffix = filename.suffix
    con = get_ibis_connection()

    if suffix == ".parquet":
        con.to_parquet(df, tmp)
        read_method = read_parquet
        kwargs = {}
    elif suffix == ".csv":
        con.to_csv(df, tmp)
        read_method = read_csv
        kwargs = {}
    elif suffix == ".json":
        msg = "overwrite_dataframe_file json not fully supported in Ibis migration yet"
        raise NotImplementedError(msg)
    else:
        msg = f"Unsupported suffix {suffix}"
        raise NotImplementedError(msg)

    delete_if_exists(filename)
    os.rename(tmp, str(filename))
    return read_method(str(filename), **kwargs)


@track_timing(timer_stats_collector)
def persist_intermediate_query(
    df: ibis.Table, scratch_dir_context: ScratchDirContext, auto_partition=False
) -> ibis.Table:
    """Persist the current query to files and then read it back and return it."""
    tmp_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    get_ibis_connection().to_parquet(df, tmp_file)
    return get_ibis_connection().read_parquet(tmp_file)


@track_timing(timer_stats_collector)
def write_dataframe_and_auto_partition(
    df: ibis.Table,
    filename: Path,
    partition_size_mb: int = MAX_PARTITION_SIZE_MB,
    columns: list[str] | None = None,
    rtol_pct: float = 50,
    min_num_partitions: int = 36,
) -> ibis.Table:
    """Write a dataframe to a Parquet file."""
    # Simplified for Ibis: Just write.
    delete_if_exists(filename)
    get_ibis_connection().to_parquet(df, filename)
    return read_parquet(filename)


@track_timing(timer_stats_collector)
def write_dataframe(df: ibis.Table, filename: str | Path, overwrite: bool = False) -> None:
    """Write a DataFrame to a file.

    This function materializes the dataframe first to avoid issues where
    ibis views reference tables that may have changed schemas.
    """
    import pyarrow.parquet as pq

    path = Path(filename)
    if overwrite:
        delete_if_exists(path)

    suffix = path.suffix
    if suffix == ".parquet":
        # Materialize the dataframe by converting to PyArrow table first.
        # This breaks any view dependency chains that could cause schema mismatch errors.
        arrow_table = df.to_pyarrow()
        pq.write_table(arrow_table, path)
    elif suffix == ".csv":
        con = get_ibis_connection()
        con.to_csv(df, path)
    elif suffix == ".json":
        msg = "write_dataframe json not fully supported in Ibis migration yet"
        raise NotImplementedError(msg)
    else:
        msg = f"Unsupported suffix {suffix}"
        raise NotImplementedError(msg)


@track_timing(timer_stats_collector)
def persist_table(df: ibis.Table, context: ScratchDirContext, tag=None) -> Path:
    """Persist a table to the scratch directory."""
    path = context.get_temp_filename(suffix=".parquet")
    logger.info("Start persist_table %s %s", path, tag or "")
    write_dataframe(df, path)
    logger.info("Completed persist_table %s %s", path, tag or "")
    return path


@track_timing(timer_stats_collector)
def create_dataframe_from_product(
    data: dict[str, list[str]],
    context: ScratchDirContext,
    max_partition_size_mb=MAX_PARTITION_SIZE_MB,
) -> ibis.Table:
    """Create a dataframe by taking a product of values/columns in a dict."""
    csv_dir = context.get_temp_filename(suffix=".csv")
    columns = list(data.keys())

    with CsvPartitionWriter(
        csv_dir, header=columns, max_partition_size_mb=max_partition_size_mb
    ) as writer:
        for row in itertools.product(*(data.values())):
            writer.add_row(row)

    return read_csv(csv_dir)


class CsvPartitionWriter:
    """Writes dataframe rows to partitioned CSV files."""

    def __init__(
        self,
        directory: Path,
        header: list[str] | None = None,
        max_partition_size_mb: int = MAX_PARTITION_SIZE_MB,
    ):
        self._directory = directory
        self._directory.mkdir(exist_ok=True)
        self._header = header
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
            if self._header:
                self._fp.write(",".join(self._header) + "\n")
        self._size += self._fp.write(line)
        self._size += self._fp.write("\n")
        if self._size >= self._max_size:
            self._fp.close()
            self._fp = None
            self._size = 0
            self._index += 1


def get_unique_values(df: ibis.Table, columns: Sequence[str]) -> set[str]:
    """Return the unique values of a dataframe in one column or a list of columns."""
    if isinstance(columns, str):
        cols = [columns]
    else:
        cols = list(columns)

    res = df.select(cols).distinct().to_pyarrow().to_pylist()

    if len(cols) > 1:
        values = {tuple(row[col] for col in cols) for row in res}
    else:
        values = {row[cols[0]] for row in res}
    return values


@track_timing(timer_stats_collector)
def check_for_nulls(df, exclude_columns=None):
    """Check if a DataFrame has null values."""
    if exclude_columns is None:
        exclude_columns = set()

    # df is Ibis Table
    cols_to_check = set(df.columns).difference(exclude_columns)
    for col in cols_to_check:
        # Check if any nulls in col
        null_count = df[col].isnull().sum().execute()
        if null_count > 0:
            msg = f"DataFrame contains NULL value(s) for column(s): {col}"
            raise DSGInvalidField(msg)


def prepare_timestamps_for_dataframe(timestamps: Iterable[datetime]) -> Iterable[datetime]:
    """Apply necessary conversions of the timestamps for dataframe creation."""
    return [x.astimezone(ZoneInfo("UTC")) for x in timestamps]


def pivot(df: ibis.Table, name_column: str, value_column: str) -> ibis.Table:
    """Unpivot the dataframe."""
    return df.pivot_wider(names_from=name_column, values_from=value_column, values_agg="sum")


def select_expr(df: ibis.Table, exprs: list[str]) -> ibis.Table:
    """Execute the SQL SELECT expression."""
    view = create_temp_view(df)
    con = get_ibis_connection()
    cols = ",".join(exprs)
    return con.sql(f"SELECT {cols} FROM {view}")


def sql_from_df(df: ibis.Table, query: str) -> ibis.Table:
    """Run a SQL query on a dataframe."""
    logger.debug("Run SQL query [%s]", query)
    view = create_temp_view(df)
    query += f" FROM {view}"
    con = get_ibis_connection()
    return con.sql(query)


def perform_interval_op(
    df: ibis.Table, time_column, op: str, val: Any, unit: str, alias: str
) -> ibis.Table:
    """Perform an interval operation ('-' or '+') on a time column."""
    # Ibis interval math
    # df is Ibis table
    interval = ibis.interval(val, unit.lower())
    col = df[time_column]
    if op == "-":
        expr = col - interval
    elif op == "+":
        expr = col + interval
    else:
        msg = f"{op=}"
        raise NotImplementedError(msg)

    return df.mutate(**{alias: expr})


def handle_column_spaces(column: str) -> str:
    """Return a column string suitable for the backend.

    DuckDB uses double quotes for identifiers, Spark uses backticks.
    """
    if get_backend_engine() == BackendEngine.SPARK:
        return f"`{column}`"
    return f'"{column}"'


def is_dataframe_empty(df: ibis.Table) -> bool:
    """Check if a DataFrame is empty.

    Parameters
    ----------
    df : ibis.Table
        The dataframe to check.

    Returns
    -------
    bool
        True if the dataframe has zero rows.
    """
    # Use limit(1) to avoid counting all rows - we only need to know if any exist
    return df.limit(1).to_pyarrow().num_rows == 0


@track_timing(timer_stats_collector)
def save_to_warehouse(df: ibis.Table, table_name: str) -> ibis.Table:
    """Save a table to the warehouse."""
    logger.info("Start save_to_warehouse %s", table_name)
    con = get_ibis_connection()
    con.create_table(table_name, df, overwrite=True)
    logger.info("Completed save_to_warehouse %s", table_name)
    return con.table(table_name)


def sql(query: str) -> ibis.Table:
    """Run a SQL query."""
    logger.debug("Run SQL query [%s]", query)
    con = get_ibis_connection()
    return con.sql(query)


def load_stored_table(table_name: str) -> ibis.Table:
    """Return a table stored in the warehouse."""
    con = get_ibis_connection()
    return con.table(table_name)


def try_load_stored_table(
    table_name: str, database: str | None = DSGRID_DB_NAME
) -> ibis.Table | None:
    """Return a table if it is stored in the warehouse."""
    con = get_ibis_connection()
    # TODO: Handle database/schema if supported by Ibis backend logic or needed.
    # For now checking list_tables.
    if table_name in con.list_tables():
        return con.table(table_name)
    return None


def is_table_stored(table_name, database=DSGRID_DB_NAME):
    return table_name in get_ibis_connection().list_tables()


def save_table(table, table_name, overwrite=True, database=DSGRID_DB_NAME):
    con = get_ibis_connection()
    # TODO: handle database/schema if needed
    con.create_table(table_name, table, overwrite=overwrite)


def list_tables(database=DSGRID_DB_NAME):
    return get_ibis_connection().list_tables()


def drop_table(table_name, database=DSGRID_DB_NAME):
    con = get_ibis_connection()
    if table_name in con.list_tables():
        con.drop_table(table_name)
        logger.info("Dropped table %s", table_name)


def get_type_from_union(python_type) -> Type:
    """Return the Python type from a Union."""
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
