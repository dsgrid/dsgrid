"""Runtime session and table IO helpers for Ibis-backed execution."""

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
from typing import Any, Generator, Iterable, Type, Union, cast, get_args, get_origin

import pandas as pd
import ibis

from dsgrid.data_models import DSGBaseModel
from dsgrid.exceptions import (
    DSGInvalidField,
    DSGInvalidFile,
    DSGInvalidOperation,
    DSGInvalidParameter,
)
from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.operations import create_temp_view, cross_join, make_temp_view_name
from dsgrid.ibis.io import read_csv, read_json, read_parquet
from dsgrid.ibis.types import is_table_empty, use_duckdb
from dsgrid.loggers import disable_console_logging
from dsgrid.utils.files import delete_if_exists, load_data
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.timing import Timer, track_timing, timer_stats_collector

if not use_duckdb():
    import pyspark.sql.functions as F
    from pyspark.sql import Row, SparkSession
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
        TimestampType,
    )
    from pyspark import SparkConf
    from pyspark.errors import AnalysisException
else:

    class _UnsupportedSparkFunctions:
        def __getattr__(self, name):
            def _unsupported(*args, **kwargs):
                msg = f"Spark function F.{name} is not available with the Ibis DuckDB backend"
                raise DSGInvalidOperation(msg)

            return _unsupported

    class _SparkType:
        pass

    class BooleanType(_SparkType):
        pass

    class ByteType(_SparkType):
        pass

    class DoubleType(_SparkType):
        pass

    class FloatType(_SparkType):
        pass

    class IntegerType(_SparkType):
        pass

    class LongType(_SparkType):
        pass

    class ShortType(_SparkType):
        pass

    class StringType(_SparkType):
        pass

    class TimestampNTZType(_SparkType):
        pass

    class TimestampType(_SparkType):
        pass

    class StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class StructType(list):
        def __init__(self, fields=None):
            super().__init__(fields or [])

        @property
        def names(self):
            return [field.name for field in self]

        def add(self, name, data_type, nullable=True):
            self.append(StructField(name, data_type, nullable=nullable))
            return self

    class Row(tuple):
        pass

    class SparkConf:
        def setAppName(self, name):
            return self

        def get(self, name, defaultValue=None):
            return defaultValue

        def set(self, name, value):
            return self

    class AnalysisException(Exception):
        pass

    class _SparkSessionBuilder:
        def config(self, *args, **kwargs):
            return self

        def getOrCreate(self):
            return get_runtime_session()

    class SparkSession:
        builder = _SparkSessionBuilder()

        @staticmethod
        def getActiveSession():
            return None

    F = _UnsupportedSparkFunctions()


logger = logging.getLogger(__name__)

# Consider using our own database. Would need to manage creation with
# spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
# Doing so has caused conflicts in tests with the Derby db.
DSGRID_DB_NAME = "default"

MAX_PARTITION_SIZE_MB = 128

_DUCKDB_RUNTIME_SESSION: Any = None

PYTHON_TO_SPARK_TYPES: dict[type[Any], Any] = {
    int: IntegerType,
    float: DoubleType,
    str: StringType,
    bool: BooleanType,
}


class _DuckDBConf:
    def __init__(self):
        self._settings = {
            "spark.app.name": "dsgrid",
            "spark.rdd.compress": "true",
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.shuffle.partitions": "200",
        }

    def get(self, name: str, default: Any | None = None) -> Any:
        return self._settings.get(name, default)

    def set(self, name: str, value: Any) -> None:
        self._settings[name] = str(value)


class _DuckDBCatalog:
    def tableExists(self, name: str) -> bool:
        table_name = name.split(".")[-1]
        return make_runtime_backend().has_table(table_name)

    def isCached(self, name: str) -> bool:
        return False

    def listTables(self, dbName: str = DSGRID_DB_NAME) -> list[Any]:
        return [
            type("TableInfo", (), {"name": name}) for name in make_runtime_backend().list_tables()
        ]


class _DuckDBReader:
    def csv(
        self, path: str, header: bool = True, schema: Any | None = None, **kwargs
    ) -> ibis.Table:
        if not header:
            names = _schema_names(schema)
            table = make_runtime_backend().connection.read_csv(
                path, header=False, all_varchar=True
            )
            return (
                table.rename({new: old for new, old in zip(names, table.columns)})
                if names
                else table
            )
        types = _schema_types(schema, ibis_types=False)
        if types:
            return make_runtime_backend().connection.read_csv(path, header=True, types=types)
        return make_runtime_backend().connection.read_csv(path, header=True, all_varchar=True)

    def json(self, path: str, **kwargs) -> ibis.Table:
        return make_runtime_backend().connection.read_json(path)

    def parquet(self, path: str, **kwargs) -> ibis.Table:
        return make_runtime_backend().connection.read_parquet(path)


class _SparkReader:
    def __init__(self, session: Any):
        self._session = session

    def csv(self, path: str, **kwargs) -> ibis.Table:
        schema = kwargs.get("schema")
        if isinstance(schema, dict):
            kwargs = dict(kwargs)
            kwargs["schema"] = _merge_spark_csv_schema(self._session, path, schema, kwargs)
        return _spark_dataframe_to_ibis_table(self._session.read.csv(path, **kwargs))

    def json(self, path: str, **kwargs) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.read.json(path, **kwargs))

    def parquet(self, path: str, **kwargs) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.read.parquet(path, **kwargs))


class _SparkRuntimeSession:
    def __init__(self, session: Any):
        self._session = session
        self.conf = session.conf
        self.catalog = session.catalog
        self.read = _SparkReader(session)
        self.sparkContext = session.sparkContext

    @property
    def raw_session(self) -> Any:
        return self._session

    def createDataFrame(self, data: Any, schema: Any | None = None) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.createDataFrame(data, schema=schema))

    def sql(self, query: str, **kwargs) -> ibis.Table:
        if kwargs:
            return _spark_dataframe_to_ibis_table(self._session.sql(query, **kwargs))
        return make_runtime_backend().sql(query)

    def table(self, name: str) -> ibis.Table:
        return make_runtime_backend().table(name.split(".")[-1])

    def stop(self) -> None:
        self._session.stop()


class _DuckDBRuntimeSession:
    def __init__(self):
        self.conf = _DuckDBConf()
        self.catalog = _DuckDBCatalog()
        self.read = _DuckDBReader()

    def createDataFrame(self, data: Any, schema: Any | None = None) -> ibis.Table:
        return _create_ibis_table(data, schema=schema)

    def sql(self, query: str, **kwargs) -> ibis.Table:
        if kwargs:
            msg = "DuckDB Ibis SQL does not support Spark keyword dataframe bindings"
            raise DSGInvalidOperation(msg)
        return make_runtime_backend().sql(query)

    def table(self, name: str) -> ibis.Table:
        return make_runtime_backend().table(name.split(".")[-1])


if use_duckdb():
    _DUCKDB_RUNTIME_SESSION = _DuckDBRuntimeSession()


def get_duckdb_runtime_session() -> Any:
    """Return the active DuckDB runtime session if it is set."""
    return _DUCKDB_RUNTIME_SESSION


def is_runtime_session_active() -> bool:
    """Return True if a runtime session is already active."""
    return get_duckdb_runtime_session() is not None or SparkSession.getActiveSession() is not None


def get_spark_session() -> Any:
    """Return the active SparkSession, creating one if Spark is the configured backend."""
    if use_duckdb():
        return None
    session = SparkSession.getActiveSession()
    if session is None:
        logger.warning("Could not find a SparkSession. Create a new one.")
        session = _create_spark_session()
    return session


def get_runtime_session() -> Any:
    """Return the active runtime session or create one if none is active."""
    session = get_duckdb_runtime_session()
    if session is not None:
        return session

    return _SparkRuntimeSession(get_spark_session())


def get_current_time_zone() -> str:
    """Return the current time zone."""
    spark = get_runtime_session()
    if use_duckdb():
        conn = cast(Any, make_runtime_backend().connection)
        result = conn.raw_sql("SELECT value FROM duckdb_settings() WHERE name = 'TimeZone'")
        row = result.fetchone()
        assert row is not None
        return row[0]

    tz = spark.conf.get("spark.sql.session.timeZone")
    assert tz is not None
    return tz


def set_current_time_zone(time_zone: str) -> None:
    """Set the current time zone."""
    session = get_runtime_session()
    if use_duckdb():
        if isinstance(session, _DuckDBRuntimeSession):
            escaped = time_zone.replace("'", "''")
            conn = cast(Any, make_runtime_backend().connection)
            conn.raw_sql(f"SET TimeZone='{escaped}'")
        else:
            session.sql(f"SET TimeZone='{time_zone}'")
        return

    session.conf.set("spark.sql.session.timeZone", time_zone)


def init_runtime_session(name="dsgrid", check_env=True, spark_conf=None) -> Any:
    """Initialize the runtime session."""
    if use_duckdb():
        logger.info("Using DuckDB as the backend engine.")
        return _DUCKDB_RUNTIME_SESSION

    return _SparkRuntimeSession(_create_spark_session(name, check_env, spark_conf))


def _create_spark_session(name="dsgrid", check_env=True, spark_conf=None) -> Any:
    """Initialize and return the raw SparkSession."""
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

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    with disable_console_logging():
        log_runtime_conf(spark)
        logger.info("Custom configuration settings: %s", spark_conf)

    return spark


def log_runtime_conf(spark: Any):
    """Log the Spark configuration details."""
    if not use_duckdb():
        conf = spark.sparkContext.getConf().getAll()
        conf.sort(key=lambda x: x[0])
        logger.info("Spark conf: %s", "\n".join([f"{x} = {y}" for x, y in conf]))


def get_active_session(*args) -> Any:
    """Return the active runtime session."""
    return get_duckdb_runtime_session() or init_runtime_session(*args)


def restart_runtime_session(*args, force=False, **kwargs) -> Any:
    """Restart the Spark runtime session with new config parameters.

    DuckDB sessions are reused because there is no Spark session to restart.

    Parameters
    ----------
    force : bool
        If True, restart the session even if the config parameters haven't changed.
        You might want to do this in order to clear cached tables or start Spark fresh.

    Returns
    -------
    runtime session

    """
    session = get_duckdb_runtime_session()
    if session is not None:
        return session

    session = get_spark_session()
    if session is None:
        return init_runtime_session(*args, **kwargs)
    needs_restart = force
    orig_time_zone = session.conf.get("spark.sql.session.timeZone")
    conf = kwargs.get("spark_conf", {})
    new_time_zone = conf.get("spark.sql.session.timeZone", orig_time_zone)

    if not force:
        for key, val in conf.items():
            current = session.conf.get(key, None)
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
        session.stop()
        logger.info("Stopped the SparkSession so that it can be restarted with a new config.")
        session = _create_spark_session(*args, **kwargs)
        if session.conf.get("spark.sql.session.timeZone") != new_time_zone:
            # We set this value in query_submitter.py and that change will get lost
            # when the session is restarted.
            session.conf.set("spark.sql.session.timeZone", new_time_zone)
    else:
        logger.info("No restart of Spark is needed.")

    return _SparkRuntimeSession(session)


@track_timing(timer_stats_collector)
def create_dataframe(records, table_name=None, require_unique=None) -> ibis.Table:
    """Create a table from a list of records.

    Parameters
    ----------
    records : list
        list of row-like objects
    table_name : str | None
        If set, cache the Ibis table in memory with this name. Must be unique.
    require_unique : list
        list of column names (str) to check for uniqueness
    """
    df = get_runtime_session().createDataFrame(records)
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


@track_timing(timer_stats_collector)
def create_dataframe_from_ids(ids: Iterable[str], column: str) -> ibis.Table:
    """Create a table from a list of dimension IDs."""
    struct_type = cast(Any, StructType)
    struct_field = cast(Any, StructField)
    string_type = cast(Any, StringType)
    schema = struct_type([struct_field(column, string_type())])
    return get_runtime_session().createDataFrame([[x] for x in ids], schema)


def create_dataframe_from_pandas(df):
    """Create a table from a pandas DataFrame."""
    return get_runtime_session().createDataFrame(df)


def create_dataframe_from_dicts(records: list[dict[str, Any]]) -> ibis.Table:
    """Create a table from a list of dictionaries.

    This avoids static-analysis issues around runtime-specific createDataFrame overloads.
    """
    if not records:
        msg = "records cannot be empty in create_dataframe_from_dicts"
        raise DSGInvalidParameter(msg)

    data = [tuple(row.values()) for row in records]
    columns = list(records[0].keys())
    return get_runtime_session().createDataFrame(data, columns)


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
    ibis.Table | None
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
    read_with_runtime: bool = True,
) -> ibis.Table:
    """Create a table from a file.

    Supported formats when read_with_runtime=True: .csv, .json, .parquet
    Supported formats when read_with_runtime=False: .csv, .json

    When reading CSV files on AWS read_with_runtime should be set to False because the
    files would need to be present on local storage for all workers. The master node
    will sync the config files from S3, read them with standard filesystem system calls,
    and then convert the data to Ibis tables.

    Parameters
    ----------
    filename : str | Path
        path to file
    table_name : str | None
        If set, cache the Ibis table in memory. Must be unique.
    require_unique : list
        list of column names (str) to check for uniqueness
    read_with_runtime : bool
        If True, read the file with the active Ibis backend. Otherwise, read the file
        natively in Python and then convert it to an Ibis table.

    Returns
    -------
    ibis.Table

    Raises
    ------
    ValueError
        Raised if a require_unique column has duplicate values.
    DSGInvalidFile
        Raised if the file cannot be read. This can happen if a Parquet write operation fails.

    """
    func = _read_with_runtime if read_with_runtime else _read_natively
    df = func(str(filename))
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


def _read_with_runtime(filename):
    if not os.path.exists(filename):
        msg = f"{filename} does not exist"
        raise FileNotFoundError(msg)
    suffix = Path(filename).suffix
    if suffix == ".csv":
        df = read_csv(filename)
    elif suffix == ".parquet":
        try:
            df = read_parquet(filename)
        except Exception as exc:
            if _is_spark_parquet_schema_exception(exc) or _is_duckdb_io_exception(exc):
                logger.exception("Failed to read Parquet file=%s. File may be invalid", filename)
                msg = f"Cannot read {filename =}"
                raise DSGInvalidFile(msg)
            else:
                raise

    elif suffix == ".json":
        df = read_json(filename)
    else:
        assert False, f"Unsupported file extension: {filename}"
    return df


def _is_duckdb_io_exception(exc: Exception) -> bool:
    cls = exc.__class__
    return cls.__name__ == "IOException" and cls.__module__.startswith("duckdb")


def _is_spark_parquet_schema_exception(exc: Exception) -> bool:
    message = str(exc)
    return exc.__class__.__name__ == "AnalysisException" and (
        "Unable to infer schema for Parquet. It must be specified manually." in message
        or "PATH_NOT_FOUND" in message
        or "Path does not exist" in message
    )


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
    return get_runtime_session().createDataFrame(obj)


def _post_process_dataframe(df, table_name=None, require_unique=None):
    if table_name is not None:
        make_runtime_backend().create_view(table_name, df)

    if require_unique is not None:
        with Timer(timer_stats_collector, "check_unique"):
            for column in require_unique:
                unique = df.select(column).distinct()
                if _table_count(unique) != _table_count(df):
                    msg = f"Ibis table has duplicate entries for {column}"
                    raise DSGInvalidField(msg)


def cross_join_dfs(dfs: list[ibis.Table]) -> ibis.Table:
    """Perform a cross join of all tables in dfs."""
    if len(dfs) == 1:
        return dfs[0]

    df = dfs[0]
    for other in dfs[1:]:
        df = cross_join(df, other)
    return df


@track_timing(timer_stats_collector)
def models_to_dataframe(models: list[DSGBaseModel], table_name: str | None = None) -> ibis.Table:
    """Converts a list of Pydantic models to a table.

    Parameters
    ----------
    models : list
    table_name : str | None
        If set, a unique ID to use as the cached table name. Return from cache if already stored.
    """
    session = get_runtime_session()
    if table_name is not None and make_runtime_backend().has_table(table_name):
        return make_runtime_backend().table(table_name)

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
                python_type = cast(type[Any], python_type)
                spark_type = PYTHON_TO_SPARK_TYPES[python_type]()
                struct_fields.append(StructField(f, spark_type, nullable=True))
            dct[f] = val
        rows.append(tuple(dct.values()))

    schema: Any = StructType(struct_fields)
    df = session.createDataFrame(rows, schema=schema)

    if table_name is not None:
        make_runtime_backend().create_view(table_name, df)

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
            msg = f"Unhandled Union type: {python_type =} {args =}"
            raise NotImplementedError(msg)
        elif len(types) > 1:
            msg = f"Unhandled Union type: {types =}"
            raise NotImplementedError(msg)
        else:
            python_type = types[0]

    return python_type


@track_timing(timer_stats_collector)
def create_dataframe_from_dimension_ids(records, *dimension_types, cache=True) -> ibis.Table:
    """Return an Ibis table created from the IDs of dimension_types.

    Parameters
    ----------
    records : sequence
        Iterable of lists of record IDs
    dimension_types : tuple
    cache : If True, cache the Ibis table.
    """
    struct_type = cast(Any, StructType)
    string_type = cast(Any, StringType)
    schema = struct_type()
    for dimension_type in dimension_types:
        schema.add(dimension_type.value, string_type(), nullable=False)
    df = get_runtime_session().createDataFrame(records, schema=schema)
    return df


@track_timing(timer_stats_collector)
def check_for_nulls(df, exclude_columns=None):
    """Check if an Ibis table has null values.

    Parameters
    ----------
    df : ibis.Table
    exclude_columns : None or Set

    Raises
    ------
    DSGInvalidField
        Raised if null exists in any column.

    """
    if exclude_columns is None:
        exclude_columns = set()
    cols_to_check = set(df.columns).difference(exclude_columns)
    if not cols_to_check:
        return
    view = create_temp_view(df)
    quote = '"'
    cols_str = ", ".join(f"{quote}{x}{quote}" for x in cols_to_check)
    filter_str = " OR ".join((f"{quote}{x}{quote} IS NULL" for x in cols_to_check))

    try:
        # Avoid iterating with many checks unless we know there is at least one failure.
        nulls = sql(f"SELECT {cols_str} FROM {view} WHERE {filter_str}")
        if not is_table_empty(nulls):
            cols_with_null = set()
            for col in cols_to_check:
                col_nulls = sql(
                    f"SELECT {quote}{col}{quote} FROM {view} "
                    f"WHERE {quote}{col}{quote} IS NULL LIMIT 1"
                )
                if not is_table_empty(col_nulls):
                    cols_with_null.add(col)
            assert cols_with_null, "Did not find any columns with NULL values"

            msg = f"Ibis table contains NULL value(s) for column(s): {cols_with_null}"
            raise DSGInvalidField(msg)
    finally:
        conn = cast(Any, make_runtime_backend().connection)
        conn.raw_sql(f"DROP VIEW IF EXISTS {view}")


@track_timing(timer_stats_collector)
def overwrite_dataframe_file(filename: Path | str, df: ibis.Table) -> ibis.Table:
    """Perform an in-place overwrite of a table, accounting for different file types
    and symlinks.

    Do not attempt to access the original dataframe unless it was fully cached.
    """
    path = Path(filename)
    suffix = path.suffix
    tmp = str(path) + ".tmp"
    tmp_posix = Path(tmp).as_posix()
    if suffix == ".parquet":
        _write_table(df, tmp_posix, "parquet")
        read_method = read_parquet
    elif suffix == ".csv":
        _write_table(df, tmp_posix, "csv")
        read_method = read_csv
    elif suffix == ".json":
        _write_table(df, tmp_posix, "json")
        read_method = read_json
    else:
        msg = f"Unsupported file suffix: {suffix}"
        raise NotImplementedError(msg)
    delete_if_exists(filename)
    os.rename(tmp, str(path))
    return read_method(path.as_posix())


@track_timing(timer_stats_collector)
def persist_intermediate_query(
    df: ibis.Table, scratch_dir_context: ScratchDirContext, auto_partition=False
) -> ibis.Table:
    """Persist the current query to files and then read it back and return it.

    This is advised when the query has become too complex or when the query might be evaluated
    twice.

    Parameters
    ----------
    df : Ibis table
    scratch_dir_context : ScratchDirContext
    auto_partition : bool
        If True, call write_dataframe_and_auto_partition.

    Returns
    -------
    Ibis table
    """
    tmp_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    if auto_partition:
        return write_dataframe_and_auto_partition(df, tmp_file)
    _write_table(df, tmp_file.as_posix(), "parquet")
    return read_parquet(tmp_file.as_posix())


@track_timing(timer_stats_collector)
def write_dataframe_and_auto_partition(
    df: ibis.Table,
    filename: Path,
    partition_size_mb: int = MAX_PARTITION_SIZE_MB,
    columns: list[str] | None = None,
    rtol_pct: float = 50,
    min_num_partitions: int = 36,
) -> ibis.Table:
    """Write a dataframe to a Parquet file and then automatically coalesce or repartition it if
    needed. If the file already exists, it will be overwritten.

    Parameters
    ----------
    df : ibis.Table
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
        _write_table(df, Path(filename).as_posix(), "parquet")
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
def write_dataframe(df: ibis.Table, filename: str | Path, overwrite: bool = False) -> None:
    """Write a table, accounting for different file types.

    Parameters
    ----------
    filename : str
    df : ibis.Table
    """
    path = Path(filename)
    if overwrite:
        delete_if_exists(path)

    suffix = path.suffix
    name = path.as_posix()
    if suffix == ".parquet":
        _write_table(df, name, "parquet")
    elif suffix == ".csv":
        _write_table(df, name, "csv")
    elif suffix == ".json":
        if use_duckdb():
            new_name = name.replace(".json", ".parquet")
            _write_table(df, new_name, "parquet")
        else:
            _write_table(df, name, "json")


@track_timing(timer_stats_collector)
def persist_table(df: ibis.Table, context: ScratchDirContext, tag=None) -> Path:
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


def sql(query: str) -> ibis.Table:
    """Run a SQL query with the active Ibis backend."""
    logger.debug("Run SQL query [%s]", query)
    return get_runtime_session().sql(query)


def load_stored_table(table_name: str) -> ibis.Table:
    """Return a table stored in the Spark warehouse."""
    spark = get_runtime_session()
    return spark.table(table_name)


def try_load_stored_table(
    table_name: str, database: str | None = DSGRID_DB_NAME
) -> ibis.Table | None:
    """Return a table if it is stored in the Spark warehouse."""
    spark = get_runtime_session()
    full_name = f"{database}.{table_name}"
    if spark.catalog.tableExists(full_name):
        return spark.table(table_name)
    return None


def is_table_stored(table_name, database=DSGRID_DB_NAME):
    spark = get_runtime_session()
    full_name = f"{database}.{table_name}"
    return spark.catalog.tableExists(full_name)


def save_table(table, table_name, overwrite=True, database=DSGRID_DB_NAME):
    if use_duckdb():
        msg = "save_table is not supported when using DuckDB"
        raise DSGInvalidOperation(msg)

    full_name = f"{database}.{table_name}"
    view = create_temp_view(table)
    writer = get_spark_session().table(view).write
    if overwrite:
        writer.mode("overwrite").saveAsTable(full_name)
    else:
        writer.saveAsTable(full_name)


def list_tables(database=DSGRID_DB_NAME):
    spark = get_runtime_session()
    return [x.name for x in spark.catalog.listTables(dbName=database)]


def drop_table(table_name, database=DSGRID_DB_NAME):
    if is_table_stored(table_name, database=database):
        get_spark_session().sql(f"DROP TABLE {table_name}")
        logger.info("Dropped table %s", table_name)


@track_timing(timer_stats_collector)
def create_dataframe_from_product(
    data: dict[str, list[str]],
    context: ScratchDirContext,
    max_partition_size_mb=MAX_PARTITION_SIZE_MB,
) -> ibis.Table:
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
    # 3. Create one table per column and then cross-join all of them. Extremely slow.
    # 4. Create one pyarrow Table, write to temp Parquet, read back in Spark. ~2x slower
    #    than CSV implementaion.
    # 5. Create the joined table via SQLite and then read the contents into Spark with a JDBC
    #    driver. Much slower.

    # Note: This location must be accessible on all compute nodes.
    csv_dir = context.get_temp_filename(suffix=".csv")
    columns = list(data.keys())
    struct_type = cast(Any, StructType)
    struct_field = cast(Any, StructField)
    string_type = cast(Any, StringType)
    schema = struct_type([struct_field(x, string_type()) for x in columns])

    with CsvPartitionWriter(csv_dir, max_partition_size_mb=max_partition_size_mb) as writer:
        for row in itertools.product(*(data.values())):
            writer.add_row(row)

    session = get_runtime_session()
    if use_duckdb():
        df = session.read.csv(f"{csv_dir.as_posix()}/*.csv", header=False, schema=schema)
    else:
        df = session.read.csv(str(csv_dir), header=False, schema=schema)
    return df


def _spark_dataframe_to_ibis_table(df: Any) -> ibis.Table:
    view = make_temp_view_name()
    df.createOrReplaceTempView(view)
    return make_runtime_backend().table(view)


def _create_ibis_table(data: Any, schema: Any | None = None) -> ibis.Table:
    if isinstance(data, ibis.Table):
        return data
    if isinstance(data, pd.DataFrame):
        pdf = data.copy()
    else:
        rows = list(data)
        names = _schema_names(schema)
        if rows and isinstance(rows[0], dict):
            pdf = pd.DataFrame(rows)
        elif names:
            pdf = pd.DataFrame(rows, columns=cast(Any, names))
        else:
            pdf = pd.DataFrame(rows)
    ibis_schema = cast(Any, _schema_types(schema))
    conn = cast(Any, make_runtime_backend().connection)
    return conn.create_table(
        make_temp_view_name(),
        obj=pdf,
        schema=ibis_schema,
        overwrite=True,
    )


def _table_count(table: ibis.Table) -> int:
    count = cast(Any, table.count().execute())
    return int(count)


def _write_table(df: ibis.Table, path: str, file_format: str) -> None:
    view = create_temp_view(df)
    if not use_duckdb():
        writer = get_spark_session().table(view).write.mode("overwrite")
        if file_format == "parquet":
            writer.parquet(path)
        elif file_format == "csv":
            writer.option("header", True).csv(path)
        elif file_format == "json":
            writer.json(path)
        else:
            msg = f"Unsupported file format: {file_format}"
            raise NotImplementedError(msg)
        return

    escaped_path = path.replace("'", "''")
    conn = cast(Any, make_runtime_backend().connection)
    if file_format == "parquet":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT PARQUET)")
    elif file_format == "csv":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT CSV, HEADER)")
    elif file_format == "json":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT JSON)")
    else:
        msg = f"Unsupported file format: {file_format}"
        raise NotImplementedError(msg)


def _schema_names(schema: Any | None) -> list[str]:
    if schema is None:
        return []
    names = getattr(schema, "names", None)
    if callable(names):
        return list(names())
    if names is not None:
        return list(names)
    if isinstance(schema, list):
        return [str(x) for x in schema]
    try:
        return [field.name for field in schema]
    except TypeError:
        return []


def _schema_types(schema: Any | None, *, ibis_types: bool = True) -> dict[str, str] | None:
    if schema is None:
        return None
    if isinstance(schema, list) and not any(hasattr(field, "dataType") for field in schema):
        return None
    if isinstance(schema, dict):
        return schema
    types = {}
    for field in schema:
        name = getattr(field, "name", None)
        data_type = getattr(field, "dataType", None)
        if name is not None and data_type is not None:
            types[name] = (
                _ibis_type_from_spark_type(data_type)
                if ibis_types
                else _duckdb_type_from_spark_type(data_type)
            )
    return types or None


def _duckdb_type_from_spark_type(data_type: Any) -> str:
    match data_type.__class__.__name__:
        case "BooleanType":
            return "BOOLEAN"
        case "ByteType":
            return "TINYINT"
        case "ShortType":
            return "SMALLINT"
        case "IntegerType":
            return "INTEGER"
        case "LongType":
            return "BIGINT"
        case "FloatType":
            return "FLOAT"
        case "DoubleType":
            return "DOUBLE"
        case "StringType":
            return "VARCHAR"
        case "TimestampType" | "TimestampNTZType":
            return "TIMESTAMP"
        case _:
            msg = f"Unsupported schema data type: {data_type}"
            raise NotImplementedError(msg)


def _merge_spark_csv_schema(
    session: Any, path: str, schema: dict[str, str], kwargs: dict[str, Any]
):
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
        TimestampType,
    )

    def make_type(dtype: str):
        match dtype.upper():
            case "BOOLEAN":
                return BooleanType()
            case "TINYINT":
                return ByteType()
            case "SMALLINT":
                return ShortType()
            case "INT" | "INTEGER":
                return IntegerType()
            case "BIGINT":
                return LongType()
            case "FLOAT":
                return FloatType()
            case "DOUBLE":
                return DoubleType()
            case "STRING" | "TEXT" | "VARCHAR":
                return StringType()
            case "TIMESTAMP":
                return TimestampType()
            case "TIMESTAMP_NTZ":
                return TimestampNTZType()
            case _:
                msg = f"Unsupported Spark CSV schema data type: {dtype}"
                raise NotImplementedError(msg)

    inference_kwargs = dict(kwargs)
    inference_kwargs.pop("schema", None)
    inference_kwargs["inferSchema"] = True
    inferred = session.read.csv(path, **inference_kwargs).schema
    fields = [
        StructField(
            field.name,
            make_type(schema[field.name]) if field.name in schema else field.dataType,
            field.nullable,
            field.metadata,
        )
        for field in inferred
    ]
    return StructType(fields)


def _ibis_type_from_spark_type(data_type: Any) -> str:
    match data_type.__class__.__name__:
        case "BooleanType":
            return "boolean"
        case "ByteType":
            return "int8"
        case "ShortType":
            return "int16"
        case "IntegerType":
            return "int32"
        case "LongType":
            return "int64"
        case "FloatType":
            return "float32"
        case "DoubleType":
            return "float64"
        case "StringType":
            return "string"
        case "TimestampType" | "TimestampNTZType":
            return "timestamp"
        case _:
            msg = f"Unsupported schema data type: {data_type}"
            raise NotImplementedError(msg)


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
def custom_runtime_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.

    """
    spark = get_duckdb_runtime_session()
    if spark is not None:
        yield
        return

    spark = get_runtime_session()
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
        spark = get_runtime_session()
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
def restart_runtime_session_with_custom_conf(conf: dict, force=False):
    """Restart the SparkSession with a custom configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.
    force : bool
        If True, restart the session even if the config parameters haven't changed.
        You might want to do this in order to clear cached tables or start Spark fresh.
    """
    spark = get_duckdb_runtime_session()
    if spark is not None:
        yield spark
        return

    spark = get_runtime_session()
    app_name = spark.conf.get("spark.app.name")
    orig_settings = {}

    try:
        for name in conf:
            current = spark.conf.get(name, None)
            if current is not None:
                orig_settings[name] = current
        new_spark = restart_runtime_session(name=app_name, spark_conf=conf, force=force)
        yield new_spark
    finally:
        restart_runtime_session(name=app_name, spark_conf=orig_settings, force=force)


@contextmanager
def set_session_time_zone(time_zone: str) -> Generator[None, None, None]:
    """Set the session time zone for execution of a code block."""
    orig = get_current_time_zone()

    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        set_current_time_zone(orig)


def union(dfs: list[ibis.Table]) -> ibis.Table:
    """Return a union of the tables, ensuring that the columns match."""
    df = dfs[0]
    if len(dfs) > 1:
        for dft in dfs[1:]:
            if df.columns != dft.columns:
                msg = f"columns don't match: {df.columns =} {dft.columns =}"
                raise Exception(msg)
            df = df.union(dft)
    return df
