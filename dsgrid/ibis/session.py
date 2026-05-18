"""Runtime session and table IO helpers for Ibis-backed execution."""

import itertools
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, cast

import pandas as pd
import ibis

from dsgrid.exceptions import DSGInvalidOperation, DSGInvalidParameter
from dsgrid.ibis.backend import get_runtime_backend, invalidate_runtime_backend_cache
from dsgrid.ibis.operations import make_temp_view_name
from dsgrid.ibis.io import (
    CsvPartitionWriter,
    MAX_PARTITION_SIZE_MB,
    _post_process_dataframe,
    read_csv,
)
from dsgrid.ibis.types import (
    spec_for_name,
    spec_for_spark_sql,
    spec_for_spark_type,
    use_duckdb,
)
from dsgrid.loggers import disable_console_logging
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.timing import track_timing, timer_stats_collector

if not use_duckdb():
    # pyspark is an optional dependency (extras = "spark"). The DuckDB
    # shims below provide the same symbols when pyspark is absent at
    # runtime. The ty CI installs the "spark" extra so these imports
    # resolve during type-checking.
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
    from dsgrid.ibis._duckdb_shims import (  # noqa: F401
        F,
        AnalysisException,
        BooleanType,
        ByteType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        Row,
        ShortType,
        SparkConf,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
        TimestampType,
    )

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


logger = logging.getLogger(__name__)

_DUCKDB_RUNTIME_SESSION: Any = None

PYTHON_TO_SPARK_TYPES: dict[type[Any], Any] = {
    int: IntegerType,
    float: DoubleType,
    str: StringType,
    bool: BooleanType,
}


class _DuckDBReader:
    """Adapter so ``session.read.{csv,json,parquet}`` works on DuckDB.

    ``csv()`` always treats the first row as a header; the dsgrid schema
    model is name-based. Callers that previously passed ``header=False``
    must rewrite the file with a header.
    """

    def csv(self, path: str, schema: Any | None = None, **kwargs) -> ibis.Table:
        # Delegate to the consolidated reader. ``schema`` can be a
        # dict[str, str] (already in backend SQL strings) or a PySpark
        # StructType; ``_schema_types`` normalizes either to a dict.
        #
        # Spark-style option names are forwarded so callers using the
        # session-API shape (``session.read.csv(path, sep="|")``) get the
        # same behavior on both backends. Without this translation
        # ``delimiter``/``encoding``/``null_values`` were silently dropped
        # on DuckDB while the Spark adapter honored them.
        types = _schema_types(schema, ibis_types=False)
        delimiter = kwargs.get("sep") or kwargs.get("delim") or kwargs.get("delimiter")
        encoding = kwargs.get("encoding") or "utf-8"
        null_value = kwargs.get("nullValue") or kwargs.get("null_values")
        null_values: list[str] | None
        if null_value is None:
            null_values = None
        elif isinstance(null_value, str):
            null_values = [null_value]
        else:
            null_values = list(null_value)
        return read_csv(
            path,
            schema=types,
            encoding=encoding,
            delimiter=delimiter,
            null_values=null_values,
        )

    def json(self, path: str, **kwargs) -> ibis.Table:
        return get_runtime_backend().connection.read_json(path)

    def parquet(self, path: str, **kwargs) -> ibis.Table:
        return get_runtime_backend().connection.read_parquet(path)


class _SparkReader:
    """Adapter so ``session.read.{csv,json,parquet}`` works on Spark."""

    def __init__(self, session: Any):
        self._session = session

    def csv(self, path: str, **kwargs) -> ibis.Table:
        schema = kwargs.get("schema")
        if isinstance(schema, dict):
            kwargs = dict(kwargs)
            kwargs["schema"] = _merge_spark_csv_schema(self._session, path, schema, kwargs)
        # The consolidated read_csv always passes header=True; this branch
        # also defaults header to True if not provided.
        kwargs.setdefault("header", True)
        return _spark_dataframe_to_ibis_table(self._session.read.csv(path, **kwargs))

    def json(self, path: str, **kwargs) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.read.json(path, **kwargs))

    def parquet(self, path: str, **kwargs) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.read.parquet(path, **kwargs))


class _SparkRuntimeSession:
    """Backend-specific implementation of the small runtime-session API.

    The public surface that production code actually uses is just
    ``createDataFrame()``, ``sql()``, and ``read.{csv,json,parquet}()``;
    the ``.conf`` / ``.catalog`` / ``.sparkContext`` accessors that earlier
    versions exposed were vestigial PySpark-shape mimicry and have been
    removed along with the Spark-warehouse helpers that needed them. The
    ``raw_session`` property remains as the documented escape hatch for
    Spark-only callers (``_create_spark_session`` post-init checks,
    ``log_runtime_conf``, ``restart_runtime_session``) that need direct
    access to the underlying PySpark ``SparkSession``.
    """

    def __init__(self, session: Any):
        self._session = session
        self.read = _SparkReader(session)

    @property
    def raw_session(self) -> Any:
        return self._session

    def createDataFrame(self, data: Any, schema: Any | None = None) -> ibis.Table:
        return _spark_dataframe_to_ibis_table(self._session.createDataFrame(data, schema=schema))

    def sql(self, query: str, **kwargs) -> ibis.Table:
        if kwargs:
            return _spark_dataframe_to_ibis_table(self._session.sql(query, **kwargs))
        return get_runtime_backend().sql(query)

    def stop(self) -> None:
        # The cached Ibis backend in dsgrid.ibis.backend is bound to this
        # SparkSession; once we stop the session, returning the cached backend
        # would hand out a stopped reference. Drop it before stopping so the
        # next get_runtime_backend() call rebuilds against a fresh session.
        invalidate_runtime_backend_cache()
        self._session.stop()


class _DuckDBRuntimeSession:
    """DuckDB-side implementation of the small runtime-session API.

    Mirrors :class:`_SparkRuntimeSession` exactly so callers don't have to
    branch on backend. See that class's docstring for the public surface.
    """

    def __init__(self):
        self.read = _DuckDBReader()

    def createDataFrame(self, data: Any, schema: Any | None = None) -> ibis.Table:
        return _create_ibis_table(data, schema=schema)

    def sql(self, query: str, **kwargs) -> ibis.Table:
        if kwargs:
            msg = "DuckDB Ibis SQL does not support Spark keyword dataframe bindings"
            raise DSGInvalidOperation(msg)
        return get_runtime_backend().sql(query)


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

    if conf.get("spark.sql.session.timeZone") is None:
        conf.set("spark.sql.session.timeZone", "UTC")

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

    if cluster is None:
        _apply_local_mode_defaults(conf)

    if check_env and cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf.setMaster(cluster)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    requested_tz = conf.get("spark.sql.session.timeZone")
    if requested_tz is not None and spark.conf.get("spark.sql.session.timeZone") != requested_tz:
        spark.conf.set("spark.sql.session.timeZone", requested_tz)

    with disable_console_logging():
        log_runtime_conf(spark)
        logger.info("Custom configuration settings: %s", spark_conf)

    return spark


def _apply_local_mode_defaults(conf: Any) -> None:
    """Apply sensible defaults for Spark running in local mode.

    Spark's out-of-the-box ``spark.sql.shuffle.partitions=200`` is catastrophic for small
    local jobs. Worse, dsgrid's plans cascade several joins/aggregations, and observed
    task counts grow roughly as ``spark.default.parallelism ** (stage depth)`` — on a
    12-core machine we have seen a single stage produce 248 832 tasks whose per-task
    overhead dwarfs the real work. A very small default bounds the damage.

    Values already set on ``conf`` by the caller (via ``spark_conf=``) are preserved.
    If ``SPARK_CONF_DIR`` is set we skip entirely and defer to the user's
    ``spark-defaults.conf`` — ``SparkConf()`` does not pre-load that file, so we cannot
    detect per-key overrides from it and fall back to "user is in charge".

    Callers with heavier workloads should override via ``SPARK_CONF_DIR`` /
    ``spark-defaults.conf`` or the ``spark_conf=`` kwarg.
    """
    if os.environ.get("SPARK_CONF_DIR"):
        return
    defaults = {
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
    for key, val in defaults.items():
        if conf.get(key) is None:
            conf.set(key, val)


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
        # Drop the cached Ibis backend; the next get_runtime_backend() call
        # must build a fresh one bound to the new session below.
        invalidate_runtime_backend_cache()
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


def sql(query: str) -> ibis.Table:
    """Run a SQL query with the active Ibis backend."""
    logger.debug("Run SQL query [%s]", query)
    return get_runtime_session().sql(query)


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

    with CsvPartitionWriter(
        csv_dir,
        max_partition_size_mb=max_partition_size_mb,
        header=tuple(columns),
    ) as writer:
        for row in itertools.product(*(data.values())):
            writer.add_row(row)

    session = get_runtime_session()
    if use_duckdb():
        df = session.read.csv(f"{csv_dir.as_posix()}/*.csv", schema=schema)
    else:
        df = session.read.csv(str(csv_dir), schema=schema)
    return df


def _spark_dataframe_to_ibis_table(df: Any) -> ibis.Table:
    view = make_temp_view_name()
    df.createOrReplaceTempView(view)
    return get_runtime_backend().table(view)


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
    conn = cast(Any, get_runtime_backend().connection)
    return conn.create_table(
        make_temp_view_name(),
        obj=pdf,
        schema=ibis_schema,
        overwrite=True,
    )


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
    try:
        return spec_for_spark_type(data_type).duckdb_sql
    except KeyError as exc:
        raise NotImplementedError(str(exc)) from exc


def _merge_spark_csv_schema(
    session: Any, path: str, schema: dict[str, str], kwargs: dict[str, Any]
):
    # Lazy: only the Spark-runtime branch reaches this helper.
    import pyspark.sql.types as pyspark_types

    def make_type(dtype: str):
        try:
            spec = spec_for_spark_sql(dtype)
        except KeyError as exc:
            # Fall back to spec_for_name to also accept user-facing aliases
            # like INTEGER/TEXT/VARCHAR that map to canonical spec_sql values.
            try:
                spec = spec_for_name(dtype)
            except KeyError:
                msg = f"Unsupported Spark CSV schema data type: {dtype}"
                raise NotImplementedError(msg) from exc
        class_name = spec.spark_type_names[0] if spec.spark_type_names else None
        if class_name is None:
            # Alias spec (e.g. INTEGER) — resolve to its canonical sibling.
            class_name = spec_for_spark_sql(spec.spark_sql).spark_type_names[0]
        return getattr(pyspark_types, class_name)()

    # Try the cheap path first: read just the header line and assume the
    # user provided a type for every column. This avoids a full Spark
    # inferSchema scan (which previously doubled the read cost on large
    # files). If any header column is missing from the user schema, fall
    # back to inferSchema so those columns keep their inferred types.
    column_names = _read_csv_header_columns(path, kwargs)
    if column_names is not None and all(name in schema for name in column_names):
        fields = [
            pyspark_types.StructField(name, make_type(schema[name]), nullable=True)
            for name in column_names
        ]
        return pyspark_types.StructType(fields)

    inference_kwargs = dict(kwargs)
    inference_kwargs.pop("schema", None)
    inference_kwargs["inferSchema"] = True
    inferred = session.read.csv(path, **inference_kwargs).schema
    fields = [
        pyspark_types.StructField(
            field.name,
            make_type(schema[field.name]) if field.name in schema else field.dataType,
            field.nullable,
            field.metadata,
        )
        for field in inferred
    ]
    return pyspark_types.StructType(fields)


def _read_csv_header_columns(path: str, kwargs: dict[str, Any]) -> list[str] | None:
    """Return the column names from a CSV file's header line.

    Returns ``None`` for inputs we can't trivially peek at (e.g. a Spark
    directory of part files or paths with non-default options like a
    custom quote character); the caller falls back to ``inferSchema``.
    """
    file_path = Path(path)
    if not file_path.is_file():
        return None
    encoding = kwargs.get("encoding") or "utf-8"
    delimiter = kwargs.get("sep") or ","
    try:
        with open(file_path, encoding=encoding) as f_in:
            header_line = f_in.readline()
    except OSError:
        return None
    if not header_line:
        return None
    return [name.strip() for name in header_line.rstrip("\r\n").split(delimiter)]


def _ibis_type_from_spark_type(data_type: Any) -> str:
    try:
        spec = spec_for_spark_type(data_type)
    except KeyError as exc:
        raise NotImplementedError(str(exc)) from exc
    # Strip any tz-info suffix from the dtype string (TIMESTAMP_TZ maps to
    # "timestamp('UTC')" for declared-cast purposes, but the inferred dtype
    # for an existing Spark column has no tz attached).
    return spec.ibis_dtype.split("(", 1)[0]


@contextmanager
def custom_runtime_conf(conf):
    """Apply a custom Spark configuration for the duration of a code block.

    Parameters
    ----------
    conf : dict
        Key-value pairs to set on the spark configuration.

    """
    if get_duckdb_runtime_session() is not None:
        yield
        return

    # Spark-only: route through the raw SparkSession because the runtime
    # session wrapper no longer exposes ``.conf``. The wrapper used to mimic
    # PySpark's surface, but now only Spark-specific lifecycle code needs
    # the conf API, so we read it directly from the underlying session.
    spark = get_spark_session()
    orig_settings = {}

    try:
        for key, val in conf.items():
            orig_settings[key] = spark.conf.get(key)
            spark.conf.set(key, val)
            logger.info("Set %s=%s temporarily", key, val)
        yield
    finally:
        # User code may have restarted the session; pick up the current one.
        spark = get_spark_session()
        for key, val in orig_settings.items():
            spark.conf.set(key, val)


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
    duckdb_session = get_duckdb_runtime_session()
    if duckdb_session is not None:
        yield duckdb_session
        return

    # Spark-only: route through the raw SparkSession (the runtime session
    # wrapper no longer mirrors ``.conf``).
    spark = get_spark_session()
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


