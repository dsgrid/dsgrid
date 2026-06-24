"""I/O helpers for the runtime backend.

Read/write helpers for CSV, JSON, and Parquet files that route through
the configured runtime backend (DuckDB or Spark). The low-level readers
(:func:`read_csv`, :func:`read_json`, :func:`read_parquet`) stay free of
``dsgrid.ibis.session`` imports so this module can be imported during
session bootstrap. Higher-level helpers (:func:`read_dataframe`,
:func:`write_dataframe`, :func:`write_table`, etc.) lazily import the
small set of runtime-session symbols they need, which avoids a circular
import with :mod:`dsgrid.ibis.session`.
"""

import logging
import math
import os
import shutil
import time
from pathlib import Path
from typing import Any, cast

import ibis

from dsgrid.exceptions import DSGInvalidField, DSGInvalidFile, DSGInvalidParameter
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.operations import coalesce, create_temp_view, repartition
from dsgrid.ibis.table_utils import count_rows
from dsgrid.ibis.types import spec_for_name, spec_for_spark_sql, use_duckdb
from dsgrid.utils.files import delete_if_exists
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.timing import Timer, timer_stats_collector, track_timing

# NOTE: ``dsgrid.ibis.session`` imports this module at module top, so
# ``from dsgrid.ibis.session import ...`` at module top creates an
# unbreakable circular import. The call sites in this file resolve session
# symbols lazily inside functions for that reason; do NOT hoist them.

logger = logging.getLogger(__name__)

MAX_PARTITION_SIZE_MB = 128


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


def read_csv(
    path: Path | str,
    *,
    schema: dict[str, str] | None = None,
    encoding: str = "utf-8",
    delimiter: str | None = None,
    null_values: list[str] | None = None,
) -> ibis.Table:
    """Return an Ibis table from a CSV file or a directory of CSV files.

    The file's first row is always treated as the header — dsgrid's
    column model is name-based and cannot work with positional column
    data. CSVs without a header must be rewritten with one before being
    read.

    Parameters
    ----------
    path : Path or str
        Path to a single CSV file or to a directory of part files (the
        usual Spark output shape).
    schema : dict[str, str], optional
        Mapping of column name to backend SQL type string. Columns not in
        the dict are inferred from the data on both backends.
    encoding : str, optional
        File encoding. Defaults to ``"utf-8"``.
    delimiter : str, optional
        Field delimiter. Passed through to the backend reader; ``None``
        uses the backend default (typically ``,``).
    null_values : list[str], optional
        Strings to interpret as NULL. Spark only honors the first entry;
        DuckDB accepts the full list.

    Returns
    -------
    ibis.Table
    """
    path = Path(path)
    # Spark's read.csv handles a directory natively (reads all CSVs in it);
    # DuckDB's read_csv requires an explicit glob. Match read_parquet's pattern
    # so write_csv -> read_csv round-trips on both backends.
    if path.is_dir() and use_duckdb():
        path_str = path.as_posix() + "/**/*.csv"
    else:
        path_str = path.as_posix()

    if use_duckdb():
        # DuckDB read_csv does not expose an encoding parameter — it always
        # reads as UTF-8 (with BOM detection). Callers that need a
        # different encoding must pre-convert the file.
        if encoding.lower() not in {"utf-8", "utf8"}:
            msg = (
                f"DuckDB backend only supports UTF-8 CSV input; got encoding={encoding!r}. "
                "Re-encode the file to UTF-8 before reading."
            )
            raise DSGInvalidParameter(msg)
        conn = get_runtime_backend().connection
        kwargs: dict[str, Any] = {"header": True}
        if schema:
            kwargs["types"] = schema
        # When no schema is declared, let DuckDB infer column types from the
        # data (its default behavior). This matches user expectations that a
        # numeric column reads back as numeric. Columns whose inferred type is
        # wrong (e.g. leading-zero FIPS-style IDs sniffed as integers) must be
        # pinned via a declared schema.
        if delimiter is not None:
            kwargs["delim"] = delimiter
        if null_values:
            # Empty list = "no override" — same shape as ``None``. DuckDB's
            # ``nullstr`` accepts a list; ``[]`` would mean "no string is
            # NULL" which is not what callers passing an empty list mean.
            kwargs["nullstr"] = null_values
        return conn.read_csv(path_str, **kwargs)

    # Spark: route through the runtime session reader so the dict-schema
    # branch in _SparkReader.csv can translate to a PySpark StructType.
    # Lazy: session.py imports this module at module top.
    from dsgrid.ibis.session import get_runtime_session

    spark_kwargs: dict[str, Any] = {"header": True, "encoding": encoding}
    if schema:
        spark_kwargs["schema"] = schema
    else:
        # No declared schema: infer column types from the data so Spark
        # matches DuckDB's default behavior (a numeric column reads back as
        # numeric, not string). Columns whose inferred type is wrong (e.g.
        # leading-zero FIPS-style IDs sniffed as integers) must be pinned via
        # a declared schema. Partial schemas take the inference path through
        # _merge_spark_csv_schema instead.
        spark_kwargs["inferSchema"] = True
    if delimiter is not None:
        spark_kwargs["sep"] = delimiter
    if null_values:
        # Empty list = "no override"; Spark's ``nullValue`` takes a single
        # string, so we take the first entry only (documented in the
        # function's null_values param).
        spark_kwargs["nullValue"] = (
            null_values[0] if isinstance(null_values, list) else null_values
        )
    return get_runtime_session().read.csv(path_str, **spark_kwargs)


def read_json(path: Path | str) -> ibis.Table:
    """Return an Ibis table from a JSON file."""
    return get_runtime_backend().connection.read_json(str(path))


def read_parquet(path: Path | str) -> ibis.Table:
    path = Path(path) if isinstance(path, str) else path
    path_str = (
        path.as_posix()
        if path.is_file() or not use_duckdb()
        else f"{path.as_posix()}/**/*.parquet"
    )
    return get_runtime_backend().connection.read_parquet(path_str)


def try_read_dataframe(filename: Path, delete_if_invalid: bool = True, **kwargs):
    """Read a regenerable cache file, returning None on a miss instead of raising.

    Used for dsgrid's hash-keyed on-disk caches (cached datasets, intermediate
    query tables): callers treat ``None`` as a cache miss and regenerate, so a
    missing and an unreadable file are handled the same way. ``delete_if_invalid``
    defaults to True because an invalid cache file is almost always a partial or
    corrupt write; deleting it lets the caller rebuild cleanly. Point this only at
    regenerable caches, not source data — pass ``delete_if_invalid=False`` to skip
    the delete.

    Parameters
    ----------
    filename : Path
        Path to a cache file (or directory of part files).
    delete_if_invalid : bool, optional
        Delete ``filename`` when it exists but cannot be read. Defaults to True.
    kwargs
        Forwarded to :func:`read_dataframe`.

    Returns
    -------
    ibis.Table | None
        The table, or ``None`` if ``filename`` does not exist or is invalid.
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
    require_unique: list[str] | None = None,
) -> ibis.Table:
    """Create a table from a file.

    Supported formats: .csv, .json, .parquet.

    Parameters
    ----------
    filename : str | Path
        path to file
    table_name : str | None
        If set, cache the Ibis table in memory. Must be unique.
    require_unique : list[str] | None
        Column names to check for uniqueness; a duplicate value in any of them
        raises. None (the default) skips the check.

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
    df = _read_with_runtime(str(filename))
    _post_process_dataframe(df, table_name=table_name, require_unique=require_unique)
    return df


def _read_with_runtime(filename: str) -> ibis.Table:
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
                msg = f"Cannot read {filename=}"
                raise DSGInvalidFile(msg)
            else:
                raise
    elif suffix == ".json":
        df = read_json(filename)
    else:
        msg = f"Unsupported file extension: {filename}"
        raise NotImplementedError(msg)
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


def _post_process_dataframe(
    df, table_name: str | None = None, require_unique: list[str] | None = None
) -> None:
    if table_name is not None:
        get_runtime_backend().create_view(table_name, df)

    if require_unique is not None:
        with Timer(timer_stats_collector, "check_unique"):
            for column in require_unique:
                unique = df.select(column).distinct()
                if count_rows(unique) != count_rows(df):
                    msg = f"Ibis table has duplicate entries for {column}"
                    raise DSGInvalidField(msg)


@track_timing(timer_stats_collector)
def overwrite_dataframe_file(filename: Path | str, df: ibis.Table) -> ibis.Table:
    """Perform an in-place overwrite of a table, accounting for different file types
    and symlinks.

    Writes to a sibling ``.tmp`` path first, then swaps it into place via an
    intermediate ``.stale`` sibling rename so a concurrent reader sees either
    the old contents (during the write) or the new contents (after the swap) —
    never a missing path. The pre-fix sequence (``delete_if_exists`` then
    ``move``) opened a window where ``path`` did not exist, surfacing as
    ``FileNotFoundError`` to any reader entering at the wrong instant.

    On the single-file DuckDB path we use ``os.replace`` directly because it
    is POSIX-atomic and avoids the rename dance entirely. On the Spark
    directory-of-files path, ``shutil.move`` handles the cross-filesystem
    ``EXDEV`` case (relevant when ``path`` is a symlink targeting another
    filesystem); the directory swap is not atomic, but ``path`` always
    references something readable.

    Do not attempt to access the original dataframe unless it was fully cached.
    """
    path = Path(filename)
    suffix = path.suffix
    tmp = path.with_name(path.name + ".tmp")
    stale = path.with_name(path.name + ".stale")
    if suffix == ".parquet":
        write_table(df, tmp.as_posix(), "parquet")
        read_method = read_parquet
    elif suffix == ".csv":
        write_table(df, tmp.as_posix(), "csv")
        read_method = read_csv
    elif suffix == ".json":
        write_table(df, tmp.as_posix(), "json")
        read_method = read_json
    else:
        msg = f"Unsupported file suffix: {suffix}"
        raise NotImplementedError(msg)
    # A prior crashed call may have left a .stale sibling lying around; clear
    # it before the rename so the os.rename below cannot collide.
    delete_if_exists(stale)
    if tmp.is_dir():
        # Spark distributed write produces a directory of part files. Step
        # through stale so path is never missing for a concurrent reader.
        if path.exists():
            os.rename(str(path), str(stale))
        try:
            shutil.move(str(tmp), str(path))
        except Exception:
            # Roll the prior contents back into place so a failed swap does
            # not strand callers with a missing path.
            if stale.exists() and not path.exists():
                os.rename(str(stale), str(path))
            raise
        delete_if_exists(stale)
    else:
        # DuckDB single-file output. os.replace atomically overwrites path in
        # place when src and dst share a filesystem (tmp is always a sibling
        # of path), so the readable window is never broken.
        os.replace(str(tmp), str(path))
    return read_method(path.as_posix())


@track_timing(timer_stats_collector)
def persist_and_reload_table(
    df: ibis.Table, scratch_dir_context: ScratchDirContext, auto_partition: bool = False
) -> ibis.Table:
    """Persist a table to a scratch file and reload it, returning the re-read table.

    The reloaded table reads from the scratch parquet, so it no longer carries the
    upstream lazy lineage — use it to checkpoint a table that has grown too complex
    or would otherwise be evaluated more than once (e.g. during query execution).

    Unlike :func:`persist_table` (persist only, returns the path) this returns the
    table. Unlike :func:`write_dataframe_and_auto_partition` (which it builds on) the
    caller doesn't manage the filename — it goes to a scratch temp path — and unlike
    the query cache (``_persist_intermediate_result``) the file is ephemeral, not a
    hash-addressed cross-run cache.

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
    write_table(df, tmp_file.as_posix(), "parquet")
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

    .. note::

       Partitioning is a Spark-only concept here. On DuckDB the function
       still performs the initial Parquet write but the partition-count
       tuning is a no-op (DuckDB writes a single file by default). Callers
       that rely on a specific on-disk partition layout should expect that
       layout only when the runtime backend is Spark.

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
        write_table(df, Path(filename).as_posix(), "parquet")
        df = read_parquet(filename)

    end_initial_write = time.time()
    duration_first_write = end_initial_write - start_initial_write

    if use_duckdb():
        logger.debug("write_dataframe_and_auto_partition is not optimized for DuckDB")
        return df

    # Count the Parquet part files INSIDE the Spark output directory, not
    # its siblings. ``filename`` here is e.g.
    # ``/path/table.parquet/part-00000-….parquet`` (a directory containing
    # part files). The previous ``filename.parent.iterdir()`` counted other
    # files in ``/path/`` instead, which is usually 1 and made this branch
    # always early-return — silently skipping coalescing/repartitioning.
    num_partitions = len(list(filename.glob("*.parquet")))
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
        df = coalesce(df, desired)
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
        # ``df`` is an Ibis table, not a PySpark DataFrame, so the
        # ``repartition`` helper in operations.py runs the temp-view dance
        # equivalent to ``coalesce``. ``columns or ()`` keeps the call
        # shape uniform for the no-columns case.
        df = repartition(df, desired, *(columns or ()))
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
        write_table(df, name, "parquet")
    elif suffix == ".csv":
        write_table(df, name, "csv")
    elif suffix == ".json":
        if use_duckdb():
            new_name = name.replace(".json", ".parquet")
            write_table(df, new_name, "parquet")
        else:
            write_table(df, name, "json")


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


def write_table(df: ibis.Table, path: str, file_format: str) -> None:
    """Write a table to ``path`` in the backend's native shape.

    On Spark, the output is a directory of part files (Spark's distributed
    write). On DuckDB, the output is a single file via ``COPY ... TO``.
    :func:`read_parquet` / :func:`read_csv` / :func:`read_json` transparently
    read either shape.

    Partitioning (e.g. Hive-style ``PARTITION_BY`` directories) is not
    handled here — neither backend writes partitioned output through this
    helper. Spark's ``writer.partitionBy(...)`` and DuckDB's
    ``COPY ... (FORMAT PARQUET, PARTITION_BY (col))`` are intentionally
    out of scope; callers that need them should issue the SQL directly.
    """
    view = create_temp_view(df)
    if not use_duckdb():
        # Lazy import: session.py imports this module during bootstrap.
        from dsgrid.ibis.session import get_spark_session

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
    conn = cast(Any, get_runtime_backend().connection)
    if file_format == "parquet":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT PARQUET)")
    elif file_format == "csv":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT CSV, HEADER)")
    elif file_format == "json":
        conn.raw_sql(f"COPY (SELECT * FROM {view}) TO '{escaped_path}' (FORMAT JSON)")
    else:
        msg = f"Unsupported file format: {file_format}"
        raise NotImplementedError(msg)


class CsvPartitionWriter:
    """Writes dataframe rows to partitioned CSV files.

    Each part file starts with the optional ``header`` row so the result
    is readable by :func:`read_csv` (which requires a header — dsgrid's
    schema model is column-name based).
    """

    def __init__(
        self,
        directory: Path,
        max_partition_size_mb: int = MAX_PARTITION_SIZE_MB,
        header: tuple[str, ...] | None = None,
    ):
        self._directory = directory
        self._directory.mkdir(exist_ok=True)
        self._max_size = max_partition_size_mb * 1024 * 1024
        self._size = 0
        self._index = 1
        self._fp = None
        self._header = header

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
            if self._header is not None:
                self._fp.write(",".join(self._header))
                self._fp.write("\n")
        self._size += self._fp.write(line)
        self._size += self._fp.write("\n")
        if self._size >= self._max_size:
            self._fp.close()
            self._fp = None
            self._size = 0
            self._index += 1
