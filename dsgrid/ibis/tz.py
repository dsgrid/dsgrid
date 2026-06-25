"""Time-zone helpers for the runtime session.

Wraps PySpark's ``spark.sql.session.timeZone`` (Spark backend) and
DuckDB's connection ``TimeZone`` setting (DuckDB backend) under a
single render-TZ API.

The two implementations are not symmetric, but they converge on the
same observable semantic when used correctly:

- **Spark** stores ``TIMESTAMP`` values as UTC microseconds and
  re-renders them through the session TZ on column extractions like
  ``.year()`` / ``.hour()``. Setting the session TZ via this module
  always affects those extractions.
- **DuckDB** distinguishes between TZ-naive ``TIMESTAMP`` and
  TZ-aware ``TIMESTAMP WITH TIME ZONE`` (``TIMESTAMPTZ``). The
  connection ``TimeZone`` setting only affects extractions on
  ``TIMESTAMPTZ`` columns. **Extractions on plain ``TIMESTAMP``
  columns are TZ-naive on DuckDB regardless of the connection TZ.**

So the contract for callers of :func:`custom_time_zone` is: the columns
you extract from inside the context must be ``TIMESTAMPTZ`` on DuckDB
for the requested TZ to take effect. If you read timestamps from a
TZ-aware source (chronify, declared TIMESTAMP_TZ schemas, ``datetime``
objects with ``tzinfo``), this happens automatically. Plain
string-parsed timestamps without TZ info will silently ignore the
context manager on DuckDB.
"""

from contextlib import contextmanager
from typing import Any, Generator, cast

import ibis
from ibis.expr.datatypes import Timestamp

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.session import get_spark_session
from dsgrid.ibis.types import use_duckdb


def assert_tz_aware_extraction(column: ibis.Column) -> None:
    """Fail loudly if ``column`` would extract TZ-naively under :func:`custom_time_zone`.

    On DuckDB, ``.year()`` / ``.hour()`` honor the connection ``TimeZone`` only for
    ``TIMESTAMP WITH TIME ZONE`` columns; a naive ``TIMESTAMP`` is extracted TZ-naively
    and silently ignores :func:`custom_time_zone`. Call this immediately before a
    TZ-sensitive extraction so that feeding it a naive column fails loudly instead of
    producing wrong results.

    This is a no-op on Spark, whose ``TIMESTAMP`` carries no naive/aware distinction and
    always renders extractions via the session time zone.

    Parameters
    ----------
    column : ibis.Column
        The timestamp column about to be extracted from inside a ``custom_time_zone`` block.

    Raises
    ------
    DSGInvalidOperation
        On DuckDB, if ``column`` is not a TZ-aware timestamp.
    """
    if not use_duckdb():
        return
    dtype = column.type()
    if not isinstance(dtype, Timestamp) or dtype.timezone is None:
        msg = (
            f"Column {column.get_name()!r} has type {dtype}, but a TZ-aware timestamp is "
            "required: extractions like .year()/.hour() on a naive DuckDB TIMESTAMP silently "
            "ignore custom_time_zone. Cast to timestamp('<tz>') or read from a TZ-aware source."
        )
        raise DSGInvalidOperation(msg)


def get_current_time_zone() -> str:
    """Return the current time zone of the runtime session."""
    if use_duckdb():
        conn = cast(Any, get_runtime_backend().connection)
        result = conn.raw_sql("SELECT value FROM duckdb_settings() WHERE name = 'TimeZone'")
        row = result.fetchone()
        assert row is not None
        return row[0]

    # Spark: read directly from the raw SparkSession. The runtime-session
    # wrapper no longer mirrors ``.conf``; only Spark-specific lifecycle
    # code needs the conf API and it has always referenced the raw session.
    tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    assert tz is not None
    return tz


def set_current_time_zone(time_zone: str) -> None:
    """Set the current time zone of the runtime session."""
    if use_duckdb():
        escaped = time_zone.replace("'", "''")
        conn = cast(Any, get_runtime_backend().connection)
        conn.raw_sql(f"SET TimeZone='{escaped}'")
        return

    get_spark_session().conf.set("spark.sql.session.timeZone", time_zone)


@contextmanager
def custom_time_zone(time_zone: str) -> Generator[None, None, None]:
    """Apply a custom time zone for the duration of a code block."""
    orig_time_zone = get_current_time_zone()
    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        # The user code may have restarted the runtime session; resolve the
        # current session inside set_current_time_zone rather than capturing
        # the original reference.
        set_current_time_zone(orig_time_zone)
