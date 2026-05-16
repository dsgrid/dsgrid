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

So the contract for callers of :func:`custom_time_zone` /
:func:`set_session_time_zone` is: the columns you extract from inside
the context must be ``TIMESTAMPTZ`` on DuckDB for the requested TZ to
take effect. If you read timestamps from a TZ-aware source (chronify,
declared TIMESTAMP_TZ schemas, ``datetime`` objects with ``tzinfo``),
this happens automatically. Plain string-parsed timestamps without
TZ info will silently ignore the context manager on DuckDB.

``custom_time_zone`` and ``set_session_time_zone`` are currently
identical wrappers; both exist because callers pre-date the rename.
Treat one as deprecated once a consolidation pass lands.
"""

from contextlib import contextmanager
from typing import Any, Generator, cast

from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.types import use_duckdb


def get_current_time_zone() -> str:
    """Return the current time zone of the runtime session."""
    # Local import to break the circular dependency: session.py imports
    # this module's helpers and is imported by callers up the stack.
    from dsgrid.ibis.session import get_runtime_session

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
    """Set the current time zone of the runtime session."""
    from dsgrid.ibis.session import _DuckDBRuntimeSession, get_runtime_session

    session = get_runtime_session()
    if use_duckdb():
        escaped = time_zone.replace("'", "''")
        if isinstance(session, _DuckDBRuntimeSession):
            conn = cast(Any, make_runtime_backend().connection)
            conn.raw_sql(f"SET TimeZone='{escaped}'")
        else:
            session.sql(f"SET TimeZone='{escaped}'")
        return

    session.conf.set("spark.sql.session.timeZone", time_zone)


@contextmanager
def custom_time_zone(time_zone: str) -> Generator[None, None, None]:
    """Apply a custom time zone for the duration of a code block."""
    orig_time_zone = get_current_time_zone()
    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        # Note that the user code could have restarted the session.
        # This will function will get the current one.
        set_current_time_zone(orig_time_zone)


@contextmanager
def set_session_time_zone(time_zone: str) -> Generator[None, None, None]:
    """Set the session time zone for execution of a code block.

    Currently identical to :func:`custom_time_zone`; both names exist to
    bridge call sites that pre-date the rename. Consolidate during the
    Phase 6 TZ correctness pass.
    """
    orig = get_current_time_zone()
    try:
        set_current_time_zone(time_zone)
        yield
    finally:
        set_current_time_zone(orig)
