from datetime import datetime, timezone

import pytest

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.table_utils import table_column_to_list
from dsgrid.ibis.tz import (
    assert_tz_aware_extraction,
    custom_time_zone,
    get_current_time_zone,
    set_current_time_zone,
)
from dsgrid.ibis.types import use_duckdb


def _years_under_tz(table, time_zone: str) -> list[int]:
    """Return the distinct years extracted from ``table['ts']`` inside ``time_zone``."""
    with custom_time_zone(time_zone):
        return sorted(
            table_column_to_list(table.select(year=table["ts"].year()).distinct(), "year")
        )


def test_current_time_zone_contexts():
    original = get_current_time_zone()
    try:
        set_current_time_zone("UTC")
        assert get_current_time_zone() == "UTC"
        with custom_time_zone("America/Denver"):
            assert get_current_time_zone() == "America/Denver"
        assert get_current_time_zone() == "UTC"
    finally:
        set_current_time_zone(original)


def test_custom_time_zone_affects_tz_aware_extraction():
    """``custom_time_zone`` must change how ``.year()`` resolves on a TZ-aware column.

    The instant ``2021-01-01 04:00 UTC`` is ``2020-12-31 20:00`` in
    ``America/Los_Angeles``, so the extracted year flips from 2021 (UTC) to 2020 (LA).
    This is the load-bearing assumption of the time migration: extractions inside
    ``custom_time_zone`` resolve in the requested zone. It backs the leap-year divisor
    in :func:`dsgrid.config.annual_time_dimension_config.map_annual_time_to_date_time`.
    """
    sess = get_runtime_session()
    if use_duckdb():
        aware = sess.sql("SELECT TIMESTAMPTZ '2021-01-01 04:00:00+00' AS ts")
        assert str(aware.schema()["ts"]).startswith("timestamp('UTC'")
    else:
        # Spark TimestampType is instant-based and always rendered via the session TZ.
        aware = sess.createDataFrame(
            [(datetime(2021, 1, 1, 4, 0, tzinfo=timezone.utc),)], schema=["ts"]
        )
    assert _years_under_tz(aware, "UTC") == [2021]
    assert _years_under_tz(aware, "America/Los_Angeles") == [2020]


@pytest.mark.skipif(not use_duckdb(), reason="naive vs TZ-aware split is DuckDB-specific")
def test_custom_time_zone_ignored_on_naive_duckdb_timestamp():
    """On DuckDB, ``custom_time_zone`` only affects ``TIMESTAMPTZ`` columns.

    A naive ``TIMESTAMP`` is extracted TZ-naively regardless of the connection
    ``TimeZone`` -- the trap the :mod:`dsgrid.ibis.tz` docstring warns about. dsgrid
    relies on timestamps reaching DuckDB as TZ-aware (chronify output, ``TIMESTAMP_TZ``
    schemas, tz-aware ``datetime`` objects); this pins the failure mode so a regression
    to naive timestamps is caught here rather than silently mis-resolving years
    downstream (e.g. the annual leap-year divisor).
    """
    sess = get_runtime_session()
    naive = sess.sql("SELECT TIMESTAMP '2021-01-01 04:00:00' AS ts")
    assert str(naive.schema()["ts"]) == "timestamp(6)"
    # Same wall-clock year under any zone -- the connection TZ is ignored.
    assert _years_under_tz(naive, "UTC") == [2021]
    assert _years_under_tz(naive, "America/Los_Angeles") == [2021]


def test_assert_tz_aware_extraction_allows_tz_aware_column():
    """The guard is a no-op for a TZ-aware column (DuckDB) and for any column on Spark."""
    sess = get_runtime_session()
    if use_duckdb():
        aware = sess.sql("SELECT TIMESTAMPTZ '2021-01-01 04:00:00+00' AS ts")
    else:
        aware = sess.createDataFrame(
            [(datetime(2021, 1, 1, 4, 0, tzinfo=timezone.utc),)], schema=["ts"]
        )
    assert_tz_aware_extraction(aware["ts"])  # must not raise


@pytest.mark.skipif(not use_duckdb(), reason="naive vs TZ-aware split is DuckDB-specific")
def test_assert_tz_aware_extraction_rejects_naive_duckdb_column():
    """On DuckDB the guard fails loudly on a naive TIMESTAMP before a TZ-sensitive extraction."""
    sess = get_runtime_session()
    naive = sess.sql("SELECT TIMESTAMP '2021-01-01 04:00:00' AS ts")
    with pytest.raises(DSGInvalidOperation, match="TZ-aware timestamp is required"):
        assert_tz_aware_extraction(naive["ts"])
