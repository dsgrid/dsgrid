"""Tests for time-in-parts to timestamp conversion helpers.

Focus:
- Build timestamp string/SQL from `TimeFormatInPartsModel`.
- Parse numeric and string UTC offsets into `timestamp_tz`.
- Validate DuckDB UTC rendering and Spark parity (conditional).

Localization call-order tests have been moved to
`tests/test_localize_timestamps_if_necessary.py`.
"""

import pytest
from unittest.mock import MagicMock
from pathlib import Path

from dsgrid.registry.dataset_registry_manager import DatasetRegistryManager
from dsgrid.config.dimensions import (
    TimeFormatInPartsModel,
)
from dsgrid.common import TIME_COLUMN
from dsgrid.ibis.functions import (
    get_runtime_session,
    select_expr,
)
from dsgrid.ibis.types import use_duckdb
import pandas as pd

from tests._helpers import collect as _collect


def _utc_ts_str_expr() -> str:
    """Return a SQL expression that renders a timestamp column as a UTC string."""
    if use_duckdb():
        return f"CAST({TIME_COLUMN} AT TIME ZONE 'UTC' AS VARCHAR) AS ts_str"
    return f"date_format(to_utc_timestamp({TIME_COLUMN}, 'UTC'), 'yyyy-MM-dd HH:mm:ss') AS ts_str"


def make_manager():
    return DatasetRegistryManager(
        Path.cwd(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )


def test_offset_parsing_in_parts_builds_correct_timestamps():
    """Ensure offset_column is parsed into timestamp as +HH:MM/-HH:MM.

    Covers positive fractional, negative whole, negative fractional hours.
    """
    mgr = make_manager()
    # Define time-in-parts format with an offset column
    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset",
    )

    # Build a small dataframe of test cases
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020] * 6,
            "month": [1] * 6,
            "day": [1] * 6,
            "hour": [0] * 6,
            "utc_offset": [5.5, -8.0, -7.75, 0.0, 9.25, -7.5],
            "val": [1, 2, 3, 4, 5, 6],
        }
    )
    df = spark.createDataFrame(pdf)

    # Build transformation SQL and apply
    base_ts_expr, offset_expr = mgr._build_timestamp_string_expr(col_format)
    ts_sql, new_col_format = mgr._build_timestamp_sql(base_ts_expr, offset_expr, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)
    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)

    # Verify new column format is timezone-aware
    assert new_col_format.dtype == "timestamp_tz"
    assert new_col_format.time_column == TIME_COLUMN

    # Cast timestamp back to string to verify absolute instants.
    # DuckDB TIMESTAMP WITH TIME ZONE displays in the
    # session time zone (default UTC).
    # These are the UTC instants corresponding to local times + offsets above.
    check_df = select_expr(out_df, [_utc_ts_str_expr()])
    rows = _collect(check_df)
    got = [row.ts_str for row in rows]
    expected = [
        "2019-12-31 18:30:00",  # +05:30
        "2020-01-01 08:00:00",  # -08:00
        "2020-01-01 07:45:00",  # -07:45
        "2020-01-01 00:00:00",  # +00:00
        "2019-12-31 14:45:00",  # +09:15
        "2020-01-01 07:30:00",  # -07:30
    ]
    assert got == expected


def test_offset_parsing_in_parts_accepts_string_offsets():
    mgr = make_manager()
    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset_str",
    )

    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020] * 3,
            "month": [1] * 3,
            "day": [1] * 3,
            "hour": [0] * 3,
            "utc_offset_str": ["+05:00", "-08:00", "-07:45"],
        }
    )
    df = spark.createDataFrame(pdf)

    base_ts_expr, offset_expr = mgr._build_timestamp_string_expr(col_format)
    ts_sql, new_col_format = mgr._build_timestamp_sql(base_ts_expr, offset_expr, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)
    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)

    assert new_col_format.dtype == "timestamp_tz"
    check_df = select_expr(out_df, [_utc_ts_str_expr()])
    rows = _collect(check_df)
    got = [row.ts_str for row in rows]
    # UTC instants corresponding to local time + string offsets
    expected = [
        "2019-12-31 19:00:00",  # +05:00
        "2020-01-01 08:00:00",  # -08:00
        "2020-01-01 07:45:00",  # -07:45
    ]
    assert got == expected


def test_invalid_numeric_offset_raises():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 0],
            "utc_offset": [24.5, -24.5],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        # Validate directly; conversion also invokes this guard
        mgr._validate_offset_bounds(df, "utc_offset")


def test_invalid_string_offset_raises():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 0],
            "utc_offset_str": ["+25:00", "-25:00"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_invalid_string_offset_minutes_out_of_range():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset_str": ["+12:60"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_invalid_string_offset_24_with_nonzero_minutes():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset_str": ["+24:01"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_invalid_string_offset_bad_format():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 0],
            "utc_offset_str": ["24:00", "+2400"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_invalid_string_offset_missing_leading_zero_hours():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset_str": ["+5:00"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_invalid_string_offset_missing_leading_zero_minutes():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset_str": ["-07:5"],
        }
    )
    df = spark.createDataFrame(pdf)
    with pytest.raises(Exception):
        mgr._validate_offset_bounds(df, "utc_offset_str")


def test_valid_string_offset_24_allowed():
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 0],
            "utc_offset_str": ["+24:00", "-24:00"],
        }
    )
    df = spark.createDataFrame(pdf)
    # Should not raise
    mgr._validate_offset_bounds(df, "utc_offset_str")


def test_cast_with_offset_24_numeric_previous_behavior():
    """Confirm numeric offset=24.0 is accepted and yields the correct UTC instant.

    The ±24 offset is normalized to ±00:00 with a 1-day adjustment so that
    both DuckDB and Spark produce the same result.
    """
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset": [24.0],
        }
    )
    df = spark.createDataFrame(pdf)

    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset",
    )
    base_ts_expr, offset_expr = mgr._build_timestamp_string_expr(col_format)
    ts_sql, _ = mgr._build_timestamp_sql(base_ts_expr, offset_expr, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)

    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)
    check_df = select_expr(out_df, [_utc_ts_str_expr()])
    rows = _collect(check_df)
    assert [row.ts_str for row in rows] == ["2019-12-31 00:00:00"]


def test_cast_with_offset_24_string_previous_behavior():
    """Confirm string offset="+24:00" is accepted and yields the correct UTC instant.

    The ±24 offset is normalized to +00:00 with a 1-day adjustment so that
    both DuckDB and Spark produce the same result.
    """
    mgr = make_manager()
    spark = get_runtime_session()
    pdf = pd.DataFrame(
        {
            "year": [2020],
            "month": [1],
            "day": [1],
            "hour": [0],
            "utc_offset_str": ["+24:00"],
        }
    )
    df = spark.createDataFrame(pdf)

    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset_str",
    )
    base_ts_expr, offset_expr = mgr._build_timestamp_string_expr(col_format)
    ts_sql, _ = mgr._build_timestamp_sql(base_ts_expr, offset_expr, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)

    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)
    check_df = select_expr(out_df, [_utc_ts_str_expr()])
    rows = _collect(check_df)
    assert [row.ts_str for row in rows] == ["2019-12-31 00:00:00"]
