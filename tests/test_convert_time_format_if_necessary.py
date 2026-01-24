"""Tests for time-in-parts to timestamp conversion helpers.

Focus:
- Build timestamp string/SQL from `TimeFormatInPartsModel`.
- Parse numeric and string UTC offsets into `TIMESTAMP_TZ`.
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
from dsgrid.spark.functions import (
    get_spark_session,
    select_expr,
    set_current_time_zone,
)
import pandas as pd


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
    mgr = make_manager()
    """Ensure offset_column is parsed into timestamp as +HH:MM/-HH:MM.

    Covers positive fractional, negative whole, negative fractional hours.
    """
    # Define time-in-parts format with an offset column
    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset",
    )

    # Build a small dataframe of test cases
    spark = get_spark_session()
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
    ts_str = mgr._build_timestamp_string_expr(col_format)
    ts_sql, new_col_format = mgr._build_timestamp_sql(ts_str, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)
    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)

    # Verify new column format is timezone-aware
    assert new_col_format.dtype == "TIMESTAMP_TZ"
    assert new_col_format.time_column == TIME_COLUMN

    # Cast timestamp back to string to verify absolute instants.
    # DuckDB TIMESTAMP WITH TIME ZONE displays in the
    # session time zone (default UTC).
    # These are the UTC instants corresponding to local times + offsets above.
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS VARCHAR) AS ts_str"])
    rows = check_df.collect()
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

    spark = get_spark_session()
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

    ts_str = mgr._build_timestamp_string_expr(col_format)
    ts_sql, new_col_format = mgr._build_timestamp_sql(ts_str, col_format)
    cols_to_drop = mgr._get_time_columns_to_drop(col_format)
    out_df = mgr._apply_timestamp_transformation(df, cols_to_drop, ts_sql)

    assert new_col_format.dtype == "TIMESTAMP_TZ"
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS VARCHAR) AS ts_str"])
    rows = check_df.collect()
    got = [row.ts_str for row in rows]
    # UTC instants corresponding to local time + string offsets
    expected = [
        "2019-12-31 19:00:00",  # +05:00
        "2020-01-01 08:00:00",  # -08:00
        "2020-01-01 07:45:00",  # -07:45
    ]
    assert got == expected


def test_offset_parsing_in_parts_spark_backend():
    # Only run when the module is loaded in Spark mode.
    import dsgrid.spark.types as types

    if types.use_duckdb():
        pytest.skip("Spark types not active; cannot switch backend at runtime")

    spark = get_spark_session()
    set_current_time_zone("UTC")

    col_format = TimeFormatInPartsModel(
        year_column="year",
        month_column="month",
        day_column="day",
        hour_column="hour",
        offset_column="utc_offset",
    )

    pdf = pd.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 0],
            "utc_offset": [5.5, -8.0],
        }
    )
    df = spark.createDataFrame(pdf)

    ts_str = DatasetRegistryManager._build_timestamp_string_expr(col_format)
    ts_sql, new_col_format = DatasetRegistryManager._build_timestamp_sql(ts_str, col_format)
    cols_to_drop = DatasetRegistryManager._get_time_columns_to_drop(col_format)
    out_df = DatasetRegistryManager._apply_timestamp_transformation(df, cols_to_drop, ts_sql)

    assert new_col_format.dtype == "TIMESTAMP_TZ"
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS STRING) AS ts_str"])
    rows = check_df.collect()
    got = [row.ts_str for row in rows]
    expected = [
        "2019-12-31 18:30:00",
        "2020-01-01 08:00:00",
    ]
    assert got == expected
