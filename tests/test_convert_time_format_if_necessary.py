"""Tests for `DatasetRegistryManager._convert_time_format_if_necessary`.

Validates behavior across time dimension formats:
- No TIME dimension: returns early without touching handler.
- TimeFormatInParts: converts, then localizes (order matters).
- TimeFormatDateTimeNTZ: skips conversion, localizes.
- TimeFormatDateTimeTZ: skips conversion, localizes.

These are unit-level tests using stubs/mocks to avoid Spark/Pandas dependencies
while asserting call ordering and argument plumbing.
"""
import pytest
from unittest.mock import MagicMock
from pathlib import Path

from dsgrid.registry.dataset_registry_manager import DatasetRegistryManager
from dsgrid.config.dimensions import (
    DateTimeDimensionModel,
    TimeFormatInPartsModel,
    TimeFormatDateTimeNTZModel,
    TimeFormatDateTimeTZModel,
    AlignedTimeSingleTimeZone,
    TimeRangeModel,
)
from dsgrid.dimension.time import TimeIntervalType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.common import TIME_COLUMN
from dsgrid.spark.functions import get_spark_session, select_expr, set_current_time_zone
import pandas as pd


class _TimeDimStub:
    def __init__(self, model):
        self.model = model


def make_datetime_config_in_parts():
    model = DateTimeDimensionModel(
        name="time",
        dimension_type=DimensionType.TIME,
        class_name="Geography",
        column_format=TimeFormatInPartsModel(
            year_column="year",
            month_column="month",
            day_column="day",
            hour_column="hour",
            offset_column=None,
        ),
        time_zone_format=AlignedTimeSingleTimeZone(time_zone=None),
        ranges=[
            TimeRangeModel(
                start="2020-01-01 00:00:00",
                end="2020-01-01 01:00:00",
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return _TimeDimStub(model)


def make_datetime_config_ntz():
    model = DateTimeDimensionModel(
        name="time",
        dimension_type=DimensionType.TIME,
        class_name="Geography",
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(time_zone=None),
        ranges=[
            TimeRangeModel(
                start="2020-01-01 00:00:00",
                end="2020-01-01 01:00:00",
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return _TimeDimStub(model)


def make_datetime_config_tz():
    model = DateTimeDimensionModel(
        name="time",
        dimension_type=DimensionType.TIME,
        class_name="Geography",
        column_format=TimeFormatDateTimeTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(time_zone="UTC"),
        ranges=[
            TimeRangeModel(
                start="2020-01-01 00:00:00",
                end="2020-01-01 01:00:00",
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return _TimeDimStub(model)


def make_manager():
    # Provide minimal, unused dependencies as mocks
    return DatasetRegistryManager(
        Path.cwd(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )


def test_no_time_dimension_does_nothing(monkeypatch):
    mgr = make_manager()
    config = MagicMock()
    handler = MagicMock()
    scratch = MagicMock()

    config.get_dimension.return_value = None

    called_convert = False
    called_localize = False

    def fake_convert(df, time_dim, cfg, scratch_ctx):
        nonlocal called_convert
        called_convert = True
        return df

    def fake_localize(df, time_dim, cfg, scratch_ctx):
        nonlocal called_localize
        called_localize = True
        return df

    monkeypatch.setattr(mgr, "_convert_time_format_in_parts_to_datetime", fake_convert)
    monkeypatch.setattr(mgr, "_localize_timestamps_if_necessary", fake_localize)

    mgr._convert_time_format_if_necessary(config, handler, scratch)

    handler.get_base_load_data_table.assert_not_called()
    assert not called_convert
    assert not called_localize


def test_in_parts_triggers_convert_then_localize(monkeypatch):
    mgr = make_manager()
    time_dim = make_datetime_config_in_parts()

    config = MagicMock()
    config.get_dimension.return_value = time_dim

    handler = MagicMock()
    base_df = object()
    converted_df = object()
    handler.get_base_load_data_table.return_value = base_df
    scratch = MagicMock()

    call_order = []

    def fake_convert(df, td, cfg, scratch_ctx):
        call_order.append("convert")
        assert df is base_df
        assert td is time_dim
        assert cfg is config
        assert scratch_ctx is scratch
        return converted_df

    def fake_localize(df, td, cfg, scratch_ctx):
        call_order.append("localize")
        # Should receive the converted df
        assert df is converted_df
        assert td is time_dim
        assert cfg is config
        assert scratch_ctx is scratch
        return df

    monkeypatch.setattr(mgr, "_convert_time_format_in_parts_to_datetime", fake_convert)
    monkeypatch.setattr(mgr, "_localize_timestamps_if_necessary", fake_localize)

    mgr._convert_time_format_if_necessary(config, handler, scratch)

    handler.get_base_load_data_table.assert_called_once()
    assert call_order == ["convert", "localize"]


def test_datetime_without_parts_triggers_only_localize(monkeypatch):
    mgr = make_manager()
    time_dim = make_datetime_config_ntz()

    config = MagicMock()
    config.get_dimension.return_value = time_dim

    handler = MagicMock()
    base_df = object()
    handler.get_base_load_data_table.return_value = base_df
    scratch = MagicMock()

    called_convert = False
    called_localize = False
    call_order = []

    def fake_convert(df, td, cfg, scratch_ctx):
        nonlocal called_convert
        called_convert = True
        call_order.append("convert")
        return df

    def fake_localize(df, td, cfg, scratch_ctx):
        nonlocal called_localize
        called_localize = True
        call_order.append("localize")
        assert df is base_df
        return df

    monkeypatch.setattr(mgr, "_convert_time_format_in_parts_to_datetime", fake_convert)
    monkeypatch.setattr(mgr, "_localize_timestamps_if_necessary", fake_localize)

    mgr._convert_time_format_if_necessary(config, handler, scratch)

    handler.get_base_load_data_table.assert_called_once()
    assert not called_convert
    assert called_localize
    assert call_order == ["localize"]


def test_datetime_tz_triggers_only_localize(monkeypatch):
    mgr = make_manager()
    time_dim = make_datetime_config_tz()

    config = MagicMock()
    config.get_dimension.return_value = time_dim

    handler = MagicMock()
    base_df = object()
    handler.get_base_load_data_table.return_value = base_df
    scratch = MagicMock()

    called_convert = False
    called_localize = False
    call_order = []

    def fake_convert(df, td, cfg, scratch_ctx):
        nonlocal called_convert
        called_convert = True
        call_order.append("convert")
        return df

    def fake_localize(df, td, cfg, scratch_ctx):
        nonlocal called_localize
        called_localize = True
        call_order.append("localize")
        assert df is base_df
        return df

    monkeypatch.setattr(mgr, "_convert_time_format_in_parts_to_datetime", fake_convert)
    monkeypatch.setattr(mgr, "_localize_timestamps_if_necessary", fake_localize)

    mgr._convert_time_format_if_necessary(config, handler, scratch)

    handler.get_base_load_data_table.assert_called_once()
    assert not called_convert
    assert called_localize
    assert call_order == ["localize"]


def test_offset_parsing_in_parts_builds_correct_timestamps():
    """Ensure offset_column is parsed into the timestamp as +HH:MM/-HH:MM.

    Validates edge cases: positive fractional hour, negative whole hour, negative fractional hour.
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

    # Cast the timestamp back to string to verify the absolute instants
    # DuckDB TIMESTAMP WITH TIME ZONE displays in session time zone (default UTC).
    # So these are the UTC instants corresponding to the local times + offsets above.
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS VARCHAR) AS ts_str"])  # duckdb path
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
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS VARCHAR) AS ts_str"])  # duckdb path
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
    check_df = select_expr(out_df, [f"CAST({TIME_COLUMN} AS STRING) AS ts_str"])  # spark path
    rows = check_df.collect()
    got = [row.ts_str for row in rows]
    expected = [
        "2019-12-31 18:30:00",
        "2020-01-01 08:00:00",
    ]
    assert got == expected
