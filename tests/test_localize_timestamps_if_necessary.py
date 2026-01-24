"""Tests for `localize_timestamps_if_necessary` pathways.

Covers:
- No-op when timestamps are already `TIMESTAMP_TZ`.
- Single time zone localization for `TIMESTAMP_NTZ`.
- Multi time zone localization via `time_zone` column (adds if missing).
- Backend routing: DuckDB, Spark+Hive Metastore, Spark+Path.
- Edge cases: multiple value columns (first is used), NTZ with no time zone
    (aligned format with `time_zone=None`) is a no-op.

All Chronify localization helpers are monkeypatched; assertions verify the
correct helper is called with expected arguments, or not called for no-op.
"""
from pathlib import Path

import pytest
from unittest.mock import MagicMock
import pandas as pd

from dsgrid.common import TIME_ZONE_COLUMN, TIME_COLUMN, VALUE_COLUMN, BackendEngine
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    TimeIntervalType,
    MeasurementType,
    TimeZoneFormat,
)
from dsgrid.config.dimensions import (
    DateTimeDimensionModel,
    TimeRangeModel,
    TimeFormatDateTimeNTZModel,
    TimeFormatDateTimeTZModel,
    AlignedTimeSingleTimeZone,
    LocalTimeMultipleTimeZones,
)
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.exceptions import DSGInvalidOperation
import dsgrid

from dsgrid.spark.functions import get_spark_session
from dsgrid.utils.dataset import localize_timestamps_if_necessary
from chronify.time_range_generator_factory import make_time_range_generator


spark = get_spark_session()


def make_datetime_config_single_tz_ntz(time_zone="ETC/GMT+7"):
    # default to Mountain Standard Time
    model = DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        class_name="Time",
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone=time_zone,
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return DateTimeDimensionConfig.load_from_model(model)


def make_dataframes_single_tz(time_dim):
    timestamps = make_time_range_generator(time_dim.to_chronify()).list_timestamps()
    df = pd.DataFrame({TIME_COLUMN: timestamps})
    df["geography"] = "dummy_geo"
    df[VALUE_COLUMN] = 1

    assert time_dim.get_localization_plan() == "localize_to_single_tz"
    to_tz = time_dim.model.time_zone_format.time_zone
    called_df = df.copy()
    called_df[TIME_COLUMN] = called_df[TIME_COLUMN].dt.tz_localize(to_tz)

    sdf = spark.createDataFrame(df)
    scalled_df = spark.createDataFrame(called_df)
    return sdf, scalled_df


def make_dataframes_multi_tz(time_dim):
    timestamps = make_time_range_generator(time_dim.to_chronify()).list_timestamps()
    df = pd.DataFrame({"ts": timestamps})
    df["geography"] = "dummy_geo"
    df[VALUE_COLUMN] = 1
    time_zones = time_dim.model.time_zone_format.get_time_zones()
    df[TIME_ZONE_COLUMN] = time_zones * (len(df) // len(time_zones))

    assert time_dim.get_localization_plan() == "localize_to_multi_tz"
    called_df = df.copy()
    for tz in time_zones:
        cond = called_df[TIME_ZONE_COLUMN] == tz
        called_df.loc[cond, TIME_COLUMN] = called_df.loc[cond, "ts"].dt.tz_localize(tz)

    sdf = spark.createDataFrame(df)
    scalled_df = spark.createDataFrame(called_df)
    return sdf, scalled_df


def make_datetime_config_multi_tz_ntz(time_zones=["ETC/GMT+5", "ETC/GMT+8"]):
    # default to Eastern and Pacific Standard Time
    model = DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        class_name="Time",
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_LOCAL_STD_TIME,
            time_zones=time_zones,
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return DateTimeDimensionConfig.load_from_model(model)


def make_datetime_config_tz_aware():
    model = DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        class_name="Time",
        column_format=TimeFormatDateTimeTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone="America/New_York",
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return DateTimeDimensionConfig.load_from_model(model)


def make_datetime_config_single_aligned_no_tz_ntz():
    model = DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        class_name="Time",
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=AlignedTimeSingleTimeZone(
            format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
            time_zone=None,
        ),
        measurement_type=MeasurementType.TOTAL,
        ranges=[
            TimeRangeModel(
                start="2018-01-01 00:00:00",
                end="2018-01-01 01:00:00",
                frequency=pd.Timedelta(hours=1),
            )
        ],
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )
    return DateTimeDimensionConfig.load_from_model(model)


class DummyDatasetConfig:
    def __init__(self, time_dim, value_columns=None, geography_dim=None):
        self._time_dim = time_dim
        self._value_columns = value_columns or [VALUE_COLUMN]
        self._geo_dim = geography_dim or MagicMock()

    def get_dimension(self, dimension_type):
        if dimension_type == DimensionType.TIME:
            return self._time_dim
        if dimension_type == DimensionType.GEOGRAPHY:
            return self._geo_dim
        return None

    def get_value_columns(self):
        return self._value_columns


def test_no_plan_returns_false(monkeypatch):
    time_dim = make_datetime_config_tz_aware()
    config = DummyDatasetConfig(time_dim)

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]

    # Ensure helper functions are not called
    for name in [
        "localize_time_zone_with_chronify_duckdb",
        "localize_time_zone_with_chronify_spark_hive",
        "localize_time_zone_with_chronify_spark_path",
        "localize_time_zone_by_column_with_chronify_duckdb",
        "localize_time_zone_by_column_with_chronify_spark_hive",
        "localize_time_zone_by_column_with_chronify_spark_path",
    ]:
        monkeypatch.setattr(f"dsgrid.utils.dataset.{name}", MagicMock())

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is False
    assert res_df is df


def test_single_tz_duckdb_calls_duckdb(monkeypatch):
    # Configure runtime
    monkeypatch.setattr(dsgrid.runtime_config, "backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr(dsgrid.runtime_config, "use_hive_metastore", False)

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]

    called_df = MagicMock()
    target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_duckdb",
        target,
    )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    target.assert_called_once()


def test_single_tz_spark_hive(monkeypatch):
    monkeypatch.setattr(dsgrid.runtime_config, "backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr(dsgrid.runtime_config, "use_hive_metastore", True)

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_single_tz(time_dim)

    target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_spark_hive",
        target,
    )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    target.assert_called_once()


def test_single_tz_spark_path(monkeypatch):
    dsgrid.runtime_config.backend_engine = BackendEngine.SPARK
    dsgrid.runtime_config.use_hive_metastore = False

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_single_tz(time_dim)

    path_target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_spark_path",
        path_target,
    )
    persist_target = MagicMock(return_value=Path("/tmp/dummy.parquet"))
    monkeypatch.setattr("dsgrid.utils.dataset.persist_table", persist_target)

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    path_target.assert_called_once()
    persist_target.assert_called_once()


def test_value_column_first_used(monkeypatch):
    # Backend choice doesn't matter; use DUCKDB
    dsgrid.runtime_config.backend_engine = BackendEngine.DUCKDB
    dsgrid.runtime_config.use_hive_metastore = False

    time_dim = make_datetime_config_single_tz_ntz()
    # Provide multiple value columns; function should pick the first
    config = DummyDatasetConfig(time_dim, value_columns=["val_a", "val_b", "val_c"])

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, "val_a", "val_b", "val_c"]

    called_df = MagicMock()
    target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_duckdb",
        target,
    )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    # Assert the first value column was passed to the localization helper
    assert target.call_count == 1
    kwargs = target.call_args.kwargs
    assert kwargs.get("value_column") == "val_a"


def test_multi_tz_duckdb_adds_tz_and_calls_duckdb(monkeypatch):
    dsgrid.runtime_config.backend_engine = BackendEngine.DUCKDB
    dsgrid.runtime_config.use_hive_metastore = False

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]  # missing TIME_ZONE_COLUMN

    add_tz_target = MagicMock(return_value=df)
    monkeypatch.setattr("dsgrid.utils.dataset.add_time_zone", add_tz_target)

    called_df = MagicMock()
    duck_target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset." + "localize_time_zone_by_column_with_chronify_duckdb",
        duck_target,
    )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    add_tz_target.assert_called_once()
    duck_target.assert_called_once()


def test_multi_tz_spark_hive_existing_tz_column(monkeypatch):
    dsgrid.runtime_config.backend_engine = BackendEngine.SPARK
    dsgrid.runtime_config.use_hive_metastore = True

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_multi_tz(time_dim)

    # Ensure add_time_zone is not called
    monkeypatch.setattr(
        "dsgrid.utils.dataset.add_time_zone",
        MagicMock(side_effect=AssertionError("should not be called")),
    )

    hive_target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset." + "localize_time_zone_by_column_with_chronify_spark_hive",
        hive_target,
    )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    hive_target.assert_called_once()


def test_multi_tz_spark_path(monkeypatch):
    dsgrid.runtime_config.backend_engine = BackendEngine.SPARK
    dsgrid.runtime_config.use_hive_metastore = False

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_multi_tz(time_dim)

    path_target = MagicMock(return_value=called_df)
    monkeypatch.setattr(
        "dsgrid.utils.dataset." + "localize_time_zone_by_column_with_chronify_spark_path",
        path_target,
    )
    persist_target = MagicMock(return_value=Path("/tmp/dummy.parquet"))
    monkeypatch.setattr("dsgrid.utils.dataset.persist_table", persist_target)

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is True
    assert res_df is called_df
    path_target.assert_called_once()
    persist_target.assert_called_once()


def test_unknown_plan_raises(monkeypatch):
    time_dim = make_datetime_config_single_tz_ntz()
    # Force unknown plan by monkeypatching instance method
    monkeypatch.setattr(
        time_dim,
        "get_localization_plan",
        lambda: "unknown_plan",
    )
    config = DummyDatasetConfig(time_dim)

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]

    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())


def test_invalid_time_dimension_raises():
    class NotDateTimeConfig:
        pass

    config = DummyDatasetConfig(time_dim=NotDateTimeConfig())
    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]

    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())


def test_ntz_no_time_zone_is_noop(monkeypatch):
    # TIMESTAMP_NTZ with aligned format and None time_zone should not localize
    time_dim = make_datetime_config_single_aligned_no_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    df = MagicMock()
    df.columns = ["geography", TIME_COLUMN, VALUE_COLUMN]

    # Ensure localization helpers are not called
    for name in [
        "localize_time_zone_with_chronify_duckdb",
        "localize_time_zone_with_chronify_spark_hive",
        "localize_time_zone_with_chronify_spark_path",
        "localize_time_zone_by_column_with_chronify_duckdb",
        "localize_time_zone_by_column_with_chronify_spark_hive",
        "localize_time_zone_by_column_with_chronify_spark_path",
    ]:
        monkeypatch.setattr(
            f"dsgrid.utils.dataset.{name}",
            MagicMock(side_effect=AssertionError("should not be called")),
        )

    res_df, changed = localize_timestamps_if_necessary(df, config, scratch_dir_context=MagicMock())
    assert changed is False
    assert res_df is df
