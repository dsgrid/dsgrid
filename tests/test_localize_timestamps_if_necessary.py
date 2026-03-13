"""Tests for `localize_timestamps_if_necessary` pathways.

Covers:
- No-op when timestamps are already `timestamp_tz`.
- No-op when NTZ has no time zone (aligned format with `time_zone=None`).
- Single time zone localization for `timestamp_ntz`:
    backend routing across DuckDB, Spark+Hive Metastore, and Spark+Path.
- Multi time zone localization via `time_zone` column:
    - `time_zone` column added automatically when absent (DuckDB path).
    - `time_zone` column already present skips `add_time_zone` (DuckDB and Spark+Hive).
    - Backend routing across DuckDB, Spark+Hive Metastore, and Spark+Path.
- Multiple value columns: the first is forwarded to the localization helper.
- Error cases: unknown localization plan and non-DateTimeDimensionConfig dimension.

Chronify localization helpers are monkeypatched with real-data capture functions;
assertions verify kwargs (from_time_dim, time_zone, value_column, scratch_dir_context,
filename) and output DataFrame contents.
"""
from pathlib import Path

import pytest
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
from dsgrid.spark.types import DataFrame
from dsgrid.spark.functions import get_spark_session
from dsgrid.utils.dataset import localize_timestamps_if_necessary
from dsgrid.utils.scratch_dir_context import ScratchDirContext


spark = get_spark_session()


def make_datetime_config_single_tz_ntz(time_zone="Etc/GMT+7"):
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
    time_range = time_dim.model.ranges[0]
    timestamps = pd.date_range(
        start=time_range.start, end=time_range.end, freq=time_range.frequency, inclusive="left"
    )
    df = pd.DataFrame({TIME_COLUMN: timestamps, "geography": "dummy_geo", VALUE_COLUMN: 1})

    assert time_dim.get_localization_plan() == "localize_to_single_tz"
    to_tz = time_dim.model.time_zone_format.time_zone
    called_df = df.copy()
    called_df[TIME_COLUMN] = called_df[TIME_COLUMN].dt.tz_localize(to_tz)

    return df, called_df


def make_dataframes_multi_tz(time_dim):
    time_zones = time_dim.model.time_zone_format.get_time_zones()
    time_range = time_dim.model.ranges[0]
    timestamps = pd.date_range(
        start=time_range.start, end=time_range.end, freq=time_range.frequency, inclusive="left"
    )

    rows = [
        {TIME_ZONE_COLUMN: tz, TIME_COLUMN: ts, "geography": "dummy_geo", VALUE_COLUMN: 1}
        for tz in time_zones
        for ts in timestamps
    ]
    df = pd.DataFrame(rows)

    assert time_dim.get_localization_plan() == "localize_to_multi_tz"
    final_tz = time_zones[0]
    called_df = df.rename(columns={TIME_COLUMN: "ts"}).copy()
    for tz in time_zones:
        cond = called_df[TIME_ZONE_COLUMN] == tz
        called_df.loc[cond, TIME_COLUMN] = (
            called_df.loc[cond, "ts"].dt.tz_localize(tz).dt.tz_convert(final_tz)
        )

    called_df.drop(columns=["ts"], inplace=True)
    return df, called_df


def make_datetime_config_multi_tz_ntz(time_zones=["Etc/GMT+5", "Etc/GMT+8"]):
    # default to Eastern and Pacific Standard Time
    model = DateTimeDimensionModel(
        name="time",
        type=DimensionType.TIME,
        module="dsgrid.dimension.standard",
        class_name="Time",
        column_format=TimeFormatDateTimeNTZModel(),
        time_zone_format=LocalTimeMultipleTimeZones(
            format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
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
        self._geo_dim = geography_dim

    def get_dimension(self, dimension_type):
        if dimension_type == DimensionType.TIME:
            return self._time_dim
        if dimension_type == DimensionType.GEOGRAPHY:
            return self._geo_dim
        return None

    def get_value_columns(self):
        return self._value_columns


def _make_simple_dataframe(extra_columns: dict | None = None) -> DataFrame:
    """Create a minimal real Spark DataFrame for routing tests."""
    pdf = pd.DataFrame(
        {
            TIME_COLUMN: [
                pd.Timestamp("2018-01-01 00:00:00"),
                pd.Timestamp("2018-01-01 01:00:00"),
            ],
            "geography": ["g1", "g1"],
            VALUE_COLUMN: [1.0, 2.0],
        }
    )
    if extra_columns:
        pdf = pdf.assign(**extra_columns)
    return spark.createDataFrame(pdf)


def test_no_plan_returns_false(tmp_path):
    time_dim = make_datetime_config_tz_aware()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe()
    original_columns = sdf.columns
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is False
    assert res_df is sdf
    assert res_df.columns == original_columns


def test_single_tz_duckdb_calls_duckdb(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    expected_tz = time_dim.get_chronify_time_zone()

    sdf = _make_simple_dataframe()
    result_sdf = _make_simple_dataframe()

    captured = {}

    def fake_localize(**kwargs):
        captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_duckdb",
        fake_localize,
    )

    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    assert captured.get("from_time_dim") is time_dim
    assert captured.get("time_zone") == expected_tz
    assert captured.get("value_column") == VALUE_COLUMN
    assert captured.get("scratch_dir_context") is ctx
    res_pdf = res_df.toPandas() if isinstance(res_df, DataFrame) else res_df
    expected_pdf = result_sdf.toPandas() if isinstance(result_sdf, DataFrame) else result_sdf
    assert list(res_pdf[TIME_COLUMN]) == list(expected_pdf[TIME_COLUMN])


def test_single_tz_spark_hive(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", True)

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    expected_tz = time_dim.get_chronify_time_zone()
    df, called_df = make_dataframes_single_tz(time_dim)
    result_sdf = spark.createDataFrame(called_df)

    captured = {}

    def fake_localize(**kwargs):
        captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_spark_hive",
        fake_localize,
    )

    sdf = spark.createDataFrame(df)
    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    assert captured.get("from_time_dim") is time_dim
    assert captured.get("time_zone") == expected_tz
    assert captured.get("value_column") == VALUE_COLUMN
    assert captured.get("scratch_dir_context") is ctx
    if isinstance(res_df, DataFrame):
        res_df = res_df.toPandas()
    assert sorted(res_df[TIME_COLUMN]) == sorted(called_df[TIME_COLUMN])


def test_single_tz_spark_path(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    expected_tz = time_dim.get_chronify_time_zone()
    df, called_df = make_dataframes_single_tz(time_dim)
    result_sdf = spark.createDataFrame(called_df)
    persisted_path = Path("/tmp/dummy.parquet")

    persist_called = {}
    helper_captured = {}

    def fake_persist(df, ctx, tag=""):
        persist_called["called"] = True
        return persisted_path

    def fake_localize(**kwargs):
        helper_captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr("dsgrid.utils.dataset.persist_table", fake_persist)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_spark_path",
        fake_localize,
    )

    sdf = spark.createDataFrame(df)
    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    assert persist_called.get("called") is True
    assert helper_captured.get("filename") == persisted_path
    assert helper_captured.get("from_time_dim") is time_dim
    assert helper_captured.get("time_zone") == expected_tz
    assert helper_captured.get("value_column") == VALUE_COLUMN
    assert helper_captured.get("scratch_dir_context") is ctx
    if isinstance(res_df, DataFrame):
        res_df = res_df.toPandas()
    assert sorted(res_df[TIME_COLUMN]) == sorted(called_df[TIME_COLUMN])


def test_value_column_first_used(monkeypatch, tmp_path):
    # Backend choice doesn't matter; use DUCKDB
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_single_tz_ntz()
    # Provide multiple value columns; function should pick the first
    config = DummyDatasetConfig(time_dim, value_columns=["val_a", "val_b", "val_c"])

    sdf = _make_simple_dataframe({"val_a": [1.0, 2.0], "val_b": [3.0, 4.0], "val_c": [5.0, 6.0]})
    result_sdf = _make_simple_dataframe()

    captured = {}

    def fake_localize(**kwargs):
        captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_with_chronify_duckdb",
        fake_localize,
    )

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is True
    # Assert the first value column was passed, not the others
    assert captured.get("value_column") == "val_a"
    assert captured.get("from_time_dim") is time_dim


def test_multi_tz_duckdb_adds_tz_and_calls_duckdb(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe()  # missing TIME_ZONE_COLUMN
    sdf_with_tz = _make_simple_dataframe({TIME_ZONE_COLUMN: ["Etc/GMT+5", "Etc/GMT+8"]})
    result_sdf = _make_simple_dataframe()

    add_tz_called = {}

    def fake_add_tz(df, geo_dim):
        add_tz_called["called"] = True
        return sdf_with_tz

    monkeypatch.setattr("dsgrid.utils.dataset.add_time_zone", fake_add_tz)

    duck_captured = {}

    def fake_duck(**kwargs):
        duck_captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_by_column_with_chronify_duckdb",
        fake_duck,
    )

    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    # add_time_zone must be called because TIME_ZONE_COLUMN was absent
    assert add_tz_called.get("called") is True
    assert duck_captured.get("from_time_dim") is time_dim
    assert duck_captured.get("value_column") == VALUE_COLUMN
    assert duck_captured.get("scratch_dir_context") is ctx
    res_pdf = res_df.toPandas() if isinstance(res_df, DataFrame) else res_df
    expected_pdf = result_sdf.toPandas() if isinstance(result_sdf, DataFrame) else result_sdf
    assert list(res_pdf[TIME_COLUMN]) == list(expected_pdf[TIME_COLUMN])


def test_multi_tz_duckdb_existing_tz_column(monkeypatch, tmp_path):
    """When TIME_ZONE_COLUMN is already in the dataframe, add_time_zone must not be called."""
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe({TIME_ZONE_COLUMN: ["Etc/GMT+5", "Etc/GMT+8"]})
    result_sdf = _make_simple_dataframe()

    def add_tz_must_not_be_called(df, geo_dim):
        msg = "add_time_zone should not be called"
        raise AssertionError(msg)

    monkeypatch.setattr("dsgrid.utils.dataset.add_time_zone", add_tz_must_not_be_called)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_by_column_with_chronify_duckdb",
        lambda **kwargs: result_sdf,
    )

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is True


def test_multi_tz_spark_hive_existing_tz_column(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", True)

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_multi_tz(time_dim)

    # Ensure add_time_zone is not called (df already has TIME_ZONE_COLUMN)
    def add_tz_must_not_be_called(df, geo_dim):
        msg = "add_time_zone should not be called"
        raise AssertionError(msg)

    monkeypatch.setattr("dsgrid.utils.dataset.add_time_zone", add_tz_must_not_be_called)

    result_sdf = spark.createDataFrame(called_df)
    hive_captured = {}

    def fake_hive(**kwargs):
        hive_captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_by_column_with_chronify_spark_hive",
        fake_hive,
    )

    sdf = spark.createDataFrame(df)
    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    assert hive_captured.get("from_time_dim") is time_dim
    assert hive_captured.get("value_column") == VALUE_COLUMN
    assert hive_captured.get("scratch_dir_context") is ctx
    if isinstance(res_df, DataFrame):
        res_df = res_df.toPandas()
    assert sorted(res_df[TIME_COLUMN]) == sorted(called_df[TIME_COLUMN])


def test_multi_tz_spark_path(monkeypatch, tmp_path):
    monkeypatch.setattr("dsgrid.runtime_config.backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr("dsgrid.runtime_config.use_hive_metastore", False)

    time_dim = make_datetime_config_multi_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    df, called_df = make_dataframes_multi_tz(time_dim)
    result_sdf = spark.createDataFrame(called_df)
    persisted_path = Path("/tmp/dummy.parquet")

    persist_called = {}
    path_captured = {}

    def fake_persist(df, ctx, tag=""):
        persist_called["called"] = True
        return persisted_path

    def fake_localize(**kwargs):
        path_captured.update(kwargs)
        return result_sdf

    monkeypatch.setattr("dsgrid.utils.dataset.persist_table", fake_persist)
    monkeypatch.setattr(
        "dsgrid.utils.dataset.localize_time_zone_by_column_with_chronify_spark_path",
        fake_localize,
    )

    sdf = spark.createDataFrame(df)
    ctx = ScratchDirContext(tmp_path)
    res_df, changed = localize_timestamps_if_necessary(sdf, config, scratch_dir_context=ctx)
    assert changed is True
    assert persist_called.get("called") is True
    assert path_captured.get("filename") == persisted_path
    assert path_captured.get("from_time_dim") is time_dim
    assert path_captured.get("value_column") == VALUE_COLUMN
    assert path_captured.get("scratch_dir_context") is ctx
    if isinstance(res_df, DataFrame):
        res_df = res_df.toPandas()
    assert sorted(res_df[TIME_COLUMN]) == sorted(called_df[TIME_COLUMN])


def test_unknown_plan_raises(monkeypatch, tmp_path):
    time_dim = make_datetime_config_single_tz_ntz()
    # Force unknown plan by monkeypatching instance method
    monkeypatch.setattr(time_dim, "get_localization_plan", lambda: "unknown_plan")
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe()
    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(
            sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
        )


def test_invalid_time_dimension_raises(tmp_path):
    class NotDateTimeConfig:
        pass

    config = DummyDatasetConfig(time_dim=NotDateTimeConfig())
    sdf = _make_simple_dataframe()
    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(
            sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
        )


def test_ntz_no_time_zone_is_noop(tmp_path):
    # timestamp_ntz with aligned format and None time_zone should not localize
    time_dim = make_datetime_config_single_aligned_no_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe()
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is False
    assert res_df is sdf
