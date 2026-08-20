"""Tests for `localize_timestamps_if_necessary` pathways.

Covers:
- No-op when timestamps are already `timestamp_tz`.
- No-op when NTZ has no time zone (aligned format with `time_zone=None`).
- Single time zone localization for `timestamp_ntz` with DuckDB and Spark backends.
- Multi time zone localization via `time_zone` column:
    - `time_zone` column added automatically when absent.
    - `time_zone` column already present skips `add_time_zone`.
    - Backend routing across DuckDB and Spark.
- Multiple value columns: only the first is used by chronify; all are preserved.
- Error cases: unknown localization plan and non-DateTimeDimensionConfig dimension.

All tests run real chronify localization processes end-to-end without patching helpers.
"""

import ibis
import pytest
import pandas as pd
from pydantic import ValidationError

from dsgrid.common import TIME_ZONE_COLUMN, TIME_COLUMN, VALUE_COLUMN
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
from dsgrid.ibis.session import (
    DoubleType,
    get_runtime_session,
    get_spark_session,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    use_duckdb,
)
from dsgrid.ibis.table_utils import table_to_pandas
from dsgrid.utils.dataset import localize_timestamps_if_necessary
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from tests._helpers import DummyDatasetConfig, DummyGeoDim


@pytest.fixture(scope="module")
def spark():
    return get_runtime_session()


skip_unless_spark = pytest.mark.skipif(
    use_duckdb(), reason="Spark routing tests only run when backend_engine is SPARK"
)
skip_unless_duckdb = pytest.mark.skipif(
    not use_duckdb(), reason="DuckDB routing tests only run when backend_engine is DUCKDB"
)


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


def test_datetime_model_rejects_dst_zone_for_naive_localization():
    """A tz-naive (NTZ) dimension localizing to a DST-observing zone must be rejected.

    DST transitions make naive wall-clock times ambiguous (fall-back) or nonexistent
    (spring-forward), so only fixed-offset zones (e.g. ``Etc/GMT+5``) are allowed when
    localizing tz-naive timestamps. This pins the ``_check_standard_time_only`` guard.
    """
    with pytest.raises(ValidationError, match="observes daylight saving time"):
        DateTimeDimensionModel(
            name="time",
            type=DimensionType.TIME,
            module="dsgrid.dimension.standard",
            class_name="Time",
            column_format=TimeFormatDateTimeNTZModel(),
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


def test_datetime_model_rejects_dst_zone_in_multi_tz_naive_localization():
    """The DST restriction applies to every zone in a multi-tz naive dimension, so a mix
    of a fixed-offset zone and a DST-observing zone is still rejected."""
    with pytest.raises(ValidationError, match="observes daylight saving time"):
        DateTimeDimensionModel(
            name="time",
            type=DimensionType.TIME,
            module="dsgrid.dimension.standard",
            class_name="Time",
            column_format=TimeFormatDateTimeNTZModel(),
            time_zone_format=LocalTimeMultipleTimeZones(
                format_type=TimeZoneFormat.ALIGNED_IN_STD_CLOCK_TIME,
                time_zones=["Etc/GMT+5", "America/Los_Angeles"],
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


def test_datetime_model_allows_dst_zone_when_tz_aware():
    """The DST-zone restriction applies only to naive localization: a tz-aware (TIMESTAMP_TZ)
    dimension may legitimately use a DST-observing zone like ``America/New_York``."""
    config = make_datetime_config_tz_aware()  # America/New_York, TZ-aware column format
    assert config.get_time_zone() == "America/New_York"


def _make_simple_dataframe(spark, extra_columns: dict | None = None) -> ibis.Table:
    """Create a minimal real Ibis table for routing tests."""
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
    schema = StructType(
        [
            StructField(TIME_COLUMN, TimestampNTZType(), True),
            StructField("geography", StringType(), True),
            StructField(VALUE_COLUMN, DoubleType(), True),
        ]
        + [StructField(col, DoubleType(), True) for col in (extra_columns or {})]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _make_multi_tz_dataframe(
    spark,
    time_zones: tuple[str, ...] = ("Etc/GMT+5", "Etc/GMT+8"),
) -> ibis.Table:
    """Create an Ibis table with TIME_ZONE_COLUMN for multi-timezone localization tests.

    Each distinct time zone rides on its own geography (``g1``, ``g2``, ...), mirroring
    real datasets where the per-row time zone is derived from the geography.
    """
    timestamps = [pd.Timestamp("2018-01-01 00:00:00"), pd.Timestamp("2018-01-01 01:00:00")]
    rows = [
        {TIME_COLUMN: ts, "geography": f"g{i + 1}", VALUE_COLUMN: 1.0, TIME_ZONE_COLUMN: tz}
        for i, tz in enumerate(time_zones)
        for ts in timestamps
    ]
    schema = StructType(
        [
            StructField(TIME_COLUMN, TimestampNTZType(), True),
            StructField("geography", StringType(), True),
            StructField(VALUE_COLUMN, DoubleType(), True),
            StructField(TIME_ZONE_COLUMN, StringType(), True),
        ]
    )
    return spark.createDataFrame(pd.DataFrame(rows), schema=schema)


def test_no_plan_returns_false(spark, tmp_path):
    time_dim = make_datetime_config_tz_aware()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe(spark)
    original_columns = sdf.columns
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is False
    assert res_df is sdf
    assert res_df.columns == original_columns


@skip_unless_duckdb
def test_single_tz_duckdb(spark, tmp_path):
    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    sdf = _make_simple_dataframe(spark)

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    sdf2 = table_to_pandas(sdf)
    tz = time_dim.model.time_zone_format.time_zone
    res_df2 = table_to_pandas(res_df)
    assert res_df2[TIME_COLUMN].dt.tz is not None
    assert set(res_df2[TIME_COLUMN]) == set(sdf2[TIME_COLUMN].dt.tz_localize(tz))


@skip_unless_spark
def test_single_tz_spark_path(spark, tmp_path):
    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim)
    sdf = _make_simple_dataframe(spark)

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    res_df2 = table_to_pandas(res_df)

    tz = time_dim.model.time_zone_format.time_zone
    sdf2 = table_to_pandas(sdf)
    assert set(res_df2[TIME_COLUMN].dt.tz_localize(session_tz)) == set(
        sdf2[TIME_COLUMN].dt.tz_localize(tz)
    )


@skip_unless_duckdb
def test_value_column_first_used(spark, tmp_path):
    """All value columns are preserved in the output; timestamps become tz-aware."""
    time_dim = make_datetime_config_single_tz_ntz()
    config = DummyDatasetConfig(time_dim, value_columns=["val_a", "val_b", "val_c"])

    sdf = _make_simple_dataframe(
        spark, {"val_a": [1.0, 2.0], "val_b": [3.0, 4.0], "val_c": [5.0, 6.0]}
    )
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    assert set(res_df.columns) >= {"val_a", "val_b", "val_c"}
    assert table_to_pandas(res_df)[TIME_COLUMN].dt.tz is not None


@skip_unless_duckdb
def test_multi_tz_duckdb_adds_tz_column(spark, tmp_path):
    """When TIME_ZONE_COLUMN is absent it is added from the geography dimension."""
    tz = "Etc/GMT+5"
    time_dim = make_datetime_config_multi_tz_ntz(time_zones=[tz])
    geo_dim = DummyGeoDim(spark)  # maps g1 -> Etc/GMT+5
    config = DummyDatasetConfig(time_dim, geography_dim=geo_dim)

    sdf = _make_simple_dataframe(spark)
    assert TIME_ZONE_COLUMN not in sdf.columns

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    sdf2 = table_to_pandas(sdf)
    res_df2 = table_to_pandas(res_df)
    assert res_df2[TIME_COLUMN].dt.tz is not None
    assert set(res_df2[TIME_COLUMN]) == set(sdf2[TIME_COLUMN].dt.tz_localize(tz))


@skip_unless_duckdb
def test_multi_tz_duckdb_existing_tz_column(spark, tmp_path):
    """When TIME_ZONE_COLUMN is already present, add_time_zone is not invoked.

    geography_dim is intentionally None: if add_time_zone were called it would fail.
    """
    tz = "Etc/GMT+5"
    time_dim = make_datetime_config_multi_tz_ntz(time_zones=[tz])
    config = DummyDatasetConfig(time_dim, geography_dim=None)

    sdf = _make_multi_tz_dataframe(spark, time_zones=(tz,))
    assert TIME_ZONE_COLUMN in sdf.columns

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    sdf2 = table_to_pandas(sdf)
    res_df2 = table_to_pandas(res_df)
    assert res_df2[TIME_COLUMN].dt.tz is not None
    assert set(res_df2[TIME_COLUMN]) == set(sdf2[TIME_COLUMN].dt.tz_localize(tz))


@skip_unless_duckdb
def test_multi_tz_duckdb_distinct_zones(spark, tmp_path):
    """Two distinct zones in the time_zone column must localize per row, not with one
    shared zone. The same naive wall-clock 2018-01-01 00:00 yields 05:00 UTC under
    Etc/GMT+5 but 08:00 UTC under Etc/GMT+8, so the four rows must produce four distinct
    UTC instants. A regression that localized every row with a single zone would collapse
    these to two.
    """
    zones = ("Etc/GMT+5", "Etc/GMT+8")
    time_dim = make_datetime_config_multi_tz_ntz(time_zones=list(zones))
    config = DummyDatasetConfig(time_dim, geography_dim=None)

    sdf = _make_multi_tz_dataframe(spark, time_zones=zones)
    assert TIME_ZONE_COLUMN in sdf.columns

    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    src = table_to_pandas(sdf)
    out = table_to_pandas(res_df)
    assert out[TIME_COLUMN].dt.tz is not None
    expected_utc = {
        pd.Timestamp(row[TIME_COLUMN]).tz_localize(row[TIME_ZONE_COLUMN]).tz_convert("UTC")
        for _, row in src.iterrows()
    }
    got_utc = {pd.Timestamp(t).tz_convert("UTC") for t in out[TIME_COLUMN]}
    assert len(expected_utc) == 4  # 4 rows, 2 zones -> 4 distinct instants
    assert got_utc == expected_utc


@skip_unless_spark
def test_multi_tz_spark_path(spark, tmp_path):
    """Spark+Path multi-tz localization with TIME_ZONE_COLUMN already present.

    geography_dim is intentionally None: if add_time_zone were called it would fail.
    """
    time_dim = make_datetime_config_multi_tz_ntz(time_zones=["Etc/GMT+5"])
    config = DummyDatasetConfig(time_dim, geography_dim=None)

    sdf = _make_multi_tz_dataframe(spark, time_zones=("Etc/GMT+5",))
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )

    assert changed is True
    session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    res_df2 = table_to_pandas(res_df)

    tz = "Etc/GMT+5"
    sdf2 = table_to_pandas(sdf)
    assert set(res_df2[TIME_COLUMN].dt.tz_localize(session_tz)) == set(
        sdf2[TIME_COLUMN].dt.tz_localize(tz)
    )


def test_unknown_plan_raises(spark, tmp_path):
    class _UnknownPlanConfig(DateTimeDimensionConfig):
        def get_localization_plan(self):
            return "unknown_plan"

    time_dim = _UnknownPlanConfig.load_from_model(make_datetime_config_single_tz_ntz().model)
    config = DummyDatasetConfig(time_dim)
    sdf = _make_simple_dataframe(spark)
    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(
            sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
        )


def test_invalid_time_dimension_raises(spark, tmp_path):
    class NotDateTimeConfig:
        pass

    config = DummyDatasetConfig(time_dim=NotDateTimeConfig())
    sdf = _make_simple_dataframe(spark)
    with pytest.raises(DSGInvalidOperation):
        localize_timestamps_if_necessary(
            sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
        )


def test_ntz_no_time_zone_is_noop(spark, tmp_path):
    time_dim = make_datetime_config_single_aligned_no_tz_ntz()
    config = DummyDatasetConfig(time_dim)

    sdf = _make_simple_dataframe(spark)
    res_df, changed = localize_timestamps_if_necessary(
        sdf, config, scratch_dir_context=ScratchDirContext(tmp_path)
    )
    assert changed is False
    assert res_df is sdf
