"""Regression tests for `map_time_dimension_with_chronify_*`.

Covers the case where a dataset's registered time dimension declares
`column_format.dtype = "timestamp_ntz"` with `aligned_in_absolute_time` plus a
single time zone (so its `get_localization_plan()` returns
`"localize_to_single_tz"`), but the data has already been localized upstream
(e.g. the registered parquet on disk is `TIMESTAMP WITH TIME ZONE`). The chronify
schema produced by `from_time_dim.to_chronify()` reports `TIMESTAMP_NTZ` with a
tz-naive `start`, while the table's time column is tz-aware. Without
post-processing, chronify creates the mapping table as `TIMESTAMP_NTZ` and the
join fails (under DuckDB with `Conversion Error: Unimplemented type for cast
(TIMESTAMP WITH TIME ZONE -> TIMESTAMP_NS)`).

The fix lives in the shared `_get_src_schema` / `_get_dst_schema` helpers in
`dsgrid.utils.dataset`, so it covers both chronify map dispatchers (DuckDB and
runtime path). Those helpers take `data_is_localized` from the caller rather than
inferring it from the time column's dtype, which is what makes this test meaningful
on both backends: Spark's `TimestampType` is instant-only, so ibis reports
`Timestamp(timezone=None)` for tz-aware and naive data alike. While detection was
dtype-based, the adjustment never fired on Spark and the resulting offset between
chronify's mapping table and the data silently dropped rows -- all four here, and
`offset` hours' worth on a realistic range.

The test body is therefore backend-agnostic, but it compares *instants* rather than
rendered timestamps: DuckDB returns the mapped column tz-aware, while Spark returns
it naive, rendered in the Spark session time zone.
"""

from datetime import timedelta

import pandas as pd

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.dimensions import (
    AlignedTimeSingleTimeZone,
    DateTimeDimensionModel,
    TimeFormatDateTimeNTZModel,
    TimeRangeModel,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    MeasurementType,
    TimeDimensionType,
    TimeIntervalType,
    TimeZoneFormat,
)
from dsgrid.ibis.io import persist_table, read_dataframe
from dsgrid.ibis.session import get_runtime_session, get_spark_session
from dsgrid.ibis.table_utils import table_to_pandas
from dsgrid.ibis.types import use_duckdb
from dsgrid.utils.dataset import (
    map_time_dimension_with_chronify_duckdb,
    map_time_dimension_with_chronify_runtime_path,
)


def _map_time_dim(df, from_time_dim, to_time_dim, scratch_dir_context):
    """Dispatch to the chronify map function for the active backend."""
    if use_duckdb():
        return map_time_dimension_with_chronify_duckdb(
            df=df,
            from_time_dim=from_time_dim,
            to_time_dim=to_time_dim,
            scratch_dir_context=scratch_dir_context,
        )
    filename = persist_table(df, scratch_dir_context, tag="map_time_dim regression")
    return map_time_dimension_with_chronify_runtime_path(
        df=read_dataframe(filename),
        filename=filename,
        from_time_dim=from_time_dim,
        to_time_dim=to_time_dim,
        scratch_dir_context=scratch_dir_context,
    )


def _utc_instants(series: pd.Series) -> set:
    """Return the set of UTC instants represented by a mapped timestamp column.

    DuckDB returns the column tz-aware. Spark's ``TimestampType`` is instant-only, so
    the column comes back tz-naive, rendered in the Spark session time zone; read that
    zone back rather than assuming UTC, since a caller may have changed it (e.g. via
    ``dsgrid.ibis.tz.custom_time_zone``).
    """
    # DuckDB path
    if series.dt.tz is not None:
        return set(series.dt.tz_convert("UTC"))
    # Spark path (tz-naive, interpreted in session time zone)
    session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    return set(series.dt.tz_localize(session_tz).dt.tz_convert("UTC"))


def _make_ntz_single_tz_config(name: str, time_zone: str) -> DateTimeDimensionConfig:
    """Build a DateTime dim with `localize_to_single_tz` plan in effect."""
    return DateTimeDimensionConfig(
        DateTimeDimensionModel(
            name=name,
            description="test",
            class_name="Time",
            type=DimensionType.TIME,
            time_type=TimeDimensionType.DATETIME,
            measurement_type=MeasurementType.TOTAL,
            column_format=TimeFormatDateTimeNTZModel(),
            time_zone_format=AlignedTimeSingleTimeZone(
                format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
                time_zone=time_zone,
            ),
            ranges=[
                TimeRangeModel(
                    start="2018-01-01 00:00:00",
                    end="2018-01-01 03:00:00",
                    str_format="%Y-%m-%d %H:%M:%S",
                    frequency=timedelta(hours=1),
                )
            ],
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
        )
    )


def test_ntz_config_with_tz_aware_dataframe(scratch_dir_context):
    """from_time_dim says NTZ, but the table is post-localization (tz-aware).

    Before the fix, this raised a backend-specific cast error when chronify
    created its mapping table as `TIMESTAMP_NTZ` while the source column was
    `TIMESTAMP WITH TIME ZONE`. After the fix, chronify is handed a
    `TIMESTAMP_TZ` schema and the mapping succeeds.
    """
    tz = "Etc/GMT+5"
    from_time_dim = _make_ntz_single_tz_config("time_va_est", tz)
    to_time_dim = _make_ntz_single_tz_config("time_est", tz)

    # Sanity check — both dims are configured to land in the localize_to_single_tz path.
    assert from_time_dim.get_localization_plan() == "localize_to_single_tz"
    assert to_time_dim.get_localization_plan() == "localize_to_single_tz"

    # Mimic the post-localization shape: tz-aware time column.
    time_column = from_time_dim.get_load_data_time_columns()[0]
    timestamps = pd.date_range(
        start="2018-01-01 00:00:00",
        end="2018-01-01 03:00:00",
        freq="1h",
        tz=tz,
    )
    pdf = pd.DataFrame(
        {
            time_column: timestamps,
            "geography": ["g1"] * len(timestamps),
            VALUE_COLUMN: [1.0, 2.0, 3.0, 4.0],
        }
    )

    df = get_runtime_session().createDataFrame(pdf)

    mapped_df = _map_time_dim(df, from_time_dim, to_time_dim, scratch_dir_context)

    out = table_to_pandas(mapped_df)
    out_time_column = to_time_dim.get_load_data_time_columns()[0]
    assert out_time_column in out.columns
    # Every input row survives the mapping. This is the assertion that fails when the
    # config handed to chronify does not match the data: the mapping table is built
    # from a start that is off by the time zone offset, so the join drops rows.
    assert len(out) == len(timestamps)
    # Compare instants — display tz differs across backends, but the set of
    # represented moments must match.
    assert _utc_instants(out[out_time_column]) == set(timestamps.tz_convert("UTC"))
    assert out[VALUE_COLUMN].notna().all()
    assert set(out[VALUE_COLUMN]) == {1.0, 2.0, 3.0, 4.0}
