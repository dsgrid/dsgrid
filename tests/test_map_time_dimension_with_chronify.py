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
runtime path). The test body is backend-agnostic: it dispatches via
`_map_time_dim` to whichever chronify map function matches the active backend.
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
from dsgrid.ibis.session import get_runtime_session
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
    # Confirm tz-awareness propagated through the backend ingestion.
    assert table_to_pandas(df)[time_column].dt.tz is not None

    mapped_df = _map_time_dim(df, from_time_dim, to_time_dim, scratch_dir_context)

    out = table_to_pandas(mapped_df)
    out_time_column = to_time_dim.get_load_data_time_columns()[0]
    assert out_time_column in out.columns
    assert out[out_time_column].dt.tz is not None
    # Compare instants — display tz can differ across backends/sessions, but the
    # set of represented moments must match.
    assert set(out[out_time_column]) == set(timestamps)
    assert out[VALUE_COLUMN].notna().all()
    assert set(out[VALUE_COLUMN]) == {1.0, 2.0, 3.0, 4.0}
