from typing import Any

import ibis
import pytest
from chronify.time_range_generator_factory import make_time_range_generator

from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.annual_time_dimension_config import (
    AnnualTimeDimensionConfig,
    AnnualTimeDimensionModel,
    map_annual_time_to_date_time,
)
from dsgrid.config.date_time_dimension_config import (
    DateTimeDimensionConfig,
    DateTimeDimensionModel,
)
from dsgrid.config.dimensions import AlignedTimeSingleTimeZone, AnnualRangeModel, TimeRangeModel
from dsgrid.dimension.time import (
    TimeZoneFormat,
    MeasurementType,
    TimeIntervalType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.utils.dataset import check_historical_annual_time_model_year_consistency
from dsgrid.ibis.functions import cache, unpersist
from dsgrid.ibis.session import (
    F,
    create_dataframe_from_dicts,
    get_runtime_session,
    use_duckdb,
)

from dsgrid.ibis.table_utils import count_rows
from dsgrid.ibis.types import is_tz_aware_timestamp
from tests._helpers import collect as _collect


def _count_timestamps_per_model_year(df, time_col: str):
    if isinstance(df, ibis.Table):
        return _collect(
            df.group_by("model_year")
            .aggregate(count_timestamps=df[time_col].count())
            .select("count_timestamps")
            .distinct()
        )
    return (
        df.groupBy("model_year")
        .agg(F.count(time_col).alias("count_timestamps"))
        .select("count_timestamps")
        .distinct()
        .collect()
    )


@pytest.fixture(scope="module")
def annual_dataframe():
    data = [
        {
            "time_year": 2019,
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
        {
            "time_year": 2021,
            "geography": "CO",
            "electricity_sales": 802872.1,
        },
        {
            "time_year": 2022,
            "geography": "CO",
            "electricity_sales": 902872.1,
        },
    ]

    df = cache(create_dataframe_from_dicts(data))
    yield df
    unpersist(df)


@pytest.fixture
def annual_dataframe_with_model_year_values():
    yield [
        {
            "time_year": 2019,
            "model_year": 2019,
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "model_year": 2020,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
        {
            "time_year": 2021,
            "model_year": 2021,
            "geography": "CO",
            "electricity_sales": 802872.1,
        },
        {
            "time_year": None,
            "model_year": 2022,
            "geography": "CO",
            "electricity_sales": 902872.1,
        },
    ]


@pytest.fixture
def time_array_rows() -> list[dict[str, Any]]:
    """Two geographies, each covering the same two years: aligned time arrays."""
    return [
        {"time_year": 2020, "geography": "CO", "electricity_sales": 602872.1},
        {"time_year": 2021, "geography": "CO", "electricity_sales": 702872.1},
        {"time_year": 2020, "geography": "NM", "electricity_sales": 802872.1},
        {"time_year": 2021, "geography": "NM", "electricity_sales": 902872.1},
    ]


def _make_time_array_df(rows: list[dict[str, Any]]) -> ibis.Table:
    # No cache(): each caller reads the result exactly once, so caching would add
    # a materialization with nothing to amortize it against.
    return create_dataframe_from_dicts(rows)


def _check_time_arrays(df: ibis.Table) -> None:
    DatasetSchemaHandlerBase._check_dataset_time_consistency_by_time_array(["time_year"], df)


@pytest.fixture
def annual_dataframe_with_model_year_valid(annual_dataframe_with_model_year_values):
    data = annual_dataframe_with_model_year_values
    df = cache(create_dataframe_from_dicts(data))
    yield df, "time_year", "model_year"
    unpersist(df)


@pytest.fixture
def annual_dataframe_with_model_year_invalid(annual_dataframe_with_model_year_values):
    data = annual_dataframe_with_model_year_values
    data.append(
        {
            "time_year": 2023,
            "model_year": 2019,
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
    )
    df = cache(create_dataframe_from_dicts(data))
    yield df, "time_year", "model_year"
    unpersist(df)


@pytest.fixture
def annual_time_dimension():
    yield AnnualTimeDimensionConfig(
        AnnualTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="AnnualTime",
            module="dsgrid.dimension.standard",
            name="annual_time",
            description="test annual time",
            ranges=[
                AnnualRangeModel(start="2010", end="2020", str_format="%Y"),
            ],
            measurement_type=MeasurementType.TOTAL,
            include_leap_day=True,
        )
    )


@pytest.fixture
def date_time_dimension():
    yield DateTimeDimensionConfig(
        DateTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="Time",
            module="dsgrid.dimension.standard",
            time_zone_format=AlignedTimeSingleTimeZone(
                format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
                time_zone="Etc/GMT+4",
            ),
            name="datetime",
            description="example date time",
            ranges=[
                TimeRangeModel(
                    start="2012-02-01 00:00:00",
                    end="2012-02-07 23:00:00",
                    frequency="P0DT1H",
                    str_format="%Y-%m-%d %H:%M:%S",
                ),
            ],
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            measurement_type=MeasurementType.TOTAL,
        )
    )


def test_map_annual_time_total_to_datetime(
    annual_dataframe, annual_time_dimension, date_time_dimension
):
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    value_columns = {"electricity_sales"}
    df = map_annual_time_to_date_time(
        annual_dataframe,
        annual_time_dimension,
        date_time_dimension,
        value_columns,
    )
    expected_by_year = {
        x.time_year: x.electricity_sales / (366 * 24) for x in _collect(annual_dataframe)
    }
    num_rows = count_rows(annual_dataframe)
    num_timestamps = 24 * 7
    assert count_rows(df) == num_rows * num_timestamps
    values = _collect(df.select("model_year", "electricity_sales").distinct())
    assert len(values) == num_rows
    by_year = {x.model_year: x.electricity_sales for x in values}
    assert len(by_year) == len(expected_by_year)
    for year in by_year:
        assert by_year[year] == expected_by_year[int(year)]

    time_col = date_time_dimension.get_load_data_time_columns()[0]
    count_timestamps_per_model_year = _count_timestamps_per_model_year(df, time_col)
    assert len(count_timestamps_per_model_year) == 1
    assert count_timestamps_per_model_year[0].count_timestamps == num_timestamps


def test_map_annual_time_total_to_datetime_with_existing_model_year(
    annual_time_dimension, date_time_dimension
):
    """Verify map_annual_time_to_date_time preserves a model_year column when it is
    already present on the input table and its values match the annual time column
    (the branch where a new model_year is NOT added)."""
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    data = [
        {
            "time_year": 2019,
            "model_year": "2019",
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "model_year": "2020",
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
    ]
    df = create_dataframe_from_dicts(data)
    value_columns = {"electricity_sales"}
    out = map_annual_time_to_date_time(
        df, annual_time_dimension, date_time_dimension, value_columns
    )
    # The pre-existing model_year column is preserved. Each input row expands to
    # (24 * 7) timestamps.
    assert "model_year" in out.columns
    assert "time_year" not in out.columns
    pairs = _collect(out.select("model_year", "electricity_sales").distinct())
    by_model_year = {row.model_year: row.electricity_sales for row in pairs}
    assert set(by_model_year) == {"2019", "2020"}
    expected_divisor = 366 * 24  # leap day enabled in this fixture
    assert by_model_year["2019"] == pytest.approx(602872.1 / expected_divisor)
    assert by_model_year["2020"] == pytest.approx(702872.1 / expected_divisor)


def test_map_annual_time_total_to_datetime_with_mismatched_model_year(
    annual_time_dimension, date_time_dimension
):
    """An existing model_year column whose values disagree with the annual time
    column must be rejected."""
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    data = [
        {
            "time_year": 2019,
            "model_year": "2030",
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
    ]
    df = create_dataframe_from_dicts(data)
    with pytest.raises(DSGInvalidDataset, match="model_year"):
        map_annual_time_to_date_time(
            df, annual_time_dimension, date_time_dimension, {"electricity_sales"}
        )


@pytest.fixture
def date_time_dimension_year_boundary():
    """A DateTime dimension whose hours straddle the UTC New-Year boundary.

    ``America/Los_Angeles`` local ``2020-12-31 14:00..23:00`` is ``2020-12-31 22:00
    UTC .. 2021-01-01 07:00 UTC``: it spans UTC years ``{2020, 2021}`` but a single
    local year 2020 (a leap year). The annual->datetime map must resolve ``.year()`` in
    the dimension's own TZ; if it ever regresses to extracting in UTC (e.g. naive DuckDB
    timestamps), it sees two years and raises, or picks the non-leap divisor.
    """
    yield DateTimeDimensionConfig(
        DateTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="Time",
            module="dsgrid.dimension.standard",
            time_zone_format=AlignedTimeSingleTimeZone(
                format_type=TimeZoneFormat.ALIGNED_IN_ABSOLUTE_TIME,
                time_zone="America/Los_Angeles",
            ),
            name="datetime",
            description="year-boundary straddle",
            ranges=[
                TimeRangeModel(
                    start="2020-12-31 14:00:00",
                    end="2020-12-31 23:00:00",
                    frequency="P0DT1H",
                    str_format="%Y-%m-%d %H:%M:%S",
                ),
            ],
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            measurement_type=MeasurementType.TOTAL,
        )
    )


def test_datetime_chronify_timestamps_are_tz_aware(date_time_dimension_year_boundary):
    """Pin the dtype the annual->datetime map relies on.

    ``map_annual_time_to_date_time`` builds its DateTime dataframe from chronify's
    ``list_timestamps()`` and extracts ``.year()`` under ``custom_time_zone``. On DuckDB
    that only works if the column is TZ-aware (``timestamp with time zone``); a regression
    to a naive ``TIMESTAMP`` would silently ignore the time zone. On Spark the type carries
    no naive/aware distinction but is always rendered via the session TZ.
    """
    dt_dim = date_time_dimension_year_boundary
    timestamps = make_time_range_generator(dt_dim.to_chronify()).list_timestamps()
    time_col = dt_dim.get_load_data_time_columns()[0]
    dt_df = get_runtime_session().createDataFrame(
        [(x.to_pydatetime(),) for x in timestamps], schema=[time_col]
    )
    dtype = dt_df.schema()[time_col]
    if use_duckdb():
        assert is_tz_aware_timestamp(dtype)
    else:
        # Spark timestamps are instant-based and report as tz-naive but render via the
        # session TZ, which is what the map relies on.
        assert dtype.is_timestamp()


def test_map_annual_time_leap_year_tz_boundary(
    annual_dataframe, annual_time_dimension, date_time_dimension_year_boundary
):
    """Regression test for time-zone resolution in the annual->datetime map on both backends.

    The DateTime dimension's hours span two UTC years but one local (leap) year. The map
    must (1) not raise "more than one year" and (2) divide annual totals by the leap-year
    divisor ``366 * 24`` -- both of which are only correct if ``.year()`` resolved in
    ``America/Los_Angeles`` rather than UTC.
    """
    annual_time_dimension.model.include_leap_day = True
    value_columns = {"electricity_sales"}
    df = map_annual_time_to_date_time(
        annual_dataframe,
        annual_time_dimension,
        date_time_dimension_year_boundary,
        value_columns,
    )
    num_timestamps = 10  # 2020-12-31 14:00..23:00, hourly and inclusive
    leap_divisor = 366 * 24
    expected_by_year = {
        str(x.time_year): x.electricity_sales / leap_divisor for x in _collect(annual_dataframe)
    }
    num_rows = count_rows(annual_dataframe)
    assert count_rows(df) == num_rows * num_timestamps
    values = _collect(df.select("model_year", "electricity_sales").distinct())
    by_year = {x.model_year: x.electricity_sales for x in values}
    assert set(by_year) == set(expected_by_year)
    for year, value in by_year.items():
        assert value == pytest.approx(expected_by_year[year])


def test_historical_annual_model_year_consistency_valid(annual_dataframe_with_model_year_valid):
    df, time_col, model_year_col = annual_dataframe_with_model_year_valid
    check_historical_annual_time_model_year_consistency(df, time_col, model_year_col)


def test_historical_annual_model_year_consistency_invalid(
    annual_dataframe_with_model_year_invalid,
):
    df, time_col, model_year_col = annual_dataframe_with_model_year_invalid
    with pytest.raises(DSGInvalidDataset):
        check_historical_annual_time_model_year_consistency(df, time_col, model_year_col)


def test_time_array_consistency_valid(time_array_rows):
    """Both geographies cover both years, so the time arrays are aligned."""
    _check_time_arrays(_make_time_array_df(time_array_rows))


def test_time_array_consistency_uneven_timestamp_repeats(time_array_rows):
    """Dropping NM's 2021 row leaves 2020 repeated twice and 2021 only once."""
    rows = [x for x in time_array_rows if not (x["geography"] == "NM" and x["time_year"] == 2021)]
    with pytest.raises(DSGInvalidDataset, match="unique timestamp repeats = 2"):
        _check_time_arrays(_make_time_array_df(rows))


def test_time_array_consistency_ragged_time_arrays(time_array_rows):
    """Reassigning NM's 2021 row to UT leaves every timestamp repeated exactly twice,
    but CO now has a two-year array while NM and UT each have a one-year array.

    The timestamp-repeat check cannot see this, which is why the time-array-length
    check exists as a separate condition.
    """
    rows = [dict(x) for x in time_array_rows]
    for row in rows:
        if row["geography"] == "NM" and row["time_year"] == 2021:
            row["geography"] = "UT"
    with pytest.raises(DSGInvalidDataset, match="unique time array lengths = 2"):
        _check_time_arrays(_make_time_array_df(rows))


def test_time_array_consistency_empty_table(time_array_rows):
    """An empty table has zero distinct timestamp repeat counts, not one."""
    df = _make_time_array_df(time_array_rows)
    with pytest.raises(DSGInvalidDataset, match="unique timestamp repeats = 0"):
        _check_time_arrays(df.filter(df["time_year"] == 9999))


def test_time_array_consistency_no_dimension_columns(time_array_rows):
    """With no dimension columns the time array is the whole table, which is always
    self-consistent."""
    rows = [
        {"time_year": x["time_year"], "electricity_sales": x["electricity_sales"]}
        for x in time_array_rows
        if x["geography"] == "CO"
    ]
    _check_time_arrays(_make_time_array_df(rows))
