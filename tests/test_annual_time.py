import ibis
import pytest
from chronify.time_range_generator_factory import make_time_range_generator

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
from dsgrid.ibis.session import (
    F,
    create_dataframe_from_dicts,
    get_runtime_session,
    use_duckdb,
)

from dsgrid.ibis.table_utils import count_rows
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

    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
        count_rows(df)
    yield df


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
def annual_dataframe_with_model_year_valid(annual_dataframe_with_model_year_values):
    data = annual_dataframe_with_model_year_values
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, "time_year", "model_year"


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
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, "time_year", "model_year"


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
    already present on the input table (the branch where a new model_year is NOT added)."""
    annual_time_dimension.model.measurement_type = MeasurementType.TOTAL
    data = [
        {
            "time_year": 2019,
            "model_year": "2030",
            "geography": "CO",
            "electricity_sales": 602872.1,
        },
        {
            "time_year": 2020,
            "model_year": "2031",
            "geography": "CO",
            "electricity_sales": 702872.1,
        },
    ]
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    value_columns = {"electricity_sales"}
    out = map_annual_time_to_date_time(
        df, annual_time_dimension, date_time_dimension, value_columns
    )
    # The pre-existing model_year values must be preserved (not overwritten by the annual
    # time year cast). Each input row expands to (24 * 7) timestamps.
    assert "model_year" in out.columns
    assert "time_year" not in out.columns
    pairs = _collect(out.select("model_year", "electricity_sales").distinct())
    by_model_year = {row.model_year: row.electricity_sales for row in pairs}
    assert set(by_model_year) == {"2030", "2031"}
    expected_divisor = 366 * 24  # leap day enabled in this fixture
    assert by_model_year["2030"] == pytest.approx(602872.1 / expected_divisor)
    assert by_model_year["2031"] == pytest.approx(702872.1 / expected_divisor)


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
    if use_duckdb():
        assert str(dt_df.schema()[time_col]).startswith("timestamp('UTC'")
    else:
        assert str(dt_df.schema()[time_col]).startswith("timestamp")


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
