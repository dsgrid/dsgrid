import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.daily_time_dimension_config import (
    DailyTimeDimensionConfig,
)
from dsgrid.config.dimensions import (
    DailyTimeDimensionModel,
    DailyRangeModel,
)
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    TimeIntervalType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.spark.types import use_duckdb
from dsgrid.utils.spark import create_dataframe_from_dicts


@pytest.fixture(scope="module")
def daily_dataframe():
    """Create a daily time dataframe with time_year, month, day columns."""
    data = []
    # Add data for Jan 1-5, 2012
    for day in range(1, 6):
        data.append(
            {
                "time_year": 2012,
                "month": 1,
                "day": day,
                "geography": "CO",
                "value": 100.0 + day,
            }
        )
    # Add data for Feb 1-5, 2012
    for day in range(1, 6):
        data.append(
            {
                "time_year": 2012,
                "month": 2,
                "day": day,
                "geography": "CO",
                "value": 200.0 + day,
            }
        )

    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
        df.count()
    yield df


@pytest.fixture
def daily_time_dimension_basic():
    """Create a basic daily time dimension config for a single month."""
    yield DailyTimeDimensionConfig(
        DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time",
            description="test daily time",
            ranges=[
                DailyRangeModel(start="2012-01-01", end="2012-01-31"),
            ],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.NONE,
            year_column="time_year",
        )
    )


@pytest.fixture
def daily_time_dimension_leap_year():
    """Create a daily time dimension config that includes a leap day."""
    yield DailyTimeDimensionConfig(
        DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time_leap",
            description="test daily time with leap year",
            ranges=[
                DailyRangeModel(start="2012-02-25", end="2012-03-05"),
            ],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.NONE,
            year_column="time_year",
        )
    )


@pytest.fixture
def daily_time_dimension_drop_feb29():
    """Create a daily time dimension config that drops Feb 29."""
    yield DailyTimeDimensionConfig(
        DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time_no_leap",
            description="test daily time without leap day",
            ranges=[
                DailyRangeModel(start="2012-02-25", end="2012-03-05"),
            ],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.DROP_FEB29,
            year_column="time_year",
        )
    )


def test_daily_time_dimension_get_load_data_time_columns(daily_time_dimension_basic):
    """Test that get_load_data_time_columns returns the correct column names."""
    columns = daily_time_dimension_basic.get_load_data_time_columns()
    assert columns == ["time_year", "month", "day"]


def test_daily_time_dimension_get_year_column(daily_time_dimension_basic):
    """Test that get_year_column returns the configured year column."""
    year_col = daily_time_dimension_basic.get_year_column()
    assert year_col == "time_year"


def test_daily_time_dimension_with_weather_year():
    """Test daily time dimension with weather_year column."""
    config = DailyTimeDimensionConfig(
        DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time_weather",
            description="test daily time with weather year",
            ranges=[
                DailyRangeModel(start="2012-01-01", end="2012-01-31"),
            ],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.NONE,
            year_column="weather_year",
        )
    )
    columns = config.get_load_data_time_columns()
    assert columns == ["weather_year", "month", "day"]
    assert config.get_year_column() == "weather_year"


def test_daily_time_dimension_list_expected_timestamps(daily_time_dimension_basic):
    """Test that list_expected_dataset_timestamps returns correct timestamps."""
    timestamps = daily_time_dimension_basic.list_expected_dataset_timestamps()
    assert len(timestamps) == 31  # January has 31 days
    # Check first and last timestamp
    assert timestamps[0].time_year == 2012
    assert timestamps[0].month == 1
    assert timestamps[0].day == 1
    assert timestamps[-1].time_year == 2012
    assert timestamps[-1].month == 1
    assert timestamps[-1].day == 31


def test_daily_time_dimension_leap_day_none(daily_time_dimension_leap_year):
    """Test that leap day is included when adjustment is NONE."""
    timestamps = daily_time_dimension_leap_year.list_expected_dataset_timestamps()
    # Feb 25-29 (5 days) + Mar 1-5 (5 days) = 10 days
    assert len(timestamps) == 10
    # Check that Feb 29 is included
    feb_29 = [t for t in timestamps if t.month == 2 and t.day == 29]
    assert len(feb_29) == 1


def test_daily_time_dimension_leap_day_drop(daily_time_dimension_drop_feb29):
    """Test that leap day is dropped when adjustment is DROP_FEB29."""
    timestamps = daily_time_dimension_drop_feb29.list_expected_dataset_timestamps()
    # Feb 25-28 (4 days, skipping 29) + Mar 1-5 (5 days) = 9 days
    assert len(timestamps) == 9
    # Check that Feb 29 is NOT included
    feb_29 = [t for t in timestamps if t.month == 2 and t.day == 29]
    assert len(feb_29) == 0


def test_daily_time_dimension_get_time_ranges(daily_time_dimension_basic):
    """Test that get_time_ranges returns DailyTimeRange objects."""
    time_ranges = daily_time_dimension_basic.get_time_ranges()
    assert len(time_ranges) == 1
    time_range = time_ranges[0]

    # Check that we can iterate over the range
    timestamps = list(time_range.list_time_range())
    assert len(timestamps) == 31

    # Check first and last timestamp
    assert timestamps[0].year == 2012
    assert timestamps[0].month == 1
    assert timestamps[0].day == 1
    assert timestamps[-1].year == 2012
    assert timestamps[-1].month == 1
    assert timestamps[-1].day == 31


def test_daily_time_dimension_get_lengths(daily_time_dimension_basic):
    """Test that get_lengths returns correct number of days."""
    lengths = daily_time_dimension_basic.get_lengths()
    assert lengths == [31]  # January has 31 days


def test_daily_time_dimension_get_start_times(daily_time_dimension_basic):
    """Test that get_start_times returns correct start timestamps."""
    start_times = daily_time_dimension_basic.get_start_times()
    assert len(start_times) == 1
    assert start_times[0].year == 2012
    assert start_times[0].month == 1
    assert start_times[0].day == 1


def test_daily_time_dimension_build_time_dataframe(daily_time_dimension_basic):
    """Test that build_time_dataframe creates correct DataFrame."""
    df = daily_time_dimension_basic.build_time_dataframe()
    assert df.count() == 31

    # Check that all required columns are present
    columns = set(df.columns)
    assert "time_year" in columns
    assert "month" in columns
    assert "day" in columns

    # Check first row
    first_row = df.orderBy("time_year", "month", "day").first()
    assert first_row.time_year == 2012
    assert first_row.month == 1
    assert first_row.day == 1


def test_daily_time_dimension_check_consistency_valid(daily_dataframe, daily_time_dimension_basic):
    """Test that check_dataset_time_consistency passes for valid data."""
    # Filter to only January data to match the dimension config
    df_jan = daily_dataframe.filter("month = 1")

    # Modify the dimension to match the data
    daily_time_dimension_basic.model.ranges = [
        DailyRangeModel(start="2012-01-01", end="2012-01-05")
    ]

    time_columns = daily_time_dimension_basic.get_load_data_time_columns()
    # This should not raise an exception
    daily_time_dimension_basic.check_dataset_time_consistency(df_jan, time_columns)


def test_daily_time_dimension_check_consistency_invalid(
    daily_dataframe, daily_time_dimension_basic
):
    """Test that check_dataset_time_consistency fails for invalid data."""
    # Use February data with a January dimension config
    df_feb = daily_dataframe.filter("month = 2")

    # Set dimension to only expect January
    daily_time_dimension_basic.model.ranges = [
        DailyRangeModel(start="2012-01-01", end="2012-01-05")
    ]

    time_columns = daily_time_dimension_basic.get_load_data_time_columns()
    # This should raise an exception because data has Feb dates but dimension expects Jan
    with pytest.raises(DSGInvalidDataset):
        daily_time_dimension_basic.check_dataset_time_consistency(df_feb, time_columns)


def test_daily_range_model_validation():
    """Test that DailyRangeModel validates date formats."""
    # Valid date range
    valid_range = DailyRangeModel(start="2012-01-01", end="2012-01-31")
    assert valid_range.start == "2012-01-01"
    assert valid_range.end == "2012-01-31"


def test_daily_time_dimension_model_year_column_validation():
    """Test that DailyTimeDimensionModel validates year_column field."""
    # Valid year_column values
    for year_col in ["time_year", "weather_year", "model_year"]:
        config = DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time",
            description="test",
            ranges=[DailyRangeModel(start="2012-01-01", end="2012-01-31")],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.NONE,
            year_column=year_col,
        )
        assert config.year_column == year_col

    # Invalid year_column should raise validation error
    with pytest.raises(ValueError):
        DailyTimeDimensionModel(
            dimension_type=DimensionType.TIME,
            class_name="DailyTime",
            module="dsgrid.dimension.standard",
            name="daily_time",
            description="test",
            ranges=[DailyRangeModel(start="2012-01-01", end="2012-01-31")],
            str_format="%Y-%m-%d",
            time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
            leap_day_adjustment=LeapDayAdjustmentType.NONE,
            year_column="invalid_column",
        )


def test_daily_time_dimension_supports_chronify(daily_time_dimension_basic):
    """Test that daily time dimension does not support chronify yet."""
    assert daily_time_dimension_basic.supports_chronify() is False
