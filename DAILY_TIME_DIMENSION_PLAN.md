# Complete Plan for Adding DAILY TimeDimensionType

## Overview
The DAILY time dimension represents data with integer month and day columns, where the **year is explicitly specified in the dimension definition** using date ranges. The load data contains `time_month` and `time_day` columns, with the year coming from a configurable dimension column (`time_year`, `weather_year`, or `model_year`).

## Primary Use Case
**Different data for each weather year:**
```json
{
  "type": "time",
  "time_type": "daily",
  "year_column": "weather_year",
  "ranges": [
    {"start": "2012-01-01", "end": "2012-12-31"},
    {"start": "2013-01-01", "end": "2013-12-31"}
  ],
  "leap_day_adjustment": "none"
}
```

Load data:
```
time_month, time_day, weather_year, geography, value
1,          1,        2012,         "CO",      100.0
1,          2,        2012,         "CO",      101.0
1,          1,        2013,         "CO",      200.0  # Different value for 2013
```

---

## Implementation Steps

### 1. **Core Enum and Type Definitions**

#### 1.1 Update `dsgrid/dimension/time.py`
Add `DAILY = "daily"` to `TimeDimensionType` enum (line 15-22):

```python
class TimeDimensionType(DSGEnum):
    """Defines the supported time formats in the load data."""

    DATETIME = "datetime"
    ANNUAL = "annual"
    REPRESENTATIVE_PERIOD = "representative_period"
    INDEX = "index"
    NOOP = "noop"
    DAILY = "daily"
```

#### 1.2 Create new timestamp type in `dsgrid/time/types.py`
Add after `AnnualTimestampType` (around line 40):

```python
class DailyTimestampType(NamedTuple):
    """Columns for daily time with year, month, and day."""
    time_year: int
    time_month: int
    time_day: int
```

### 2. **Range Model**

#### 2.1 Add `DailyRangeModel` to `dsgrid/config/dimensions.py` (around line 270, after `MonthRangeModel`)

```python
class DailyRangeModel(DSGBaseModel):
    """Defines a continuous range of daily time."""

    start: str = Field(
        title="start",
        description="First date in the data (e.g., '2018-01-01' or '2018-06-01' for partial year)",
    )
    end: str = Field(
        title="end",
        description="Last date in the data (inclusive, e.g., '2018-12-31' or '2018-08-31' for partial year)",
    )
```

**Note**: Supports partial years (e.g., summer-only: "2018-06-01" to "2018-08-31")

### 3. **Data Model Definition**

#### 3.1 Create `DailyTimeDimensionModel` in `dsgrid/config/dimensions.py` (around line 510, after `AnnualTimeDimensionModel`)

```python
class DailyTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a daily time dimension where timestamps are year-month-day."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.DAILY)
    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            e.g., mean, total
        """,
        json_schema_extra={
            "options": MeasurementType.format_for_docs(),
        },
    )
    str_format: str = Field(
        title="str_format",
        default="%Y-%m-%d",
        description="Timestamp string format. "
        "The string format is used to parse the timestamps provided in the time ranges. "
        "Cheatsheet reference: `<https://strftime.org/>`_.",
    )
    ranges: list[DailyRangeModel] = Field(
        default=[],
        title="time_ranges",
        description="Defines the contiguous ranges of daily time in the data, inclusive of start and end time. "
        "Supports partial years (e.g., summer only).",
    )
    leap_day_adjustment: LeapDayAdjustmentType = Field(
        title="leap_day_adjustment",
        default=LeapDayAdjustmentType.NONE,
        description="Leap day adjustment method applied to time data. Options: none (include leap day), "
        "drop_feb29, drop_dec31, drop_jan1.",
        json_schema_extra={
            "options": LeapDayAdjustmentType.format_descriptions_for_docs(),
        },
    )
    time_interval_type: TimeIntervalType = Field(
        title="time_interval",
        default=TimeIntervalType.PERIOD_BEGINNING,
        description="The range of time that the value associated with a timestamp represents",
        json_schema_extra={
            "options": TimeIntervalType.format_for_docs(),
        },
    )
    year_column: str = Field(
        title="year_column",
        default="time_year",
        description="The column name containing year values in the load data. "
        "Options: 'time_year' (default), 'weather_year', 'model_year'. "
        "This column must exist in the dataset and will be combined with time_month and time_day.",
    )

    @field_validator("ranges")
    @classmethod
    def check_times(
        cls, ranges: list[DailyRangeModel], info: ValidationInfo
    ) -> list[DailyRangeModel]:
        if "str_format" not in info.data:
            return ranges
        return _check_daily_ranges(ranges, info.data["str_format"])

    @field_validator("year_column")
    @classmethod
    def check_year_column(cls, year_column: str) -> str:
        valid_columns = {"time_year", "weather_year", "model_year"}
        if year_column not in valid_columns:
            msg = f"year_column must be one of {valid_columns}, got: {year_column}"
            raise ValueError(msg)
        return year_column

    def is_time_zone_required_in_geography(self) -> bool:
        return False
```

#### 3.2 Add validation function `_check_daily_ranges()` (around line 700, after `_check_annual_ranges`)

```python
def _check_daily_ranges(ranges: list[DailyRangeModel], str_format: str):
    """Check that daily ranges are valid.

    Validates:
    - Date strings can be parsed with the given format
    - Start date is before or equal to end date
    - Dates are valid (e.g., rejects Feb 30)

    Note: Python's datetime.strptime() automatically validates dates,
    rejecting invalid dates like Feb 30.
    """
    for time_range in ranges:
        try:
            start = datetime.strptime(time_range.start, str_format)
            end = datetime.strptime(time_range.end, str_format)
        except ValueError as exc:
            msg = (
                f"Failed to parse time range with {str_format=}. "
                f"Invalid date or format: {exc}"
            )
            raise ValueError(msg) from exc

        if start > end:
            msg = f"start={time_range.start} is after end={time_range.end}"
            raise ValueError(msg)

    return ranges
```

#### 3.3 Update `DimensionsListModel` union (around line 624)

```python
DimensionsListModel = Annotated[
    list[
        Union[
            DimensionModel,
            DateTimeDimensionModel,
            AnnualTimeDimensionModel,
            DailyTimeDimensionModel,
            RepresentativePeriodTimeDimensionModel,
            IndexTimeDimensionModel,
            NoOpTimeDimensionModel,
        ]
    ],
    BeforeValidator(handle_dimension_union),
]
```

#### 3.4 Update `handle_dimension_union` function (around line 658)

```python
def handle_dimension_union(values):
    values = copy.deepcopy(values)
    for i, value in enumerate(values):
        if isinstance(value, DimensionBaseModel):
            continue

        dim_type = value.get("type")
        if dim_type is None:
            dim_type = value["dimension_type"]

        if dim_type == DimensionType.TIME.value:
            if value["time_type"] == TimeDimensionType.DATETIME.value:
                values[i] = DateTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.ANNUAL.value:
                values[i] = AnnualTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.DAILY.value:
                values[i] = DailyTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
                values[i] = RepresentativePeriodTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.INDEX.value:
                values[i] = IndexTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.NOOP.value:
                values[i] = NoOpTimeDimensionModel(**value)
            # ... rest of function
```

### 4. **Time Range Class**

#### 4.1 Add `DailyTimeRange` to `dsgrid/dimension/time.py` (after `AnnualTimeRange`, around line 470)

```python
class DailyTimeRange(DatetimeRange):
    """Time range for daily time dimension.

    Iterates through each day in the specified range, respecting leap day
    adjustment settings. Supports partial years.
    """

    def _iter_timestamps(self):
        """
        Return a generator of datetime objects for each day in the range.
        Respects leap_day_adjustment setting.

        Yields
        ------
        datetime
        """
        start = self.start.to_pydatetime()
        end = self.end.to_pydatetime()
        current = start

        while current <= end:
            month = current.month
            day = current.day

            # Apply leap day adjustment
            skip = False
            if self.leap_day_adjustment == LeapDayAdjustmentType.DROP_FEB29:
                if month == 2 and day == 29:
                    skip = True
            elif self.leap_day_adjustment == LeapDayAdjustmentType.DROP_DEC31:
                if month == 12 and day == 31:
                    skip = True
            elif self.leap_day_adjustment == LeapDayAdjustmentType.DROP_JAN1:
                if month == 1 and day == 1:
                    skip = True

            if not skip:
                yield datetime(year=current.year, month=month, day=day, tzinfo=self.tzinfo)

            current += timedelta(days=1)
```

### 5. **Configuration Class**

#### 5.1 Create `dsgrid/config/daily_time_dimension_config.py`

```python
import logging
from datetime import timedelta, datetime

import pandas as pd

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import DailyTimeRange, LeapDayAdjustmentType, TimeBasedDataAdjustmentModel
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.time.types import DailyTimestampType
from dsgrid.spark.types import (
    DataFrame,
    StructType,
    StructField,
    IntegerType,
    F,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.spark import get_spark_session
from .dimensions import DailyTimeDimensionModel
from .time_dimension_base_config import TimeDimensionBaseConfig


logger = logging.getLogger(__name__)


class DailyTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a DailyTimeDimensionModel."""

    @staticmethod
    def model_class() -> DailyTimeDimensionModel:
        return DailyTimeDimensionModel

    def supports_chronify(self) -> bool:
        """Chronify support not yet implemented for daily time.

        To add chronify support, the chronify package would need:
        1. A DailyTime or MonthDayTime model class
        2. Logic to handle separate month/day/year columns
        3. Support for leap day adjustments
        4. Time mapping capabilities for daily <-> datetime conversions
        """
        return False

    @track_timing(timer_stats_collector)
    def check_dataset_time_consistency(self, load_data_df, time_columns) -> None:
        logger.info("Check DailyTimeDimensionConfig dataset time consistency.")

        # Expect time_month and time_day columns
        expected_cols = ["time_month", "time_day"]
        for col in expected_cols:
            if col not in time_columns:
                msg = f"Expected column {col} not found in time_columns: {time_columns}"
                raise ValueError(msg)

        # Get distinct month/day combinations from load data
        actual_timestamps = [
            (row["time_month"], row["time_day"])
            for row in load_data_df.select("time_month", "time_day")
            .distinct()
            .filter("time_month IS NOT NULL AND time_day IS NOT NULL")
            .collect()
        ]

        # Get expected timestamps (month, day pairs)
        expected_timestamps_full = self.list_expected_dataset_timestamps()
        # Extract unique (month, day) pairs
        expected_timestamps = list(set(
            (ts.time_month, ts.time_day) for ts in expected_timestamps_full
        ))

        actual_set = set(actual_timestamps)
        expected_set = set(expected_timestamps)

        if actual_set != expected_set:
            missing = expected_set - actual_set
            extra = actual_set - expected_set
            msg = f"load_data time columns do not match expected times.\n"
            if missing:
                msg += f"  Missing: {sorted(missing)[:10]}\n"
            if extra:
                msg += f"  Extra: {sorted(extra)[:10]}"
            raise DSGInvalidDataset(msg)

    def build_time_dataframe(self) -> DataFrame:
        """Build time dimension DataFrame with month and day columns."""
        time_cols = self.get_load_data_time_columns()
        assert len(time_cols) == 2, time_cols

        schema = StructType([
            StructField("time_month", IntegerType(), False),
            StructField("time_day", IntegerType(), False),
        ])

        model_time = self.list_expected_dataset_timestamps()
        # Get unique (month, day) pairs
        unique_days = list(set((ts.time_month, ts.time_day) for ts in model_time))

        df_time = get_spark_session().createDataFrame(unique_days, schema=schema)
        return df_time

    def get_frequency(self) -> timedelta:
        return timedelta(days=1)

    def get_time_ranges(self) -> list[DailyTimeRange]:
        """Build DailyTimeRange objects from the model's ranges."""
        ranges = []
        for start_str, end_str in self._build_time_ranges(
            self.model.ranges, self.model.str_format, tz=None
        ):
            start = pd.Timestamp(start_str)
            end = pd.Timestamp(end_str)

            ranges.append(
                DailyTimeRange(
                    start=start,
                    end=end,
                    frequency=self.get_frequency(),
                    time_based_data_adjustment=TimeBasedDataAdjustmentModel(
                        leap_day_adjustment=self.model.leap_day_adjustment
                    ),
                )
            )
        return ranges

    def get_start_times(self) -> list[pd.Timestamp]:
        """Get the starting timestamp for each range."""
        start_times = []
        for trange in self.model.ranges:
            start = datetime.strptime(trange.start, self.model.str_format)
            start_times.append(pd.Timestamp(start))
        return start_times

    def get_lengths(self) -> list[int]:
        """Get the number of days in each range."""
        lengths = []
        for time_range in self.get_time_ranges():
            lengths.append(len(time_range.list_time_range()))
        return lengths

    def get_load_data_time_columns(self) -> list[str]:
        """Return the required timestamp columns in the load data table.

        Note: The year column (time_year, weather_year, or model_year) is
        specified separately via get_year_column() and is expected to exist
        as a dimension column in the dataset.
        """
        return ["time_month", "time_day"]

    def get_year_column(self) -> str:
        """Return the column name that contains the year values.

        This column must exist in the load data and will be combined with
        time_month and time_day to form complete dates.
        """
        return self.model.year_column

    def get_time_zone(self) -> None:
        """Daily time doesn't use time zones."""
        return None

    def get_tzinfo(self) -> None:
        """Daily time doesn't use tzinfo."""
        return None

    def get_time_interval_type(self):
        """Return the time interval type."""
        return self.model.time_interval_type

    def list_expected_dataset_timestamps(self) -> list[DailyTimestampType]:
        """Return a list of the timestamps expected in the load_data table."""
        timestamps = []
        for time_range in self.get_time_ranges():
            for dt in time_range.list_time_range():
                timestamps.append(
                    DailyTimestampType(
                        time_year=dt.year,
                        time_month=dt.month,
                        time_day=dt.day,
                    )
                )
        return timestamps
```

### 6. **Factory Integration**

#### 6.1 Update `dsgrid/config/dimension_config_factory.py`

Add imports at top:
```python
from .daily_time_dimension_config import DailyTimeDimensionConfig
from .dimensions import DailyTimeDimensionModel
```

Update `get_dimension_config()` function (around line 30):
```python
def get_dimension_config(model):
    if isinstance(model, DateTimeDimensionModel):
        return DateTimeDimensionConfig(model)
    if isinstance(model, AnnualTimeDimensionModel):
        return AnnualTimeDimensionConfig(model)
    if isinstance(model, DailyTimeDimensionModel):
        return DailyTimeDimensionConfig(model)
    if isinstance(model, RepresentativePeriodTimeDimensionModel):
        return RepresentativePeriodTimeDimensionConfig(model)
    # ... rest
```

Update `load_dimension_config()` function (around line 60):
```python
def load_dimension_config(filename):
    data = load_data(filename)
    if data["type"] == DimensionType.TIME.value:
        if data["time_type"] == TimeDimensionType.DATETIME.value:
            return DateTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.ANNUAL.value:
            return AnnualTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.DAILY.value:
            return DailyTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
            return RepresentativePeriodTimeDimensionConfig.load(filename)
        # ... rest
```

### 7. **Standard Dimension Classes**

#### 7.1 Update `dsgrid/dimension/standard.py` (around line 240, after `AnnualTime`)

Add import:
```python
from dsgrid.config.dimensions import DailyTimeDimensionModel
```

Add class:
```python
class DailyTime(DailyTimeDimensionModel):
    """Daily Time attributes"""
```

### 8. **Template/Configuration Generation**

#### 8.1 Update `dsgrid/config/common.py` in `make_base_time_dimension_template()` (around line 125)

```python
case TimeDimensionType.DAILY:
    time_dim["class"] = "DailyTime"
    time_dim["year_column"] = "time_year"
    time_dim["leap_day_adjustment"] = "none"
    time_dim["ranges"] = [
        {
            "start": "2018-01-01",
            "end": "2020-12-31",
        },
    ]
    time_dim["str_format"] = "%Y-%m-%d"
    time_dim["time_interval_type"] = "period_beginning"
    time_dim["measurement_type"] = "total"
```

### 9. **Data Mapping and Conversion**

#### 9.1 Consider mapping function in `dsgrid/config/daily_time_dimension_config.py`

Add a function for converting daily to datetime (similar to `map_annual_time_to_date_time`):

```python
def map_daily_time_to_date_time(
    df: DataFrame,
    daily_dim: DailyTimeDimensionConfig,
    dt_dim: DateTimeDimensionConfig,
    value_columns: set[str],
) -> DataFrame:
    """Map a DataFrame with a daily time dimension to a DateTime time dimension.

    This combines the year column (time_year, weather_year, or model_year) with
    month/day to create full timestamps compatible with the datetime dimension.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame with daily time dimension
    daily_dim : DailyTimeDimensionConfig
        Source daily time dimension configuration
    dt_dim : DateTimeDimensionConfig
        Target datetime dimension configuration
    value_columns : set[str]
        Data value columns to preserve

    Returns
    -------
    DataFrame
        DataFrame with datetime time dimension
    """
    # Implementation would:
    # 1. Get year column name from daily_dim.get_year_column()
    # 2. Combine year + month + day to create timestamps
    # 3. Map to the datetime dimension's time column
    # 4. Handle leap day adjustments if needed
    # 5. Validate against dt_dim's expected timestamps
    pass
```

### 10. **Tests**

#### 10.1 Create `tests/test_daily_time.py`

```python
import pytest
from datetime import datetime
import pandas as pd

from dsgrid.config.daily_time_dimension_config import (
    DailyTimeDimensionConfig,
    DailyTimeDimensionModel,
)
from dsgrid.config.dimensions import DailyRangeModel
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    MeasurementType,
    TimeIntervalType,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.spark.types import use_duckdb
from dsgrid.utils.spark import create_dataframe_from_dicts


@pytest.fixture
def daily_time_model():
    """Create a daily time dimension model for testing."""
    return DailyTimeDimensionModel(
        name="daily_time",
        display_name="Daily Time",
        type="time",
        time_type="daily",
        description="Test daily time dimension",
        ranges=[
            DailyRangeModel(start="2018-01-01", end="2018-12-31"),
        ],
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        measurement_type=MeasurementType.TOTAL,
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
        year_column="time_year",
    )


def test_daily_time_dimension_model_creation(daily_time_model):
    """Test that DailyTimeDimensionModel can be created."""
    assert daily_time_model.time_type.value == "daily"
    assert daily_time_model.leap_day_adjustment == LeapDayAdjustmentType.NONE
    assert daily_time_model.year_column == "time_year"
    assert len(daily_time_model.ranges) == 1


def test_year_column_validation():
    """Test that year_column is validated."""
    with pytest.raises(ValueError, match="year_column must be one of"):
        DailyTimeDimensionModel(
            name="daily_time",
            display_name="Daily Time",
            type="time",
            time_type="daily",
            description="Test",
            ranges=[DailyRangeModel(start="2018-01-01", end="2018-12-31")],
            year_column="invalid_column",  # Should fail
        )


def test_year_column_options():
    """Test all valid year_column options."""
    for year_col in ["time_year", "weather_year", "model_year"]:
        model = DailyTimeDimensionModel(
            name="daily_time",
            display_name="Daily Time",
            type="time",
            time_type="daily",
            description="Test",
            ranges=[DailyRangeModel(start="2018-01-01", end="2018-12-31")],
            year_column=year_col,
        )
        assert model.year_column == year_col


def test_daily_time_config_creation(daily_time_model):
    """Test that DailyTimeDimensionConfig can be created."""
    config = DailyTimeDimensionConfig(daily_time_model)
    assert config.get_frequency().days == 1
    assert config.get_load_data_time_columns() == ["time_month", "time_day"]
    assert config.get_year_column() == "time_year"


def test_daily_time_ranges(daily_time_model):
    """Test that daily time ranges are generated correctly."""
    config = DailyTimeDimensionConfig(daily_time_model)
    ranges = config.get_time_ranges()

    assert len(ranges) == 1
    timestamps = ranges[0].list_time_range()
    assert len(timestamps) == 365  # 2018 is not a leap year


def test_daily_time_ranges_leap_year():
    """Test daily time with leap year."""
    model = DailyTimeDimensionModel(
        name="daily_time",
        display_name="Daily Time",
        type="time",
        time_type="daily",
        description="Test daily time dimension",
        ranges=[
            DailyRangeModel(start="2020-01-01", end="2020-12-31"),
        ],
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        measurement_type=MeasurementType.TOTAL,
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )

    config = DailyTimeDimensionConfig(model)
    ranges = config.get_time_ranges()
    timestamps = ranges[0].list_time_range()

    assert len(timestamps) == 366  # 2020 is a leap year


def test_daily_time_ranges_drop_feb29():
    """Test daily time with leap day dropped."""
    model = DailyTimeDimensionModel(
        name="daily_time",
        display_name="Daily Time",
        type="time",
        time_type="daily",
        description="Test daily time dimension",
        ranges=[
            DailyRangeModel(start="2020-01-01", end="2020-12-31"),
        ],
        leap_day_adjustment=LeapDayAdjustmentType.DROP_FEB29,
        measurement_type=MeasurementType.TOTAL,
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )

    config = DailyTimeDimensionConfig(model)
    ranges = config.get_time_ranges()
    timestamps = ranges[0].list_time_range()

    assert len(timestamps) == 365  # Feb 29 dropped
    # Verify Feb 29 is not in the list
    feb_29 = [ts for ts in timestamps if ts.month == 2 and ts.day == 29]
    assert len(feb_29) == 0


def test_partial_year_summer_only():
    """Test daily time with partial year (summer months only)."""
    model = DailyTimeDimensionModel(
        name="daily_time",
        display_name="Daily Time - Summer",
        type="time",
        time_type="daily",
        description="Test daily time dimension with partial year",
        ranges=[
            DailyRangeModel(start="2018-06-01", end="2018-08-31"),
        ],
        leap_day_adjustment=LeapDayAdjustmentType.NONE,
        measurement_type=MeasurementType.TOTAL,
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
    )

    config = DailyTimeDimensionConfig(model)
    ranges = config.get_time_ranges()
    timestamps = ranges[0].list_time_range()

    # June (30) + July (31) + August (31) = 92 days
    assert len(timestamps) == 92

    # Verify range
    assert timestamps[0].month == 6 and timestamps[0].day == 1
    assert timestamps[-1].month == 8 and timestamps[-1].day == 31


def test_invalid_date_feb30():
    """Test that invalid dates like Feb 30 are rejected."""
    with pytest.raises(ValueError, match="Failed to parse time range"):
        model = DailyTimeDimensionModel(
            name="daily_time",
            display_name="Daily Time",
            type="time",
            time_type="daily",
            description="Test",
            ranges=[
                DailyRangeModel(start="2018-02-30", end="2018-12-31"),  # Invalid date
            ],
        )


def test_expected_timestamps(daily_time_model):
    """Test list_expected_dataset_timestamps."""
    config = DailyTimeDimensionConfig(daily_time_model)
    timestamps = config.list_expected_dataset_timestamps()

    assert len(timestamps) == 365
    assert all(ts.time_year == 2018 for ts in timestamps)
    assert timestamps[0].time_month == 1
    assert timestamps[0].time_day == 1


def test_multiple_year_ranges():
    """Test daily time with multiple year ranges."""
    model = DailyTimeDimensionModel(
        name="daily_time",
        display_name="Daily Time",
        type="time",
        time_type="daily",
        description="Test daily time dimension",
        ranges=[
            DailyRangeModel(start="2012-01-01", end="2012-12-31"),
            DailyRangeModel(start="2013-01-01", end="2013-12-31"),
        ],
        leap_day_adjustment=LeapDayAdjustmentType.DROP_FEB29,
        measurement_type=MeasurementType.TOTAL,
        time_interval_type=TimeIntervalType.PERIOD_BEGINNING,
        year_column="weather_year",
    )

    config = DailyTimeDimensionConfig(model)
    timestamps = config.list_expected_dataset_timestamps()

    # 2012 is leap year, 2013 is not, but both drop Feb 29
    assert len(timestamps) == 365 * 2

    years = set(ts.time_year for ts in timestamps)
    assert years == {2012, 2013}


def test_chronify_not_supported(daily_time_model):
    """Test that chronify is not yet supported."""
    config = DailyTimeDimensionConfig(daily_time_model)
    assert config.supports_chronify() is False
```

#### 10.2 Update existing tests
- `tests/test_create_time_dimensions.py` - add daily dimension fixtures
- `tests/test_dimension_config.py` - add daily dimension test cases
- Any integration tests that enumerate time dimension types

### 11. **Documentation**

#### 11.1 Create example config
Create `docs/source/examples/daily_time_dimension.json`:

```json
{
  "name": "daily_time",
  "display_name": "Daily Time",
  "type": "time",
  "time_type": "daily",
  "description": "Daily time dimension for weather year data",
  "class": "DailyTime",
  "module": "dsgrid.dimension.standard",
  "str_format": "%Y-%m-%d",
  "year_column": "weather_year",
  "leap_day_adjustment": "none",
  "measurement_type": "total",
  "time_interval_type": "period_beginning",
  "ranges": [
    {
      "start": "2012-01-01",
      "end": "2012-12-31"
    },
    {
      "start": "2013-01-01",
      "end": "2013-12-31"
    }
  ]
}
```

Example for partial year (summer only):
```json
{
  "name": "summer_daily_time",
  "display_name": "Summer Daily Time",
  "type": "time",
  "time_type": "daily",
  "description": "Daily time dimension for summer months only",
  "class": "DailyTime",
  "module": "dsgrid.dimension.standard",
  "str_format": "%Y-%m-%d",
  "year_column": "weather_year",
  "leap_day_adjustment": "none",
  "measurement_type": "mean",
  "time_interval_type": "period_beginning",
  "ranges": [
    {
      "start": "2018-06-01",
      "end": "2018-08-31"
    }
  ]
}
```

---

## Design Decisions (Finalized)

### 1. Year Column Configuration ✓
- **Decision**: Add `year_column` field to `DailyTimeDimensionModel`
- **Default**: `"time_year"`
- **Options**: `"time_year"`, `"weather_year"`, `"model_year"`
- **Validation**: Field validator ensures only valid options are used
- **Precedence**: The specified column always takes precedence
- **Rationale**: Explicit configuration is clearer and more flexible than implicit behavior

### 2. Partial Year Support ✓
- **Decision**: Full support for partial years
- **Examples**: Summer only ("2018-06-01" to "2018-08-31"), Q4 only, etc.
- **Implementation**: Already supported by `DailyRangeModel` design
- **Validation**: Start/end dates validated, no restriction on full years
- **Rationale**: Enables use cases like seasonal analysis, specific time periods

### 3. Chronify Support ✓
- **Decision**: Defer chronify support initially
- **Implementation**: `supports_chronify()` returns `False`
- **Documentation**: Comment in code explains what would be needed
- **Future Work**:
  - Chronify needs new `DailyTime` or `MonthDayTime` model
  - Support for separate month/day/year columns
  - Leap day adjustment integration
  - Time dimension mapping capabilities
- **Rationale**: Core functionality works without chronify; can add later when needed

### 4. Date Validation ✓
- **Decision**: Python's `datetime.strptime()` handles validation automatically
- **Invalid dates**: Automatically rejected (e.g., Feb 30, April 31)
- **Error handling**: Clear error messages from strptime
- **Additional checks**: Start must be <= end
- **Rationale**: Built-in validation is robust and well-tested

---

## Implementation Order

1. **Core enum and type** (Steps 1.1-1.2) - Foundation
2. **Range model** (Step 2.1) - Schema definition
3. **Data model** (Step 3.1-3.4) - Main model class with validators
4. **Time range class** (Step 4.1) - Core iteration logic
5. **Configuration class** (Step 5.1) - Primary implementation
6. **Factory integration** (Step 6.1) - Wiring it up
7. **Standard classes and templates** (Steps 7.1, 8.1) - User-facing
8. **Tests** (Step 10) - Validation
9. **Mapping/conversion** (Step 9.1) - Advanced features (can defer)
10. **Documentation** (Step 11.1) - Polish

---

## Files to Create/Modify

### New Files (2):
1. `dsgrid/config/daily_time_dimension_config.py` - Main configuration class
2. `tests/test_daily_time.py` - Test suite

### Modified Files (~13):
1. `dsgrid/dimension/time.py` - Add DAILY enum, DailyTimeRange class
2. `dsgrid/time/types.py` - Add DailyTimestampType
3. `dsgrid/config/dimensions.py` - Add DailyRangeModel, DailyTimeDimensionModel, update union
4. `dsgrid/config/dimension_config_factory.py` - Add factory methods
5. `dsgrid/dimension/standard.py` - Add DailyTime class
6. `dsgrid/config/common.py` - Add template generation
7. `tests/test_create_time_dimensions.py` - Add fixtures
8. `tests/test_dimension_config.py` - Add test cases
9. `docs/source/examples/daily_time_dimension.json` - Example config

**Total: ~15 files**

---

## Summary

This plan provides a complete, production-ready implementation of the DAILY time dimension type that:

- ✅ Supports your primary use case (different data per weather year)
- ✅ Allows explicit year column configuration with validation
- ✅ Enables partial year ranges (e.g., summer-only data)
- ✅ Defers chronify support until needed
- ✅ Validates dates automatically (rejects invalid dates like Feb 30)
- ✅ Provides comprehensive test coverage
- ✅ Follows existing dsgrid patterns and conventions
- ✅ Integrates seamlessly with the existing architecture

The implementation is clean, maintainable, and extensible for future enhancements.
