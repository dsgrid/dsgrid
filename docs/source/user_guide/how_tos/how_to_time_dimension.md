# How to Define a Time Dimension

Time dimensions work differently from other dimension types: instead of a records CSV, they are defined entirely by parameters in the config file. This guide covers each supported time type with example data tables and configs.

## Overview

Select the right `time_type` based on how your dataset represents time:

| `time_type` | `class` | Use when... |
|---|---|---|
| `datetime` | `Time` | Data has explicit timestamps (hourly, sub-hourly, daily, etc.) |
| `index` | `Time` | Data uses sequential integer indices that map to a known start time |
| `representative_period` | `Time` | Data covers a representative period (e.g., a typical week per month) |
| `annual` | `AnnualTime` | Data has one value per year |
| `noop` | `NoOpTime` | Data has no time component at all |

All time dimension configs require `type: "time"` and a `class` referencing `dsgrid.dimension.standard`. The `time_type` field selects the model variant.

---

## 1. Datetime

Datetime is the most common time type. The config describes two things:

- **`column_format`** -- how timestamps are stored in the data table (`timestamp_tz`, `timestamp_ntz`, or `time_format_in_parts`)
- **All other fields** -- what the time data represents, used for validation on registration

### 1a. Globally aligned -- single time zone

All geographies share identical timestamps in **absolute time**. This is the simplest case: every row with the same timestamp represents the same instant everywhere.

**Example data table** (hourly, UTC-5):

```
timestamp                  | geography | value
---------------------------|-----------|------
2012-01-01 00:00:00-05:00  | g1        | 1.2
2012-01-01 01:00:00-05:00  | g1        | 0.9
2012-01-01 00:00:00-05:00  | g2        | 3.1
...
```

**Config** (`column_format.dtype = "timestamp_tz"`, the default):

```javascript
{
  type: "time",
  name: "Hourly 2012 EST",
  "class": "Time",
  time_type: "datetime",
  ranges: [
    {
      start: "2012-01-01 00:00:00",
      end: "2012-12-31 23:00:00",
      frequency: "01:00:00",
    },
  ],
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: "Etc/GMT+5",         // IANA fixed-offset zone; Etc/GMT+5 = UTC-5
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Config** with timezone-naive timestamps (`column_format.dtype = "timestamp_ntz"`). dsgrid will localize the data table timestamps to `Etc/GMT+5` during registration:

```javascript
{
  type: "time",
  name: "Hourly 2012 EST",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "timestamp_ntz",
    time_column: "timestamp",
  },
  ranges: [
    {
      start: "2012-01-01 00:00:00",
      end: "2012-12-31 23:00:00",
      frequency: "01:00:00",
    },
  ],
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: "Etc/GMT+5",
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

Note dsgrid can only localize timezone-naive timestamps to standard time, and only when the timestamps contain no daylight saving time gaps or duplicates, because standard libraries cannot handle fallback duplicates correctly.

To store timezone-naive timestamps **without any localization** (e.g., data already in UTC with no offset to apply), set `time_zone` to `null`:

```javascript
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: null,
  },
```

### 1b. Locally aligned -- multiple time zones

Timestamps cover the **same interval of local clock time** across geographies -- e.g., every geography has data from midnight to midnight in its own local time. Because the clocks read the same local standard time but represent different absolute instants, each row must include a `time_zone` column.

**Example data table** (hourly, NTZ, two time zones):

```
timestamp            | time_zone  | geography | value
---------------------|------------|-----------|------
2012-01-01 00:00:00  | Etc/GMT+5  | g_east    | 1.2
2012-01-01 01:00:00  | Etc/GMT+5  | g_east    | 0.9
2012-01-01 00:00:00  | Etc/GMT+8  | g_west    | 3.1
2012-01-01 01:00:00  | Etc/GMT+8  | g_west    | 2.7
...
```

The `time_zone` column must contain only IANA time zone strings that also appear in `time_zone_format.time_zones`.

**Config**:

```javascript
{
  type: "time",
  name: "Local Hourly 2012",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "timestamp_ntz",
    time_column: "timestamp",
  },
  ranges: [
    {
      start: "2012-01-01 00:00:00",
      end: "2012-12-31 23:00:00",
      frequency: "01:00:00",
    },
  ],
  time_zone_format: {
    format_type: "aligned_in_std_clock_time",
    // All unique IANA fixed-offset time zones that appear in the data table's time_zone column.
    time_zones: ["Etc/GMT+5", "Etc/GMT+6", "Etc/GMT+7", "Etc/GMT+8"],
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

dsgrid localizes each row's tz-naive timestamp to the IANA zone in its `time_zone` column during registration. Note dsgrid can only localize timezone-naive timestamps to standard time, and only when the timestamps contain no daylight saving time gaps or duplicates, because standard libraries cannot handle fallback duplicates correctly.

If this is a problem, consider using timezone-aware timestamps or timestamps broken out in parts, including offset in the data table.

### 1c. Time stored in separate columns (`time_format_in_parts`)

Some datasets store the date and time as separate integer columns rather than a single timestamp. Use `dtype: "time_format_in_parts"` in `column_format`. dsgrid combines the part columns into a single `timestamp` column during registration.

**Example data table**:

```
year | month | day | hour | geography | value
-----|-------|-----|------|-----------|------
2012 |     1 |   1 |    0 | g1        | 1.2
2012 |     1 |   1 |    1 | g1        | 0.9
```

**Config**:

```javascript
{
  type: "time",
  name: "Hourly 2012 in Parts",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "time_format_in_parts",
    year_column: "year",
    month_column: "month",
    day_column: "day",
    hour_column: "hour",          // optional; omit to treat all rows as hour 0
    // offset_column: "utc_offset"  // optional; UTC offset in hours, e.g. -8 or "-08:00"
  },
  ranges: [
    {
      start: "2012-01-01 00:00:00",
      end: "2012-12-31 23:00:00",
      frequency: "01:00:00",
    },
  ],
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: "Etc/GMT+5",
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

If `offset_column` is provided, the resulting timestamp column will be timezone-aware (`timestamp_tz`); otherwise it will be timezone-naive (`timestamp_ntz`).

### Key fields for `datetime`

| Field | Description |
|---|---|
| `ranges[].start` / `end` | First and last timestamps in the data (inclusive), parsed by `str_format` |
| `ranges[].str_format` | `strftime` format string for parsing `start`/`end` (default: `"%Y-%m-%d %H:%M:%S"`) |
| `ranges[].frequency` | Time step size as a `timedelta` string (`"01:00:00"`, `"00:15:00"`) or ISO 8601 duration (`"P1D"`, `"PT1H"`) |
| `time_zone_format.format_type` | `"aligned_in_absolute_time"` or `"aligned_in_std_clock_time"` |
| `time_interval_type` | `"period_beginning"` -- timestamp labels the start of the interval; `"period_ending"` -- labels the end; `"instantaneous"` -- a point measurement |
| `measurement_type` | `"total"`, `"mean"`, `"min"`, `"max"`, or `"measured"` |

---

## 2. Index Time

Index time is a variant of datetime where the data table uses sequential **integer indices** (`time_index`) instead of timestamps. The config maps those indices to a real time range via a `starting_timestamp` and `frequency`. This is useful when source data uses row numbers or model time steps rather than calendar timestamps.

**Example data table** (index 0 = 2012-01-01 00:00 EST, hourly):

```
time_index | time_zone  | geography | value
-----------|------------|-----------|------
         0 | Etc/GMT+5  | g1        | 1.2
         1 | Etc/GMT+5  | g1        | 0.9
         2 | Etc/GMT+5  | g1        | 1.4
...
      8783 | Etc/GMT+5  | g1        | 2.0
```

**Config**:

```javascript
{
  type: "time",
  name: "Hourly 2012 Index",
  "class": "Time",
  time_type: "index",
  ranges: [
    {
      start: 0,                                     // first index value (inclusive)
      end: 8783,                                    // last index value (inclusive); 8784 hours in 2012
      starting_timestamp: "2012-01-01 00:00:00",   // real datetime that index 0 corresponds to
      str_format: "%Y-%m-%d %H:%M:%S",
      frequency: "01:00:00",
    },
  ],
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

Index time always requires a `time_zone` column in both the geography dimension records and the data table (used to convert indices to localized datetimes when mapping to a project's datetime dimension). All time zones (including those observing daylight savings) are accepted in this time class.

---

## 3. Representative Time

Representative time is used when data covers a **typical period** rather than actual calendar dates -- for example, a typical week for each month (used by TEMPO). The data table uses integer columns instead of timestamps.

Two formats are currently supported, selected by the `format` field.

### 3a. `one_week_per_month_by_hour`

Data has one representative week per month. The data table must have three columns: `month`, `day_of_week`, and `hour`.

- `month`: 1--12
- `day_of_week`: 0 (Monday) -- 6 (Sunday), following Python's `datetime.weekday()` convention
- `hour`: 0--23

**Example data table**:

```
month | day_of_week | hour | geography | value
------|-------------|------|-----------|------
    1 |           0 |    0 | g1        | 1.2
    1 |           0 |    1 | g1        | 0.9
    1 |           6 |   23 | g1        | 0.8
...
```

**Config**:

```javascript
{
  type: "time",
  name: "Representative Week per Month",
  "class": "Time",
  time_type: "representative_period",
  format: "one_week_per_month_by_hour",
  ranges: [
    {
      start: 1,    // first month (January)
      end: 12,     // last month (December); use a subset for partial-year data
    },
  ],
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

### 3b. `one_weekday_day_and_one_weekend_day_per_month_by_hour`

Data has one weekday and one weekend day per month. The data table must have three columns: `month`, `is_weekday`, and `hour`.

- `month`: 1--12
- `is_weekday`: `true` or `false`
- `hour`: 0--23

**Example data table**:

```
month | is_weekday | hour | geography | value
------|------------|------|-----------|------
    1 |       true |    0 | g1        | 1.2
    1 |       true |    1 | g1        | 0.9
    1 |      false |    0 | g1        | 0.7
...
```

**Config**:

```javascript
{
  type: "time",
  name: "Representative Weekday/Weekend per Month",
  "class": "Time",
  time_type: "representative_period",
  format: "one_weekday_day_and_one_weekend_day_per_month_by_hour",
  ranges: [
    {
      start: 1,
      end: 12,
    },
  ],
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

Both representative period formats require a `time_zone` column in the geography dimension records because representative periods must be localized when mapping to a project's datetime dimension. All time zones (including those observing daylight savings) are accepted in this time class.

---

## 4. Annual Time

Use annual time when each value represents an **aggregate for an entire year** (e.g., annual energy consumption in MWh). The data table has a single `time_year` integer column.

**Example data table**:

```
time_year | geography | value
----------|-----------|------
     2020 | g1        | 105.3
     2021 | g1        | 108.7
     2022 | g1        | 112.0
```

**Config**:

```javascript
{
  type: "time",
  name: "Annual 2020-2050",
  "class": "AnnualTime",
  time_type: "annual",
  ranges: [
    {
      start: "2020",
      end: "2050",
    },
  ],
  include_leap_day: false,    // set true if annual totals include Feb 29 data in leap years
  measurement_type: "total",  // annual time only supports "total"
}
```

`include_leap_day` indicates whether values in leap years account for February 29. This matters when dsgrid disaggregates annual totals to sub-annual (e.g., hourly) resolution.

Annual time does not require a `time_zone` column in the geography dimension.

---

## 5. NoOp Time

Use NoOp when the dataset has **no time dimension at all** -- for example, a static lookup table or a single-year snapshot where time is not a relevant axis.

**Example data table**:

```
geography | value
----------|------
g1        | 42.0
g2        | 17.5
```

**Config**:

```javascript
{
  type: "time",
  name: "No Time",
  "class": "NoOpTime",
  time_type: "noop",
}
```

NoOp time does not require a `time_zone` column in the geography dimension.

---

## Related References

- [Dimension Concepts -- Time Dimensions](../dataset_registration/dimension_concepts.md#time-dimensions) -- conceptual overview and `column_format` details
- [Dimension Data Models](../../software_reference/data_models/dimension_model.md) -- complete config schema reference
- [How to Define Dimensions](how_to_dimensions.md) -- general dimension workflow
