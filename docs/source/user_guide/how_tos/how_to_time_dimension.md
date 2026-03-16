# How to Define a Time Dimension

Time dimensions work differently from other dimension types: instead of a records CSV, they are defined entirely by parameters in the config file. This guide covers each supported time type with example data tables and configs.

:::{note}
DST = Daylight Saving Time

IANA = Internet Assigned Numbers Authority
:::

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
- **`time_zone_format`** -- whether timestamps are aligned in absolute time (single timezone for all geographies) or local standard clock time (one timezone per geography)

### Time zones, UTC offsets, Localization, and Validation

Data table timestamps are either *timezone-aware* (`timestamp_tz` or `time_format_in_parts` with per-row UTC offsets in `offset_column`) or *timezone-naive* (`timestamp_ntz` or `time_format_in_parts` with no offset column) and dsgrid provides time checks based on IANA time zones. *If input timestamps are timezone-aware, dsgrid supports all IANA time zones*, including time zones like "America/New York" that include DST. *If input timestamps are timezone-naive, dsgrid requires standard time* or fixed-offset time zones that do not include DST. In either case, the `time_zone` field must be specified in `time_zone_format`. The `time_zone` field is required even for timezone-aware timestamps because dsgrid uses it to construct validation data independent of the column format.

dsgrid automatically localizes timezone-naive timestamps to the zone(s) specified in `time_zone_format` during registration. Timezone-naive data is indicated by `column_format.dtype` as `timestamp_ntz` or `time_format_in_parts` without `offset_column`. For correct localization, input timestamps must be laid out in standard time (without DST skips and duplicates), because standard libraries cannot safely handle ambiguous or missing hours at DST transitions. Consequently, when using timezone-naive data, `time_zone_format` can only reference IANA zones with constant UTC offsets (e.g., "Etc/GMT+5"), not DST-observing zones like "America/New_York". This restriction applies only to timezone-naive localization; for all other scenarios, both time zone types are supported.

:::{note}
IANA time zone names (e.g., "America/New_York", "Etc/GMT+5") are distinct from UTC offsets (e.g., UTC-5).

Fixed-offset or standard time time zones like "Etc/GMT+5" observe a single UTC offset (UTC-5) year-round, while DST-observing zones like "America/New_York" have multiple offsets depending on the calendar date (UTC-5 during standard time, UTC-4 during DST).
:::

### 1a. Column Format Distinctions

Three formats are supported for storing time data:

**`timestamp_tz` (timezone-aware)** — A single timestamp column with embedded UTC offset/timezone information. Timestamps represent absolute UTC instants and include offset information (e.g., `2012-01-01 00:00:00-05:00`). Use this format when input data already has timezone information. See Examples 1 and 5 below.

**`timestamp_ntz` (timezone-naive)** — A single timestamp column with no offset/timezone information (e.g., `2012-01-01 00:00:00`). dsgrid automatically localizes these to the zone(s) specified in `time_zone_format` during registration. Input data must use standard time (no DST observance) for accurate localization. See Examples 2 and 6 below.

**`time_format_in_parts`** — Timestamps split across multiple integer columns (year, month, day, hour) instead of a single column. dsgrid combines these into a single `timestamp` column during registration. Optionally, per-row UTC offsets can be provided via `offset_column` to construct timezone-aware timestamps directly; otherwise, the constructed timestamps are timezone-naive. Localization can be applied using `time_zone_format`. See Examples 3 and 4 below.

---

### 1b. Globally Aligned — Single Time Zone

All geographies share identical timestamps in **absolute time**. Every row with the same timestamp represents the same instant everywhere. Use a single `time_zone` in `time_zone_format`.

#### With `timestamp_tz` (timezone-aware single column)

**Example 1 data table** (hourly, timezone-aware):

```
timestamp | geography | value
---------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | g_east | 1.2
2012-01-01 01:00:00-05:00 | g_east | 0.9
2012-06-01 00:00:00-04:00 | g_east | 2.1
2012-06-01 01:00:00-04:00 | g_east | 1.8
...
```

**Config 1**:

```javascript
{
  type: "time",
  name: "Hourly 2012 EST",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "timestamp_tz",
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
    time_zone: "America/New_York",         // IANA DST-observing time zone
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Data table 1 after registration**:

```
timestamp | geography | value
---------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | g_east | 1.2
2012-01-01 01:00:00-05:00 | g_east | 0.9
2012-06-01 00:00:00-04:00 | g_east | 2.1
2012-06-01 01:00:00-04:00 | g_east | 1.8
...
```

The input data is already timezone-aware with explicit UTC offsets, so no localization is needed. The timestamps are preserved as-is.

#### With `timestamp_ntz` (timezone-naive single column)

**Example 2 data table** (hourly, timezone-naive, standard time only):

```
timestamp | geography | value
-------------------|-----------|-----
2012-01-01 00:00:00 | g_east | 1.2
2012-01-01 01:00:00 | g_east | 0.9
2012-05-31 23:00:00 | g_east | 2.1
2012-06-01 00:00:00 | g_east | 1.8
...
```

**Config 2** — dsgrid will automatically localize these timestamps to `Etc/GMT+5` during registration:

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
    time_zone: "Etc/GMT+5",         // IANA fixed-offset zone; must be standard time (no DST)
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Data table 2 after registration**:

```
timestamp | geography | value
---------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | g_east | 1.2
2012-01-01 01:00:00-05:00 | g_east | 0.9
2012-05-31 23:00:00-05:00 | g_east | 2.1
2012-06-01 00:00:00-05:00 | g_east | 1.8
...
```

The timezone-naive timestamps are localized to `Etc/GMT+5` during registration, resulting in timezone-aware timestamps.

To store timezone-naive timestamps **without any localization**, set `time_zone` to `null`:

```javascript
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: null,
  },
```

#### With `time_format_in_parts` (separate columns, no offset)

**Example 3 data table**:

```
year | month | day | hour | geography | value
-----|-------|-----|------|-----------|-----
2012 | 1 | 1 | 0 | g_east | 1.2
2012 | 1 | 1 | 1 | g_east | 0.9
2012 | 5 | 31 | 23 | g_east | 2.1
2012 | 6 | 1 | 0 | g_east | 1.8
...
```

**Config 3** — dsgrid combines the parts and localizes to `Etc/GMT+5`:

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
    hour_column: "hour",
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

**Data table 3 after registration**:

```
timestamp | geography | value
---------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | g_east | 1.2
2012-01-01 01:00:00-05:00 | g_east | 0.9
2012-05-31 23:00:00-05:00 | g_east | 2.1
2012-06-01 00:00:00-05:00 | g_east | 1.8
...
```

The input columns (`year`, `month`, `day`, `hour`) are combined into a single `timestamp` column and then localized to `Etc/GMT+5`, resulting in a timezone-aware timestamp.

#### With `time_format_in_parts` (separate columns, with offset)

**Example 4 data table** (capturing DST variations):

```
year | month | day | hour | utc_offset | geography | value
-----|-------|-----|------|------------|-----------|-----
2012 | 1 | 1 | 0 | -5.0 | g_east | 1.2
2012 | 1 | 1 | 1 | -5.0 | g_east | 0.9
2012 | 6 | 1 | 0 | -4.0 | g_east | 2.1
2012 | 6 | 1 | 1 | -4.0 | g_east | 1.8
...
```

Winter (January) has a UTC offset of -5.0 (standard time), while summer (June) has -4.0 (DST).

**Config 4** — dsgrid combines the parts and applies each row's offset:

```javascript
{
  type: "time",
  name: "Hourly 2012 in Parts with DST",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "time_format_in_parts",
    year_column: "year",
    month_column: "month",
    day_column: "day",
    hour_column: "hour",
    offset_column: "utc_offset",    // numeric offset in hours (e.g., -5.0) or string (e.g., "-05:00")
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
    time_zone: "America/New_York",        // required: reference timezone for validation and truth data creation
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Data table 4 after registration**:

```
timestamp | geography | value
---------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | g_east | 1.2
2012-01-01 01:00:00-05:00 | g_east | 0.9
2012-06-01 00:00:00-04:00 | g_east | 2.1
2012-06-01 01:00:00-04:00 | g_east | 1.8
...
```

The input columns (`year`, `month`, `day`, `hour`) are combined with the `offset_column` values to produce timezone-aware timestamps, preserving DST variations.

---

### 1c. Locally Aligned — Multiple Time Zones

Timestamps cover the **same interval of local standard time** across geographies — e.g., every geography has data from midnight to midnight in its own local time. Because the clocks read the same local time but represent different absolute instants, each row must include a `time_zone` column. Use `format_type: "aligned_in_std_clock_time"` in `time_zone_format`.

#### With `timestamp_tz` (timezone-aware single column)

**Example 5 data table**:

```
timestamp | time_zone | geography | value
---------------------------|-------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | America/New_York | g_east | 1.2
2012-01-01 01:00:00-05:00 | America/New_York | g_east | 0.9
2012-01-01 00:00:00-08:00 | America/Los_Angeles | g_west | 3.1
2012-01-01 01:00:00-08:00 | America/Los_Angeles | g_west | 2.7
...
```

The `time_zone` column contains IANA time zone strings that must match entries in `time_zone_format.time_zones`. Timezone-aware format supports both fixed-offset and DST-observing time zones.

**Config 5**:

```javascript
{
  type: "time",
  name: "Local Hourly 2012",
  "class": "Time",
  time_type: "datetime",
  column_format: {
    dtype: "timestamp_tz",
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
    time_zones: ["America/New_York", "America/Los_Angeles"],
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Data table 5 after registration**:

```
timestamp | time_zone | geography | value
---------------------------|-------------------------|-----------|-----
2012-01-01 00:00:00-05:00 | America/New_York | g_east | 1.2
2012-01-01 01:00:00-05:00 | America/New_York | g_east | 0.9
2012-01-01 00:00:00-08:00 | America/Los_Angeles | g_west | 3.1
2012-01-01 01:00:00-08:00 | America/Los_Angeles | g_west | 2.7
```

The input data is already timezone-aware with matching time zone values, so no localization is needed. The time zone column preserves which IANA zone each row belongs to.

#### With `timestamp_ntz` (timezone-naive single column)

**Example 6 data table**:

```
timestamp | time_zone | geography | value
-------------------|-----------|-----------|-----
2012-01-01 00:00:00 | Etc/GMT+5 | g_east | 1.2
2012-01-01 01:00:00 | Etc/GMT+5 | g_east | 0.9
2012-01-01 00:00:00 | Etc/GMT+8 | g_west | 3.1
2012-01-01 01:00:00 | Etc/GMT+8 | g_west | 2.7
...
```

For timezone-naive data, the `time_zone` column must contain **only fixed-offset IANA zones** (no DST observance), and the timestamps must be laid out in standard time for proper localization.

**Config 6**:

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
    // All unique IANA fixed-offset time zones that appear in the data table's time_zone column
    time_zones: ["Etc/GMT+5", "Etc/GMT+8"],
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

**Data table 6 after registration**:

```
timestamp | time_zone | geography | value
---------------------------|-----------|-----------|-----
2012-01-01 00:00:00-05:00 | Etc/GMT+5 | g_east | 1.2
2012-01-01 01:00:00-05:00 | Etc/GMT+5 | g_east | 0.9
2012-01-01 00:00:00-08:00 | Etc/GMT+8 | g_west | 3.1
2012-01-01 01:00:00-08:00 | Etc/GMT+8 | g_west | 2.7
```

Each row's tz-naive timestamp is localized to the IANA zone in its `time_zone` column during registration, resulting in timezone-aware timestamps with the appropriate offsets. To store timezone-naive timestamps **without localization**, use `aligned_in_absolute_time` with `time_zone: null`.

The `time_format_in_parts` column format is also supported for locally aligned time. See Examples 3 and 4 for additional configuration options.

---

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

Index time uses sequential **integer indices** instead of explicit timestamps. The config maps these indices to a real time range using `starting_timestamp`, `frequency`, and the range of start/end indices. This is useful when source data is indexed (e.g., 1-8760 or 0-8783) rather than timestamped directly.

**How it works**: Index `start` corresponds to `starting_timestamp`, and each subsequent index shifts forward by one `frequency` increment.

**Requirements**:
- **`time_zone` column required**: Must exist in both the geography dimension records and the data table. Used when converting indices to datetimes during dataset-to-project mapping.
- **All time zones allowed**: Including DST-observing zones like `"America/New_York"` and fixed-offset zones like `"Etc/GMT+5"`.

**Conversion behavior**: Index-to-datetime conversion happens only when the dataset (with index time) maps to a project (with datetime time). The conversion respects time adjustments specified in the project config, such as `daylight_saving_adjustment`, `leap_day_adjustment`, and `wrap_time_allowed`.

---

### Common use cases

**Example 1: Electricity data with standard 8760 indexing**

Electricity datasets commonly index hourly values from 0-8759 (or 1-8760). The config records the starting point to preserve time specificity (day of week, season, etc.).

**Example data table**:

```
time_index | time_zone  | geography | value
-----------|------------|-----------|------
         0 | Etc/GMT+5  | g_east    | 1.2
         1 | Etc/GMT+5  | g_east    | 0.9
         2 | Etc/GMT+5  | g_east    | 1.4
...
      8783 | Etc/GMT+5  | g_east    | 2.0
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

**Example 2: Industrial data with DST adjustment**

Some datasets (e.g., IEF industrial profiles) are stitched together from representative diurnal profiles in local standard time. When mapping to a project, both the timestamps and data values must be adjusted for DST transitions. Geographies observing DST will have dropped (spring-forward) or duplicated (fall-back) hours. Interpolation is available for the fall-back hour adjustment.

**Dataset config**:

```javascript
{
  type: "time",
  "class": "Time",
  time_type: "index",
  name: "ind_time",
  description: "Industrial 8760-indexed time with 2018-01-01 00:00:00 start, stitched from representative diurnal profiles in local standard time. Requires DST adjustment during mapping to account for DST transitions.",
  time_interval_type: "period_beginning",
  ranges: [
    {
      start: 0,
      end: 8759,
      starting_timestamp: "2018-01-01 00:00:00",
      str_format: "%Y-%m-%d %H:%M:%S",
      frequency: "01:00:00",
    },
  ],
  measurement_type: "total",
}
```

**Project config** to apply daylight saving adjustment:

```javascript
{
  dataset_id: 'ief_2025_industry',
  wrap_time_allowed: true,
  time_based_data_adjustment: {
    daylight_saving_adjustment: {
      spring_forward_hour: "drop",
      fall_back_hour: "duplicate",
    },
  },
  ...
}
```

When the dataset maps to the project, the index time is first converted to standard time datetimes. Then, for each geography's time zone, data rows are adjusted according to the `daylight_saving_adjustment` setting if that zone observes DST. Hours during spring-forward are dropped and hours during fall-back are duplicated, affecting both timestamps and their associated data values.

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

Both representative period formats require a `time_zone` column in the geography dimension records because representative periods must be localized when mapping to a project's datetime dimension. All time zones (including those observing DST) are accepted in this time class.

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
