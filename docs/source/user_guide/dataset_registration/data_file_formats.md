# Data File Formats

dsgrid aims to support all data file formats that users need for efficient queries and analysis. If you need a new format, please {ref}`contact the dsgrid team <contact-us>` to discuss it.

## Requirements

1. Metric data should usually be stored in Parquet files. [CSV files](#csv-files) are also supported. If you need or want another optimized columnar file format, please contact the dsgrid team.
2. If the data tables contain time-series data, each unique time array must contain an identical range of timestamps.
3. Values of dimension columns except `model_year` and `weather_year` must be strings. `model_year` and `weather_year` can be integers.
4. Each dimension column name except time must match dsgrid dimension types (geography, sector, subsector, etc.) either directly or by specifying a [column mapping](#custom-column-names).
5. The values in each dimension column must match the dataset's dimension records.

## Recommendations

1. Enable compression in all Parquet files. `Snappy` is preferred.
2. The recommended size of individual Parquet files is 128 MiB. Making the files too big can cause memory issues. Making them too small adds overhead and hurts performance.
3. [Trivial dimensions](./dimension_concepts.md#trivial-dimensions) (one-element records) should not be stored in the data files. They should instead be defined in the dataset config. dsgrid will add them dynamically at runtime.
4. Floating point data can be 64-bit or 32-bit. 64-bit floats provide more precision but require twice as much storage space as 32-bit floats.

## Table Formats

Input datasets can use a **one-table** or **two-table** format.

(one-table-format)=
### One Table Format

All metric data and dimension records are stored in one Parquet file (or set of Parquet files in a directory).

The following example shows a stacked one-table format. The `metric` column contains dimension record IDs and the `value` column contains the data:

:::{list-table} load_data.parquet
:header-rows: 1
:widths: 28 10 12 25 15 10

* - timestamp
  - geography
  - scenario
  - subsector
  - metric
  - value
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - heating
  - 1.234
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - cooling
  - 0.002
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - interior_equipment
  - 0.051
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - primary_school
  - heating
  - 2.345
* - 2012-01-01T01:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - heating
  - 1.456
* - …
  - …
  - …
  - …
  - …
  - …
:::

:::{note}
This example omits the `sector`, `model_year`, and `weather_year` columns because they are [trivial dimensions](./dimension_concepts.md#trivial-dimensions) — each has only a single record (e.g., `sector = "com"`, `model_year = 2020`, `weather_year = 2012`). Trivial dimensions are declared in the dataset config and added by dsgrid at runtime, so they do not need to appear in the data files.
:::

(two-table-format)=
### Two Table Format

Two Parquet files comprise the dataset: a **data table** with time-series metric values and a **lookup table** that maps an integer ID to combinations of dimension records.

**Data table** (`load_data.parquet`) — required columns and content:
- `id` (integer) — links each row to a dimension-record combination in the lookup table.
- Time columns — one or more columns representing time (e.g., `timestamp`). See [Time Formats](#time-formats) below.
- Value data — either a single `value` column (stacked) or one column per metric record ID (pivoted). See [Value Formats](#value-formats) below.
- Dimension columns are optional. Any non-time, non-trivial dimension not present here must appear in the lookup table. In practice, most dimensions go in the lookup table.

**Lookup table** (`load_data_lookup.parquet`) — required columns and content:
- `id` (integer) — matches the IDs in the data table.
- Every non-time, non-trivial dimension that is *not* already a column in the data table. This typically includes all non-time dimensions (geography, sector, subsector, metric, scenario, model_year, weather_year, etc.).
- `scaling_factor` (float, optional) — see [Lookup Table with Scaling Factor](#lookup-table-with-scaling-factor) below.

#### Pivoted, No Scaling Factor

**`load_data.parquet`** — Time-series values for all metric data. This example pivots the metric dimension, so the metric record IDs (`heating`, `cooling`, `interior_equipment`) appear as column names. The `id` column links each row to a dimension record combination in the lookup table:

:::{list-table} load_data.parquet (time-series metric values; example shows a pivoted metric dimension)
:header-rows: 1
:widths: 28 5 15 15 20

* - timestamp
  - id
  - heating
  - cooling
  - interior_equipment
* - 2012-01-01T00:00:00+00:00
  - 1
  - 0.214
  - 0.002
  - 0.051
* - 2012-01-01T01:00:00+00:00
  - 1
  - 0.329
  - 0.000
  - 0.051
* - 2012-01-01T02:00:00+00:00
  - 1
  - 0.369
  - 0.000
  - 0.066
* - 2012-01-01T00:00:00+00:00
  - 2
  - 1.023
  - 0.015
  - 0.102
* - 2012-01-01T01:00:00+00:00
  - 2
  - 1.156
  - 0.012
  - 0.102
* - …
  - …
  - …
  - …
  - …
:::

**`load_data_lookup.parquet`** — Maps each `id` to a combination of dimension records:

:::{list-table} load_data_lookup.parquet
:header-rows: 1
:widths: 5 10 8 25 12 12 12

* - id
  - geography
  - sector
  - subsector
  - scenario
  - model_year
  - weather_year
* - 1
  - 01001
  - com
  - full_service_restaurant
  - reference
  - 2020
  - 2012
* - 2
  - 01001
  - com
  - primary_school
  - reference
  - 2020
  - 2012
* - 3
  - 01003
  - com
  - full_service_restaurant
  - reference
  - 2020
  - 2012
* - 4
  - 01003
  - com
  - primary_school
  - reference
  - 2020
  - 2012
* - …
  - …
  - …
  - …
  - …
  - …
  - …
:::

:::{note}
All non-time, non-trivial dimensions should appear in the lookup table. Trivial dimensions can still be omitted and declared in the dataset config. In this example, if `sector`, `model_year`, and `weather_year` were all trivial, the lookup table would only need `id`, `geography`, `subsector`, and `scenario` columns.
:::

Each unique time array in `load_data` must be denoted with an integer ID that corresponds to a record in `load_data_lookup`. The ID is user-defined. Users may want to use a sequentially-increasing integer or encode other information into specific bytes/digits of each integer.

#### Lookup Table with Scaling Factor

The lookup table may optionally include a `scaling_factor` column. When present, dsgrid multiplies each value column by the row's scaling factor at query time and then drops the `scaling_factor` column from the result.

This is useful when the data table stores normalized profiles and the actual magnitude varies by dimension combination. For example, a distributed generation dataset might store a single set of hourly capacity-factor profiles in the data table, while the lookup table records the installed capacity (in kW) for each geography. At query time, dsgrid multiplies the normalized profile by the installed capacity to produce absolute generation values. This format avoids duplicating 8,760 hourly rows for every geography.

:::{list-table} load_data_lookup.parquet with scaling_factor
:header-rows: 1
:widths: 5 12 8 20 12 15

* - id
  - geography
  - sector
  - subsector
  - model_year
  - scaling_factor
* - 1
  - 01001
  - com
  - rooftop_pv
  - 2020
  - 10.5
* - 1
  - 01001
  - com
  - rooftop_pv
  - 2025
  - 102.3
* - 1
  - 01001
  - com
  - rooftop_pv
  - 2030
  - 245.7
* - …
  - …
  - …
  - …
  - …
  - …
:::

Multiple rows can share the same hourly profile shape in `load_data` by referencing the same ID, but produce different absolute values because their scaling factors differ.

:::{note}
If `scaling_factor` is `null` for a given row, the value passes through unchanged (i.e., no multiplication is applied). A scaling factor of `1.0` explicitly multiplies by one, which also leaves the value unchanged. Either is acceptable for rows that do not need scaling.
:::

This format minimizes file storage because:

1. Time arrays can be shared across combinations of dimension records, each with its own scaling factor.
2. Dimension information is not repeated for every timestamp. (This could be minimal because of compression inside the Parquet files.)

(value-formats)=
## Value Formats

Both table formats support two value layouts:

### Stacked

Each non-time dimension has its own column, plus a single `value` column containing the data. A `metric` column identifies which metric each row represents. This format is a good default choice.

The [one-table example](#one-table-format) above uses stacked format. The same layout works with two-table format: the `value`, `metric`, and any time columns appear in the data table, and other dimensions go in the lookup table.

### Pivoted

The record IDs of one dimension become column names in the data table, and each row contains all values for that combination of other (non-pivoted) dimensions at once. In practice, the pivoted dimension is almost always `metric`. The code allows pivoting on other dimensions, but this is uncommon.

The [two-table example](#two-table-format) above uses pivoted format. Here is a one-table pivoted equivalent in which the `metric` and `value` columns are replaced by one column per metric record ID:

:::{list-table} One-table pivoted example
:header-rows: 1
:widths: 28 10 12 25 15 15 20

* - timestamp
  - geography
  - scenario
  - subsector
  - heating
  - cooling
  - interior_equipment
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - 1.234
  - 0.002
  - 0.051
* - 2012-01-01T00:00:00+00:00
  - 01001
  - reference
  - primary_school
  - 2.345
  - 0.008
  - 0.073
* - 2012-01-01T01:00:00+00:00
  - 01001
  - reference
  - full_service_restaurant
  - 1.456
  - 0.003
  - 0.049
* - …
  - …
  - …
  - …
  - …
  - …
  - …
:::

Pivoted format saves storage space by avoiding repeated dimension values across rows, but can make inspection and data operations more complex because the column names carry semantic meaning. dsgrid converts pivoted data to stacked format during registration, so all downstream processing and queries operate on stacked data.

## Data Layout

The `data_layout` section of a dataset configuration defines the data file locations, table format, and value format. It has the following structure:

```javascript
data_layout: {
  table_format: "two_table",       // or "one_table"
  value_format: {
    format_type: "pivoted",        // or "stacked"
    pivoted_dimension_type: "metric",  // required if pivoted
  },
  data_file: {
    path: "load_data.parquet",
    columns: [                     // optional
      {
        name: "column_name",       // actual name in the file
        data_type: "STRING",       // optional, for type override
        dimension_type: "geography",  // optional, for column renaming
      },
    ],
    ignore_columns: ["col1", "col2"],  // optional, columns to drop
  },
  lookup_data_file: {              // required for "two_table" format
    path: "load_data_lookup.parquet",
    columns: [],                   // optional, same structure as data_file
    ignore_columns: [],            // optional, same as data_file
  },
  missing_associations: [          // optional, list of paths
    "missing_associations.parquet",
    "additional_missing",
  ],
}
```

**Fields:**

- `table_format`: Defines the table structure: `"two_table"` or `"one_table"`.
- `value_format`: Defines whether values are `"pivoted"` or `"stacked"`.
- `data_file`: Main data file configuration (required).
  - `path`: Path to the data file. Can be absolute or relative to the config file. You can also use the `--data-base-dir` CLI option to specify a different base directory for resolving relative paths.
  - `columns`: Optional list of column definitions for type overrides and renaming.
    - `name`: The actual column name in the file (required).
    - `data_type`: Data type override (optional). See [Specifying Column Data Types](#specifying-column-data-types) for supported types.
    - `dimension_type`: The dsgrid dimension type this column represents (optional, but required if this column is not a time column and `name` is not already a [dimension type](../../software_reference/data_models/enums.md#dimensiontype)). When specified, the column will be renamed to match the dimension type.
  - `ignore_columns`: Optional list of column names to drop when reading the file. Cannot overlap with columns defined in `columns`.
- `lookup_data_file`: Lookup file configuration (required for `two_table` format). Has the same structure as `data_file`.
- `missing_associations`: List of paths to files or directories defining missing dimension combinations (optional). Paths can be absolute or relative to the config file. You can also use the `--missing-associations-base-dir` CLI option to specify a different base directory for resolving relative paths. See [How to Handle Missing Dimension Associations](../how_tos/how_to_missing_associations) for details.

## Column Configuration

(csv-files)=
### CSV Files

While not generally recommended, dsgrid does support CSV data files. By default, dsgrid will let Spark and DuckDB attempt to infer the schema of the file. Because there may be cases of type ambiguities, such as integer vs string, integer vs float, and timestamps with time zones, dsgrid provides a mechanism for defining column data types directly in the dataset configuration.

Consider this example that uses county FIPS codes to identify the geography of each data point:

:::{list-table}
:header-rows: 1
:widths: 28 10 15 25 10

* - timestamp
  - geography
  - scenario
  - subsector
  - value
* - 2011-12-31T22:00:00-07:00
  - 01001
  - efs_high_ldv
  - full_service_restaurant
  - 1.234
* - 2011-12-31T22:00:00-07:00
  - 01001
  - efs_high_ldv
  - primary_school
  - 2.345
:::

The default behavior of IO libraries like Pandas, Spark, and DuckDB is to infer data types by inspecting the data. They will all decide that the geography column contains integers and drop the leading zeros. This will result in an invalid geography column, which is required to be a string data type, and will not match the project's geography dimension (assuming that the project is also defined over county FIPS codes).

Secondly, you may want to specify the minimum required size for each number. For example, if you don't need the precision that comes with 8-byte floats, choose `FLOAT` and Spark/DuckDB will store all values in 4-byte floats, halving the required storage size.

(specifying-column-data-types)=
#### Specifying Column Data Types

To specify column data types, add a `columns` field to the `data_file` (or `lookup_data_file`) section in your dataset configuration. You can specify types for all columns or just a subset — columns without explicit types will have their types inferred.

The supported data types are (case-insensitive):

- `BOOLEAN`: boolean
- `INT`: 4-byte integer
- `INTEGER`: 4-byte integer
- `TINYINT`: 1-byte integer
- `SMALLINT`: 2-byte integer
- `BIGINT`: 8-byte integer
- `FLOAT`: 4-byte float
- `DOUBLE`: 8-byte float
- `STRING`: string
- `TEXT`: string
- `VARCHAR`: string
- `TIMESTAMP_TZ`: timestamp with time zone
- `TIMESTAMP_NTZ`: timestamp without time zone

Example dataset configuration with column data types:

```javascript
data_layout: {
  table_format: "one_table",
  value_format: {
    format_type: "stacked",
  },
  data_file: {
    path: "load_data.csv",
    columns: [
      {
        name: "timestamp",
        data_type: "TIMESTAMP_TZ",
      },
      {
        name: "geography",
        data_type: "STRING",
      },
      {
        name: "scenario",
        data_type: "STRING",
      },
      {
        name: "subsector",
        data_type: "STRING",
      },
      {
        name: "value",
        data_type: "FLOAT",
      },
    ],
  },
}
```

You can also specify types for only the columns that need explicit typing:

```javascript
data_file: {
  path: "load_data.csv",
  columns: [
    {
      name: "geography",
      data_type: "STRING",  // Prevent FIPS codes from being read as integers
    },
  ],
}
```

(custom-column-names)=
### Custom Column Names

By default, dsgrid expects data files to have columns named after the standard dimension types (`geography`, `sector`, `subsector`, `metric`, etc.). Data files with value format "stacked" are also expected to have a `value` column. However, your data files may use different column names. dsgrid provides a mechanism to map custom column names to the expected dimension types.

To rename columns, add the `dimension_type` field to the column definition. This tells dsgrid what dimension the column represents, and dsgrid will automatically rename it at runtime.

This feature works for all file formats (Parquet, CSV), not just CSV files.

Example with custom column names:

```javascript
data_file: {
  path: "load_data.parquet",
  columns: [
    {
      name: "county",           // Actual column name in the file
      dimension_type: "geography",  // Will be renamed to "geography"
    },
    {
      name: "end_use",          // Actual column name in the file
      dimension_type: "metric",     // Will be renamed to "metric"
    },
    {
      name: "building_type",    // Actual column name in the file
      dimension_type: "subsector",  // Will be renamed to "subsector"
    },
  ],
}
```

You can combine `dimension_type` with `data_type` when using CSV files:

```javascript
data_file: {
  path: "load_data.csv",
  columns: [
    {
      name: "fips_code",
      data_type: "STRING",
      dimension_type: "geography",
    },
    {
      name: "value",
      data_type: "DOUBLE",
    },
  ],
}
```

### Ignoring Columns

Data files may contain columns that are not needed for dsgrid processing. Rather than modifying your source files, you can tell dsgrid to ignore (drop) specific columns when reading the data.

To ignore columns, add an `ignore_columns` field to the `data_file` (or `lookup_data_file`) section in your dataset configuration. This field accepts a list of column names to drop.

This feature works for all file formats (Parquet, CSV).

Example with ignored columns:

```javascript
data_file: {
  path: "load_data.parquet",
  ignore_columns: ["internal_id", "notes"],
}
```

You can combine `ignore_columns` with `columns` for type overrides and renaming:

```javascript
data_file: {
  path: "load_data.csv",
  columns: [
    {
      name: "county",
      data_type: "STRING",
      dimension_type: "geography",
    },
    {
      name: "value",
      data_type: "DOUBLE",
    },
  ],
  ignore_columns: ["internal_id", "notes"],
}
```

:::{note}
A column cannot appear in both `columns` and `ignore_columns` — dsgrid will raise an error if there is any overlap.
:::

## Time

dsgrid automates timeseries data alignment by requiring datasets and projects to clearly specify time conventions in their respective configuration files and then applying standardized transformations to align dataset time with project time. Project time is typically hourly, aligned with one or more weather years, and defined over a specific time zone that uses standard time (no daylight saving time). For interoperability, dsgrid stores timestamps in UTC.

Datasets might have timestamps already aligned for a single time zone, or they might be in local standard time, they might use representative time (daily load shapes for weekdays and weekends varying by month), etc. The intention is for dsgrid to handle the time transformations implied by dataset-project time mismatches, because these are typically a time sink and common source of errors. Thus if your dataset's time convention is not yet supported, please {ref}`contact the dsgrid team <contact-us>` to discuss.

### Time Zones

Timestamps must be converted to UTC when written to the Parquet files. Do not use the Pandas feature where it records time zone information into the Parquet metadata.

We recommend that you use Spark to create the Parquet files, but that is not required. If you do use Spark, note the following:

- Spark implicitly interprets timestamps in the time zone of the current SQL session and converts them to UTC when writing dataframes to Parquet.
- You can override the SQL session time zone programmatically or in your Spark configuration file. The setting is `spark.sql.session.timeZone`.

#### Time Zone Aware Timestamps

dsgrid can convert timestamps in data tables to the proper time zone by looking up the time dimension.

#### Time Zone Unaware Timestamps

Time-zone-unaware timestamps that will be interpreted as local time should be written as UTC timestamps (i.e., 12pm with no time zone should be written as 12pm UTC).

To resolve local times to absolute timestamps, dsgrid needs a time zone for each geographic location. There are two ways to provide this:

- **Dataset geography records** (default) — include a `time_zone` column with IANA time zone strings (e.g., `US/Eastern`) in the dataset's geography dimension records file.
- **Project geography records** — set `use_project_geography_time_zone: true` in the dataset config. dsgrid will look up time zones from the project's geography dimension instead. This is useful when the dataset's geography records do not include time zone information.

See [Dataset Concepts — Configuration Options](dataset_concepts.md#configuration-options) for more on this setting.

(time-formats)=
### Time Formats

#### DateTime

Datetime time dimensions use the `column_format` field in the time dimension config to describe how timestamps are represented in the data files. Three formats are supported:

##### Timezone-aware timestamps (`TIMESTAMP_TZ`)

This is the default. The load data table has one column representing time (typically called `timestamp`). When written to Parquet files the type should be the `TIMESTAMP` logical type (integer, not string) and be adjusted to UTC. When read into Spark the type should be `TimestampType`.

```javascript
// Time dimension config (default — can be omitted)
column_format: {
  dtype: "TIMESTAMP_TZ",
  time_column: "timestamp",
}
```

The [one-table](#one-table-format) and [two-table](#two-table-format) examples above both use this format — note the UTC-offset suffix on each timestamp (e.g., `2012-01-01T00:00:00+00:00`).

(timestamp-ntz)=
##### Timezone-naive timestamps (`TIMESTAMP_NTZ`)

For datasets where timestamps are local time without an explicit time zone. The data must include a `time_zone` column (or the time zone must be derivable from the geography dimension) so dsgrid can localize the values. When read into Spark the type should be `TimestampNTZType`.

```javascript
column_format: {
  dtype: "TIMESTAMP_NTZ",
  time_column: "timestamp",
}
```

In the data file the timestamp column contains naive timestamps with no time zone suffix:

:::{list-table} TIMESTAMP_NTZ data file example (two-table, pivoted)
:header-rows: 1
:widths: 25 8 15 15

* - timestamp
  - id
  - heating
  - cooling
* - 2012-01-01 00:00:00
  - 1
  - 0.214
  - 0.002
* - 2012-01-01 01:00:00
  - 1
  - 0.329
  - 0.000
* - …
  - …
  - …
  - …
:::

##### Time-in-parts

For datasets that store time components in separate columns (year, month, day, and optionally hour) rather than a single timestamp column. During registration, dsgrid automatically converts these columns into a single timestamp column.

```javascript
column_format: {
  dtype: "time_format_in_parts",
  year_column: "year",
  month_column: "month",
  day_column: "day",
  hour_column: "hour",    // optional — omit for daily data
  time_zone: "US/Eastern", // optional — if omitted, derived from geography
}
```

When `time_zone` is specified, the resulting timestamps are timezone-aware (`TIMESTAMP_TZ`) and the year, month, day, and hour columns are assumed to already be aligned in the specified time zone (US/Eastern in the example).

When `time_zone` is omitted, dsgrid assumes that year, month, day, and hour correspond to local time. To create specific timestamps in this case, dsgrid joins the geography dimension's records onto the data and pulls in the `time_zone` column — this requires that the geography dimension's records file includes a column named `time_zone` containing IANA time zone strings. The resulting data has a naive `timestamp` column (`TIMESTAMP_NTZ`) alongside the `time_zone` column, such that the combination is a fully specified point in time.

In the data file, time components appear as separate integer columns:

:::{list-table} Time-in-parts data file example (two-table, stacked)
:header-rows: 1
:widths: 8 8 8 8 8 12 10

* - year
  - month
  - day
  - hour
  - id
  - metric
  - value
* - 2012
  - 1
  - 1
  - 0
  - 1
  - heating
  - 0.214
* - 2012
  - 1
  - 1
  - 0
  - 1
  - cooling
  - 0.002
* - 2012
  - 1
  - 1
  - 1
  - 1
  - heating
  - 0.329
* - …
  - …
  - …
  - …
  - …
  - …
  - …
:::

:::{note}
The time-in-parts columns are dropped from the data during registration and replaced by a single `timestamp` column. The original data files are not modified.
:::

#### Datetime with External Time Zone

For datasets where timestamps are timezone-naive and the time zone is derived from the geography dimension. This uses a dedicated time dimension model ([DatetimeExternalTimeZoneDimensionModel](../../software_reference/data_models/dimension_model.md#datetimeexternaltimezonedimensionmodel)) rather than the `column_format` field used by the standard [DateTime](#datetime) format.

The data files look the same as [TIMESTAMP_NTZ](#timestamp-ntz) — naive timestamps with no time zone suffix. The geography dimension's records must include a `time_zone` column with IANA time zone strings so dsgrid can localize the timestamps.

```javascript
// Time dimension config
time_type: "datetime_external_tz",
time_zone_format: {
  format_type: "aligned_in_clock_time",
  time_zones: ["US/Eastern", "US/Central", "US/Mountain", "US/Pacific"],
},
ranges: [{
  start: "2012-01-01 00:00:00",
  end: "2012-12-31 23:00:00",
  str_format: "%Y-%m-%d %H:%M:%S",
  frequency: "01:00:00",
}],
time_interval_type: "period_beginning",
measurement_type: "total",
```

See [DatetimeExternalTimeZoneDimensionModel](../../software_reference/data_models/dimension_model.md#datetimeexternaltimezonedimensionmodel) for all config fields.

#### Annual

Annual time dimensions contain one value per year per dimension combination. The time dimension config specifies the range of years:

```javascript
// Time dimension config
time_type: "annual",
ranges: [{
  start: "2020",
  end: "2030",
  str_format: "%Y",
  frequency: 1,
}],
measurement_type: "total",
```

The data file includes a `model_year` column with integer year values:

:::{list-table} Annual data file example (one-table, stacked)
:header-rows: 1
:widths: 12 12 20 15 10

* - model_year
  - geography
  - subsector
  - metric
  - value
* - 2020
  - 01001
  - rooftop_pv
  - capacity_kw
  - 10.5
* - 2021
  - 01001
  - rooftop_pv
  - capacity_kw
  - 25.3
* - 2022
  - 01001
  - rooftop_pv
  - capacity_kw
  - 42.7
* - 2020
  - 01003
  - rooftop_pv
  - capacity_kw
  - 8.1
* - …
  - …
  - …
  - …
  - …
:::

See [AnnualTimeDimensionModel](../../software_reference/data_models/dimension_model.md#annualtimedimensionmodel) for all config fields.

#### Index

Index time dimensions use integer time steps rather than timestamp values. The time dimension config maps each index to a point in time via a starting timestamp and frequency:

```javascript
// Time dimension config
time_type: "index",
ranges: [{
  start: 0,
  end: 8783,
  starting_timestamp: "2012-01-01 00:00:00",
  str_format: "%Y-%m-%d %H:%M:%S",
  frequency: "P0DT1H0M0.000000S",  // 1 hour
}],
time_interval_type: "period_beginning",
measurement_type: "total",
```

In the data file the time column is an integer named `time_index`:

:::{list-table} Index data file example (two-table, pivoted)
:header-rows: 1
:widths: 12 8 15 15

* - time_index
  - id
  - heating
  - cooling
* - 0
  - 1
  - 0.214
  - 0.002
* - 1
  - 1
  - 0.329
  - 0.000
* - 2
  - 1
  - 0.369
  - 0.000
* - …
  - …
  - …
  - …
* - 8783
  - 1
  - 0.198
  - 0.001
:::

During registration, dsgrid converts the integer indices to timestamps using the config's starting timestamp and frequency. The original integer column is replaced by a proper timestamp column in the registered data.

See [IndexTimeDimensionModel](../../software_reference/data_models/dimension_model.md#indextimedimensionmodel) for all config fields.

#### Representative Period

Metric data contains timestamps that represent multiple periods. dsgrid supports the following formats:

##### one_week_per_month_by_hour

Each time array contains one week of hourly data (24 hours per day) that applies to an entire month. The times represent local time (no time zone). There are no shifts, missing hours, or extra hours for daylight savings time.

- All time columns must be integers.
- `month` is one-based, starting in January. `Jan` → 1, `Feb` → 2, etc.
- `day_of_week` is zero-based, starting on Monday. `Mon` → 0, `Tue` → 1, etc.
- `hour` is zero-based, starting at midnight.

:::{list-table} one_week_per_month_by_hour (two-table, pivoted metrics)
:header-rows: 1
:widths: 5 10 15 10 15 15

* - id
  - month
  - day_of_week
  - hour
  - L1andL2
  - DCFC
* - 1
  - 1
  - 0
  - 0
  - 0.523
  - 0.102
* - 1
  - 1
  - 0
  - 1
  - 0.498
  - 0.095
* - 1
  - 1
  - 0
  - 2
  - 0.412
  - 0.078
* - 1
  - 1
  - 0
  - 3
  - 0.387
  - 0.064
* - …
  - …
  - …
  - …
  - …
  - …
* - 1
  - 1
  - 6
  - 23
  - 0.601
  - 0.134
* - 1
  - 2
  - 0
  - 0
  - 0.545
  - 0.110
* - …
  - …
  - …
  - …
  - …
  - …
:::

:::{note}
This example uses the two-table format with pivoted metrics (`L1andL2`, `DCFC`). The `id` column links to the lookup table, which maps each ID to a combination of geography, scenario, subsector, etc. Only a subset of rows is shown — a complete dataset would have 2,016 rows per ID (12 months × 7 days × 24 hours).
:::

##### one_weekday_day_and_one_weekend_day_per_month_by_hour

Each time array contains one representative weekday and one representative weekend day of hourly data for each month. The times represent local time (no time zone).

- All time columns must be integers except `is_weekday`, which is boolean.
- `month` is one-based, starting in January. `Jan` → 1, `Feb` → 2, etc.
- `hour` is zero-based, starting at midnight.
- `is_weekday` is `true` for the weekday profile and `false` for the weekend profile.

:::{list-table} one_weekday_day_and_one_weekend_day_per_month_by_hour (one-table, stacked)
:header-rows: 1
:widths: 10 12 10 12 12 10

* - month
  - is_weekday
  - hour
  - geography
  - metric
  - value
* - 1
  - false
  - 0
  - 06037
  - electricity
  - 0.594
* - 1
  - false
  - 1
  - 06037
  - electricity
  - 0.162
* - 1
  - false
  - 2
  - 06037
  - electricity
  - 0.379
* - …
  - …
  - …
  - …
  - …
  - …
* - 1
  - true
  - 0
  - 06037
  - electricity
  - 0.612
* - …
  - …
  - …
  - …
  - …
  - …
:::

:::{note}
A complete dataset has 576 time rows per dimension combination (12 months × 2 day types × 24 hours).
:::

See [RepresentativePeriodTimeDimensionModel](../../software_reference/data_models/dimension_model.md#representativeperiodtimedimensionmodel) and [RepresentativePeriodFormat](../../software_reference/data_models/enums.md#representativeperiodformat) for all config fields and supported formats.

#### NoOp (No Time Dimension)

For time-invariant datasets that have no time component — for example, annual growth factors or static capacity values. The data files contain no time columns.

```javascript
// Time dimension config
time_type: "noop",
```

:::{list-table} NoOp data file example (one-table, stacked)
:header-rows: 1
:widths: 12 25 20 10

* - geography
  - subsector
  - metric
  - value
* - 01001
  - full_service_restaurant
  - growth_factor
  - 1.05
* - 01001
  - primary_school
  - growth_factor
  - 1.02
* - 01003
  - full_service_restaurant
  - growth_factor
  - 1.08
* - …
  - …
  - …
  - …
:::

See [NoOpTimeDimensionModel](../../software_reference/data_models/dimension_model.md#nooptimedimensionmodel) for all config fields.
