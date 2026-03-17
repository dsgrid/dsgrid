# Dimension Concepts

The datasets dsgrid works with are typically highly multi-dimensional. A single data point might represent energy use for a particular end use and fuel type, in a certain type of building, at a specific hour in a particular county under a future scenario. One of the key challenges of assembling coherent analyses from many disparate datasets is aligning across all relevant *dimensions*.

## Dimension Types

From previous work, the dsgrid team has found it important to define and map data over eight different [dimension types](../../software_reference/data_models/enums.md#dimensiontype):

- **scenario** - Modeling scenarios or cases (e.g., reference, high electrification)
- **model_year** - Historical or future years for which data is reported, modeled, or projected
- **weather_year** - Years representing weather patterns used; also typically matches calendar year
- **geography** - Spatial units (e.g., counties, states, census regions)
- **time** - Temporal resolution and format (e.g., hourly timestamps, annual totals, representative periods)
- **sector** - Broad economic sectors (e.g., residential, commercial, industrial, transportation, electricity)
- **subsector** - Detailed sector breakdowns (e.g., building types, industries, transportation modes)
- **metric** - Measured quantities and their attributes (e.g., energy end use, energy intensity, population, stock)

Individual datasets might have zero, one, or more fields that map to any given dimension type. Although the actual dimensionality of datasets varies, this list of eight types has proven sufficient and workable for mapping many disparate datasets to a common set of dimensions for combined analysis.

## Dimension Configs and Records

Specific instances of a dimension type are defined by a **dimension configuration** and, in most cases, a table of **dimension records** (usually a CSV file). A dimension records file has one row per element of that dimension. For example, a sector dimension might have rows for "Commercial" and "Residential". The records' `id` values are what appear in the dataset's column for that dimension type.

A dimension config specifies the dimension's type, name, record class, and the path to the records file:

```json5
{
  type: "sector",
  name: "EFS Sectors",
  "class": "Sector",
  file: "dimensions/sectors.csv",
  description: "Residential and commercial sectors",
}
```
:::{note}
The key `class` must be quoted because it is a JavaScript reserved word, and JSON5 is based on JavaScript syntax.
:::

The corresponding records file has one row per record. All records must have `id` and `name` columns; additional columns depend on the [record class](#dimension-record-classes):

```text
id,name
com,Commercial
res,Residential
```

Records can also be listed directly in the configuration. For example:

```json5
{
  type: "sector",
  name: "EFS Sectors",
  "class": "Sector",
  description: "Residential and commercial sectors",
  records: [
    {id: "com", name: "Commercial"},
    {id: "res", name: "Residential"},
  ],
}
```

### Time Dimensions

Time dimensions work differently. Instead of a records CSV, they are defined entirely by parameters in the config. The `time_type` field selects the time dimension variant, and the `class` field must reference the matching class from `dsgrid.dimension.standard`.

A datetime time dimension config serves two purposes: it describes how the timestamp column is stored in the data table (via `column_format`), and it describes what the time data represents (via the remaining fields) so that dsgrid can validate the data table on registration.

The following example shows a `datetime` time dimension with hourly timestamps aligned to a single time zone:

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
    time_zone: "America/New_York",
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```

The `column_format` field specifies how time is stored in the data table. Three dtypes are supported:

- **`timestamp_tz`** — a single timezone-aware timestamp column (default). The `time_column` field sets the column name (default: `"timestamp"`). Works with both fixed offset time zones and daylight savings observing time zones in the config.
- **`timestamp_ntz`** — a single timezone-naive timestamp column. Same `time_column` field. Any time zone specified in the config must be null for no localization or in standard time (fixed offset) for localization (see [Time Zone Localization](#time-zone-localization)). Localization does not work with time zones that observe daylight savings due to inability to localize fallback duplicate timestamps accurately.
- **`time_format_in_parts`** — time is split across multiple integer columns instead of a single timestamp column. Required columns are `year_column`, `month_column`, and `day_column`; optional columns are `hour_column` (defaults to 0 for all rows if omitted) and `offset_column` (UTC offset in hours, e.g. `-8` or `"-08:00"`). dsgrid automatically combines the part columns into a single column named `timestamp` on registration.

For practical examples of how these formats appear in actual Parquet data files (including both single and two-table layouts), see [Data File Formats — Time Formats](../data_file_formats.md#time-formats).

```javascript
  // timezone-aware single column (default):
  column_format: {dtype: "timestamp_tz", time_column: "timestamp"}

  // timezone-naive single column:
  column_format: {dtype: "timestamp_ntz", time_column: "timestamp"}

  // time split across multiple columns:
  column_format: {
    dtype: "time_format_in_parts",
    year_column: "year",
    month_column: "month",
    day_column: "day",
    hour_column: "hour",       // optional; omit to default all hours to 0
    offset_column: "utc_offset",  // optional
  }
```

Although the schema accepts multiple `ranges` entries, dsgrid currently only supports a single continuous range. The time zone is specified through `time_zone_format`, which supports two variants:

- **`aligned_in_absolute_time`** — all geographies share the same timestamps in absolute time. Provide a single `time_zone` (an IANA time zone string such as `"America/New_York"` or `"Etc/GMT+5"`) or `None` for no time zone. Accepted `time_zones` types depend on `column_format`. When input timestamps are tz-aware, both fixed offset and DST-observing zones are accepted. When timestamps are tz-naive, only fixed UTC offset zones (due to localization requirement) or `None` are allowed.
- **`aligned_in_std_clock_time`** — timestamps cover the same interval of standard clock time across geographies (e.g., all of 2012 as experienced locally in standard time). The data table must have a `time_zone` column with per-row IANA time zones. Provide a `time_zones` list of all unique time zones in the data table. Accepted `time_zones` types depend on `column_format`. When input timestamps are tz-aware, both fixed offset and DST-observing zones are accepted. When timestamps are tz-naive, only fixed UTC offset zones are allowed due to localization requirement. `None` is not allowed as timestamps cannot be a mix of timezone-aware and -naive types.

(column-format)=

For details on how `time_zone` columns are sourced and structured in actual data files, see [Data File Formats — Time Zone Column Sourcing](../data_file_formats.md#time-zone-column-sourcing).

Example using local standard clock time (multiple time zones):

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
    time_zones: ["Etc/GMT+5", "Etc/GMT+6", "Etc/GMT+7", "Etc/GMT+8"],
  },
  time_interval_type: "period_beginning",
  measurement_type: "total",
}
```
`Etc/GMT+5` through `Etc/GMT+8` are the IANA fixed-offset zones corresponding to UTC−5 through UTC−8 (US Eastern through Pacific standard time offsets). These time zones must observe standard time (no daylight savings) because dsgrid will localize the timezone-naive timestamps (`timestamp_ntz`) (see [Time Zone Localization](#time-zone-localization)).

For detailed examples for each time dimension type, see [How to Define a Time Dimension](../how_tos/how_to_time_dimension).



#### Time Zone Localization
When the timestamps in the data table are parsed as timezone-naive (`timestamp_ntz`, `time_format_in_parts` without `offset_column`) but the config specifies a timezone, dsgrid automatically localizes the timestamps during dataset registration. All time zone(s) must be in standard time (fixed offset without daylight savings) for time zone localization because duplicated tz-naive timestamps cannot be localized accurately.

For `aligned_in_absolute_time`, localization uses `time_zone_format.time_zone`.

For `aligned_in_std_clock_time`, localization uses the per-row `time_zone` column in the data table. Every value in that column must be one of the IANA time zone strings listed in `time_zone_format.time_zones`.

To store timezone-naive timestamps without any localization, set `format_type` to `aligned_in_absolute_time` and `time_zone` to `null`:

For practical examples of timezone-naive data in actual Parquet files, see [Data File Formats — Timezone-naive timestamps](../data_file_formats.md#timestamp-ntz).

```javascript
  ...
  column_format: {
    dtype: "timestamp_ntz",
    time_column: "timestamp",
  },
  time_zone_format: {
    format_type: "aligned_in_absolute_time",
    time_zone: null,
  },
```

### Trivial Dimensions

Not all dimension types need to be present in every dataset. A dimension with only one record — for example, a single scenario for historical data — is called a **trivial dimension**. Trivial dimensions must be declared in the dataset config, but their records do not need to appear in the data files. Their (single) record values do need to be defined, either in a file or in the config itself.

## Dimension Record Classes

Every dimension config has a `class` field that selects a **record class**. The record class determines what columns are required or optional in the dimension records CSV. All record classes require `id` and `name` columns; each class may add additional fields. For example, a metric dimension using the `EnergyEndUse` class requires `fuel_id` and `unit` columns in addition to `id` and `name`.

The `class` field must reference a class from the `dsgrid.dimension.standard` module. The available classes for each dimension type are listed in the [Dimension Record Classes](../../software_reference/data_models/dimension_classes) reference. Metric dimensions have the most variety. Choose the class that best matches what your data represents, or {ref}`contact-us` to suggest a new metric type:

| Metric Class | Description | Key Fields |
|-------|-------------|------------|
| **EnergyEndUse** | Energy demand by end use | `fuel_id`, `unit` |
| **EnergyEfficiency** | Efficiency of building stock or equipment | `fuel_id`, `unit` |
| **EnergyServiceDemand** | Energy service demand (e.g., heating degree-hours) | `unit` |
| **EnergyServiceDemandRegression** | Service demand regression over time | `unit`, `regression_type` |
| **EnergyIntensity** | Energy intensity per capita, GDP, etc. | `unit` |
| **EnergyIntensityRegression** | Energy intensity regression over time | `unit`, `regression_type` |
| **Population** | Population counts | `unit` |
| **Stock** | Stock quantities (GDP, building stock, equipment) | `unit` |
| **StockRegression** | Stock regression over time | `unit`, `regression_type` |
| **StockShare** | Market share of a technology (generally dimensionless) | `unit` |
| **FractionalIndex** | Bounded index (e.g., HDI) | `unit`, `min_value`, `max_value` |
| **PeggedIndex** | Index relative to a base year (e.g., normalized to 1 or 100) | `unit`, `base_year`, `base_value` |
| **WeatherVariable** | Weather attributes (e.g., dry bulb temperature, relative humidity) | `unit` |

See the [Dimension Record Classes](../../software_reference/data_models/dimension_classes.md) reference for full field definitions, including accepted enum values.

## Time Dimensions

Time dimensions work differently from other dimensions. Instead of records in a CSV file, they are defined by parameters like time ranges and time zone format. dsgrid supports the following time dimension types, selected via the `time_type` field:

| `time_type` | `class` | Description |
|---|---|---|
| `datetime` | `Time` | Standard datetime timestamps; specify `time_zone_format` for single or per-geography time zones |
| `annual` | `AnnualTime` | Yearly aggregated totals; ranges use year strings (e.g., `"2020"`) |
| `representative_period` | `Time` | Typical periods (e.g., one week per month by hour); specify `format` and month `ranges` |
| `index` | `Time` | Integer-indexed time steps mapped to a starting timestamp and frequency |
| `noop` | `NoOpTime` | Time-invariant data; no time component in the dataset |

## Learn More

- [Dimension Data Models](../../software_reference/data_models/dimension_model.md) - Config model specifications
- [Dimension Record Classes](../../software_reference/data_models/dimension_classes.md) - Full listing and tables of fields for all record classes
- [How to Define Dimensions](../how_tos/how_to_dimensions) - Step-by-step workflow
- [How to Define a Time Dimension](../how_tos/how_to_time_dimension) - Detailed examples for each time dimension type
- [Dataset Concepts](dataset_concepts) - Learn about datasets, including dataset types and file formats
