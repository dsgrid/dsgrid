# Dimension Concepts

The datasets dsgrid works with are typically highly multi-dimensional. A single data point might represent energy use for a particular end use and fuel type, in a certain type of building, at a specific hour in a particular county under a future scenario.
One of the key challenges of assembling coherent analyses from many such different datasets is aligning across all relevant *dimensions*.

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

Individual datasets might have 0, 1, or more fields that could be mapped to an individual dimension type, but we have generally found this list to be sufficient and workable, and important for enabling many disparate datasets to be mapped to a common set of dimensions and then analyzed as a whole.

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

Time dimensions work differently. Instead of a records CSV, they are defined entirely by parameters in the config, for example:

```javascript
{
  type: "time",
  name: "Hourly 2012 EST",
  "class": "Time",
  time_type: "datetime",
  ranges: [
    {
      start: "2012-01-01 01:00:00",
      end: "2013-01-01 00:00:00",
    },
  ],
  frequency: "01:00:00",
  timezone: "Etc/GMT+5",
  time_interval_type: "period_ending",
  measurement_type: "total",
}
```

### Trivial Dimensions

Not all dimension types need to be present in every dataset. A dimension with only one record — for example, a single scenario for historical data — is called a **trivial dimension**. Trivial dimensions must be declared in the dataset config, but their records do not need to appear in the data files. Their (single) record values do need to be defined, either in a file or in the config itself.

## Dimension Record Classes

Every dimension config has a `class` field that selects a **record class**. The record class determines what columns are required or optional in the dimension records CSV. All record classes require `id` and `name` columns; each class may add additional fields.For example, a metric dimension using the `EnergyEndUse` class requires `fuel_id` and `unit` columns in addition to `id` and `name`.

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

Time dimensions work differently from other dimensions. Instead of records in a CSV file, they are defined by parameters like time ranges and frequency. dsgrid supports the following time dimension types:

- [**DateTimeDimensionModel**](../../software_reference/data_models/dimension_model.md#datetimedimensionmodel) - Standard datetime timestamps with configurable time zones, ranges, and frequency
- [**AnnualTimeDimensionModel**](../../software_reference/data_models/dimension_model.md#annualtimedimensionmodel) - Yearly aggregated data with configurable year ranges
- [**RepresentativePeriodTimeDimensionModel**](../../software_reference/data_models/dimension_model.md#representativeperiodtimedimensionmodel) - Typical periods (e.g., typical week or day)
- [**DatetimeExternalTimeZoneDimensionModel**](../../software_reference/data_models/dimension_model.md#datetimeexternaltimezonedimensionmodel) - Datetime data where time zones are defined outside dsgrid (e.g., in a separate column)
- [**IndexTimeDimensionModel**](../../software_reference/data_models/dimension_model.md#indextimedimensionmodel) - Integer-indexed time steps mapped to a starting timestamp and frequency
- [**NoOpTimeDimensionModel**](../../software_reference/data_models/dimension_model.md#nooptimedimensionmodel) - Time-invariant data (no time component)

## Learn More

- [Dimension Data Models](../../software_reference/data_models/dimension_model.md) - Config model specifications
- [Dimension Record Classes](../../software_reference/data_models/dimension_classes.md) - Full listing and tables of fields for all record classes
- [How to Define Dimensions](../how_tos/how_to_dimensions) - Step-by-step workflow
- [Dataset Concepts](dataset_concepts) - Learn about datasets, including dataset types and file formats
