# Dimension Concepts

## Dimension Types

dsgrid specifies the allowable dimension types. From previous work, the dsgrid team has found it important to clearly differentiate datasets and data based on:

- **scenario** - What scenario is being described (if any)
- **model_year** - What historical or future year is being described
- **weather_year** - What year's weather patterns are being used; also analogous with calendar year
- **geography** - What geographic entity is being summarized
- **time** - Timestamps, indicator of annual data, or other descriptor of how within-year data is provided
- **sector** - Commercial, Residential, Industrial, Transportation
- **subsector** - Specific building types, industries, transportation modes, etc.
- **metric** - (including energy differentiated by end-use) - Further breakdowns of energy use, energy service, population, etc.

## Dimension Configs and Records

Specific instances of a dimension type are defined by a dimension configuration and, in most cases, a table of **dimension records** (usually a CSV file). Dimension records have a row for each record, i.e., for each individual element in a given dimension, to which data points can be assigned.

dsgrid time dimensions typically do not have records because the values can be generated programmatically from parameters like starting and ending timestamps and frequency. Here are the time formats supported by dsgrid:

- Date-time dimensions - Standard datetime timestamps
- Annual time dimensions - Yearly aggregated data
- Representative period time dimensions - Typical periods (e.g., typical week)
- No-op time dimensions - Time-invariant data

:::{seealso}
For detailed specifications, see the [Dimension Data Model](../../software_reference/data_models/dimension_model.md).
:::
