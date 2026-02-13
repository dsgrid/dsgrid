# Dimension Record Classes

These classes define the schema for dimension record files (typically CSVs).
Every dimension config has a `class` field that selects one of these classes.
The class determines what columns are required or optional in the records file.

:::{note}
Time dimensions do not use record classes. They are configured entirely by
parameters in the config file. See the [Dimensions](dimension_model.md) reference
for time dimension config models (DateTimeDimensionModel, AnnualTimeDimensionModel, etc.).
:::

## Geography

### CensusDivision

*dsgrid.dimension.standard.CensusDivision*

Census Region attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>

### CensusRegion

*dsgrid.dimension.standard.CensusRegion*

Census Region attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>

### County

*dsgrid.dimension.standard.County*

County attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `state` | `str` | *(required)* |  |

</div>

### Geography

*dsgrid.dimension.standard.Geography*

Generic geography with optional time_zone

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>

### State

*dsgrid.dimension.standard.State*

State attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `is_conus` | `bool | None` | `None` |  |
| `census_division` | `str` | `""` |  |
| `census_region` | `str` | `""` |  |

</div>

---

## Sector

### Sector

*dsgrid.dimension.standard.Sector*

Sector attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `category` | `str` | `""` | Sector dimension |

</div>

---

## Subsector

### Subsector

*dsgrid.dimension.standard.Subsector*

Subsector attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `sector` | `str` | `""` |  |
| `abbr` | `str` | `""` |  |

</div>

---

## Metric

### EnergyEfficiency

*dsgrid.dimension.standard.EnergyEfficiency*

Energy Efficiency of building stock or equipment

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `fuel_id` | `str` | *(required)* |  |
| `unit` | `str` | *(required)* |  |

</div>

### EnergyEndUse

*dsgrid.dimension.standard.EnergyEndUse*

Energy Demand End Use attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `fuel_id` | `str` | *(required)* |  |
| `unit` | `str` | *(required)* |  |

</div>

### EnergyIntensity

*dsgrid.dimension.standard.EnergyIntensity*

Energy Intensity per capita, GDP, etc.

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |

</div>

### EnergyIntensityRegression

*dsgrid.dimension.standard.EnergyIntensityRegression*

Energy Intensity per capita, GDP, etc. regression over time or other variables

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `regression_type` | [`FunctionalForm`](enums.md#functionalform) | `"linear"` | Specifies the functional form of the regression model |
| `unit` | `str` | *(required)* |  |

</div>

### EnergyServiceDemand

*dsgrid.dimension.standard.EnergyServiceDemand*

Energy Service Demand attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |

</div>

### EnergyServiceDemandRegression

*dsgrid.dimension.standard.EnergyServiceDemandRegression*

Energy Service Demand, can be per floor area, vehicle, etc., regression
over time or other variables

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `regression_type` | [`FunctionalForm`](enums.md#functionalform) | `"linear"` | Specifies the functional form of the regression model |
| `unit` | `str` | *(required)* |  |

</div>

### FractionalIndex

*dsgrid.dimension.standard.FractionalIndex*

Fractional Index attributes - e.g., human development index (HDI)

Generally dimensionless, but a unit string can be provided to assist with
calculations.

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |
| `min_value` | `float` | *(required)* |  |
| `max_value` | `float` | *(required)* |  |

</div>

### PeggedIndex

*dsgrid.dimension.standard.PeggedIndex*

Pegged Index attributes

Data relative to a base year that is normalized to a value like 1 or 100.

Generally dimensionless, but a unit string can be provided to assist with
calculations.

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |
| `base_year` | `int` | *(required)* |  |
| `base_value` | `float` | *(required)* |  |

</div>

### Population

*dsgrid.dimension.standard.Population*

Population attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |

</div>

### Stock

*dsgrid.dimension.standard.Stock*

Stock attributes - e.g., GDP, building stock, equipment

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |

</div>

### StockRegression

*dsgrid.dimension.standard.StockRegression*

Stock, can be per capita, GDP, etc., regression over time or other variables

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `regression_type` | [`FunctionalForm`](enums.md#functionalform) | `"linear"` | Specifies the functional form of the regression model |
| `unit` | `str` | *(required)* |  |

</div>

### StockShare

*dsgrid.dimension.standard.StockShare*

Stock Share attributes - e.g., market share of a technology

Generally dimensionless, but a unit string can be provided to assist with
calculations.

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |
| `unit` | `str` | *(required)* |  |

</div>

---

## Model Year

### ModelYear

*dsgrid.dimension.standard.ModelYear*

Model Year attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>

---

## Weather Year

### WeatherYear

*dsgrid.dimension.standard.WeatherYear*

Weather Year attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>

---

## Scenario

### Scenario

*dsgrid.dimension.standard.Scenario*

Scenario attributes

<div class="model-fields-table">

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `str` | *(required)* | Unique identifier within a dimension |
| `name` | `str` | *(required)* | User-defined name |

</div>
