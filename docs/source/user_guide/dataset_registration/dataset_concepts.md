# Dataset Concepts

A **dsgrid dataset** is a self-contained collection of metric data, typically the output of a single model or data source, together with a [dataset config](../../software_reference/data_models/dataset_model). The configuration file describes metadata, including data provenance, other attributes, dimensionality, and file format. Datasets typically cover a specific domain (e.g., buildings, electric vehicles, distributed generation, historical utility sales, etc.) and contribute to the larger energy picture assembled by a dsgrid project.

## Kinds of Datasets

### By Type

The `dataset_type` field classifies the origin of the data:

- **Modeled** — data generated an energy model (e.g., ComStock building energy simulations, TEMPO EV charging profiles, dGen distributed PV capacity projections).
- **Historical** — real-world observed data from past measurements (e.g., EIA 861 utility customer sales).
- **Benchmark** — reference datasets used for calibration or comparison (e.g., AEO energy by end-use projections).

### By Domain

dsgrid projects typically assemble datasets from multiple domains to build a comprehensive picture of energy demand. Common examples include:

- **Sector-level modeled energy use** — hourly building energy simulations covering end uses like heating, cooling, and lighting for residential or commercial building types (e.g., ResStock, ComStock); industrial energy demand projected by an integrated assessment model (e.g., GCAM) and downscaled using other data sources.
- **Distributed energy resources** — capacity and hourly generation profiles for technologies such as rooftop solar PV (e.g., dGen). A single technology may produce two linked datasets: one for installed capacity and one for normalized generation profiles.
- **Transportation electrification** — EV charging load profiles by vehicle type and charging level (e.g., TEMPO).
- **Historical electricity data** — observed utility sales or grid load, often at annual or monthly resolution (e.g., EIA 861).
- **Benchmark projections and growth factors** — reference-case energy projections or annual growth rates used to scale or calibrate modeled data (e.g., AEO end-use projections).

### By Data Classification

The `data_classification` field describes the nature of the values:

- **Quantity** (default) — absolute values such as energy consumption (kWh), capacity (kW), or count.
- **Growth rate** — multiplicative factors applied to scale other datasets over time. Growth-rate datasets may include additional metadata fields such as `growth_rate_base_year` and `growth_rate_base_year_value`.

## Dataset Dimensions

Each dataset defines values over up to eight [dimension types](dimension_concepts.md#dimension-types). A dataset config either defines dimensions inline (with records files) or references already-registered dimensions by ID.

### Shared vs. Dataset-Specific Dimensions

In practice, some dimensions are often **shared across a project** because all contributing datasets need to align on the same elements:

- **Geography** — spatial units (counties, states, census regions). Often project-defined, though datasets may use finer resolution (e.g., census tracts) and map to the project level.
- **Scenario** — modeling scenarios (e.g., reference, high electrification). Usually project-defined.
- **Model year** and **weather year** — typically project-defined.

Other dimensions are more **dataset-specific** because they reflect the internal structure of a particular model:

- **Subsector** — building types, industries, vehicle classes, etc. Each model has its own categorization.
- **Metric** — measured quantities (energy end uses, capacity, charging profiles, population). Different models measure different things.
- **Sector** — while the project defines the overall sectors (residential, commercial, industrial, transportation), individual datasets usually cover only one.

### Multiple Metric Types in a Project

Different datasets in the same project may measure fundamentally different things — for example, energy consumption (kWh) vs. installed capacity (kW) vs. vehicle counts. In dsgrid, each metric type is a separate dataset with its own metric dimension records and [record class](dimension_concepts.md#dimension-record-classes).

A project can define **multiple base dimensions of the same type** to accommodate this. For example, a project might have three metric base dimensions: one for energy end uses, one for DPV generation profiles, and one for DPV capacity. Each dataset is assigned to the appropriate metric dimension through the project's `required_dimensions` configuration.

### Trivial Dimensions

Not all dataset dimensions are significant. For example, historical data will generally have a trivial (i.e., one-element) `scenario` dimension, and a single-sector dataset will have a trivial `sector` dimension. These one-element dimensions are called **trivial dimensions**.

Trivial dimensions do not need to appear as columns in the data files — they are declared in the dataset config and added by dsgrid at runtime. This saves storage space and simplifies the data files. See [Trivial Dimensions](dimension_concepts.md#trivial-dimensions) for details.

### Inline Dimensions vs. Dimension References

A dataset config specifies its dimensions in one of two ways:

`dimensions`
: Define a dimension **inline** by providing its records file and metadata directly in the config. dsgrid will automatically register the dimension during dataset registration. Use this when the dimension is unique to your dataset (e.g., a custom set of building subsectors or metric end uses).

`dimension_references`
: **Reference** an already-registered dimension by its `dimension_id` (a UUID assigned when the dimension was first registered), `dimension_type`, and `version`. Use this when you want to reuse a dimension from the project or from another dataset — for example, a shared geography or scenario dimension that the project admin has already registered. You do not need to look up UUIDs manually: `dsgrid registry datasets generate-config` automatically writes `dimension_references` entries for any dimensions it matches in the registry.

You can mix both styles in the same config. A common pattern is to reference project-defined dimensions (geography, scenario, model year, weather year) and define dataset-specific dimensions inline (subsector, metric).

## How Datasets Relate to Projects

Datasets are **standalone entities** — they can be registered independently of any project. A **project** assembles multiple datasets into a coherent whole by defining common base dimensions that all datasets must map onto.

### Three Dataset Operations

Getting a dataset into a project involves up to three operations:

1. **Registration** — validates the dataset's internal consistency: schema, dimensions, and data completeness. The dataset becomes a versioned entity in the registry. No project is required. (`dsgrid registry datasets register`)

2. **Submission** — submits a registered dataset to a specific project. This step requires **dimension mappings** that align each dataset dimension to the corresponding project base dimension. dsgrid validates that the mappings are consistent and that the dataset provides all expected data points. (`dsgrid registry projects submit-dataset`)

3. **Combined register-and-submit** — performs both operations in a single command, which is convenient during iterative development. (`dsgrid registry projects register-and-submit-dataset`)

Dimension mappings are often the most labor-intensive part of the process. A mapping defines how each dataset dimension record corresponds to one or more project dimension records — for example, mapping ComStock building types to the project's standard building categories, or aggregating census-tract geographies up to counties.

See [Dataset Submitters](../../getting_started/dataset_submitters) for the full workflow and [How to Create Dataset Dimensions](../how_tos/how_to_dimensions) for guidance on dimension records.

## Configuration Options

Most dataset config fields are self-explanatory or covered by the [schema reference](../../software_reference/data_models/dataset_model). Two boolean flags deserve additional explanation:

`use_project_geography_time_zone`
: When `true`, dsgrid derives each record's time zone from the **project's** geography dimension (which must include a `time_zone` column) rather than from the dataset's own geography records. Set this to `true` when your timestamps represent local time but your dataset's geography dimension does not include a `time_zone` column — for example, TEMPO and dGen datasets whose time values are local to the modeled location. When `false` (the default), the dataset's own geography records must provide the `time_zone` column. See [Time Formats](data_file_formats.md#time-formats) for details on how dsgrid handles time zones.

`enable_unit_conversion`
: When `true` (the default), dsgrid performs automatic unit conversion at query time by comparing the `unit` column in the dataset's metric dimension records with the corresponding project metric records. Set this to `false` only when the dataset's **dimension mapping** for the metric dimension already accounts for the unit difference through its mapping fractions. In that case, dsgrid's built-in conversion would double-count the scaling.

## File Format

A dataset must comply with a supported dsgrid data file format. The main choices are:

- **Table format**: [one-table](data_file_formats.md#one-table-format) or [two-table](data_file_formats.md#two-table-format)
- **Value format**: [stacked or pivoted](data_file_formats.md#value-formats)

See [Data File Formats](data_file_formats) for requirements, recommendations, and detailed examples.

## Examples

The [dsgrid-StandardScenarios repository](https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/dsgrid_project/datasets) contains dataset configs that illustrate a range of domains and formats:

### Historical

- [EIA 861 Utility Customer Sales (MWh) by State by Sector by Year for 2010-2020](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/historical/eia_861_annual_energy_use_state_sector/dataset.json5) — annual historical utility sales data; one-table stacked format.

### Modeled

- [ResStock](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/resstock/dataset.json5) — hourly residential building energy simulations; two-table pivoted on metric.
- [ComStock](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/comstock/dataset.json5) — hourly commercial building energy simulations; two-table pivoted on metric.
- [TEMPO](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/tempo/dataset.json5) — EV charging load profiles; two-table pivoted on metric with representative-period time.

### Benchmark / Growth Factors

- [AEO 2021 Reference Case Residential Energy End Use Annual Growth Factors](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/aeo2021_reference/residential/End_Use_Growth_Factors/dataset.json5) — unitless growth rates; one-table pivoted on metric.

:::{seealso}
- [Dataset Data Model](../../software_reference/data_models/dataset_model) — full schema reference for the dataset config
- [Data File Formats](data_file_formats) — file format requirements and examples
- [Dimension Concepts](dimension_concepts) — dimension types, records, and record classes
- [Dataset Submitters](../../getting_started/dataset_submitters) — step-by-step workflow for registration and submission
:::
