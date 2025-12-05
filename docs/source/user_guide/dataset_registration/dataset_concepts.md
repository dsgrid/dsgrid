# Dataset Concepts

A dataset is defined by its [dataset config](../../software_reference/data_models/dataset_config). This data structure describes the dataset's attributes, dimensionality, and file format characteristics.

## Dataset Types

dsgrid supports three types of datasets:

1. **Benchmark** - Reference datasets used for comparison
2. **Historical** - Real-world observed data from past measurements
3. **Modeled** - Simulation or model-generated data

## Dimensions

Similar to projects, dataset configs can contain already-registered dimension IDs or unregistered dimension configs. Refer to [](concepts.md) for background on dimensions and mappings. Dataset contributors will likely want to define new dimensions in the dataset config using the streamlined workflow.

## Trivial Dimensions

Not all dataset dimensions have to be significant. For example, historical data will generally have a trivial (i.e., one-element) scenario dimension. We call these one-element dimensions **trivial dimensions**.

For space-saving reasons, these trivial dimensions do not need to exist in the dataset files. However, they must be declared as trivial dimensions in the dataset config.

## File Format

A dataset must comply with a supported dsgrid file format. Please refer to [](../../software_reference/dataset_formats) for options.

## Submit-to-Project

A dataset must be submitted to a project before it can be used in dsgrid queries. The dsgrid submission process verifies that the dataset's dimensions either meet the project requirements or have valid mappings.

Learn more about the submission process in the [dataset submission tutorial](../../user_guide/tutorials/create_and_submit_dataset).

## Examples

The [dsgrid-StandardScenarios repository](https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/dsgrid_project/datasets) contains datasets that you can use as examples.

### Historical Example

[EIA 861 Utility Customer Sales (MWh) by State by Sector by Year for 2010-2020](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/historical/eia_861_annual_energy_use_state_sector/dataset.json5)

### Modeled Examples

- [ResStock eulp_final](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/resstock/dataset.json5)
- [AEO 2021 Reference Case Residential Energy End Use Annual Growth Factors](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/aeo2021_reference/residential/End_Use_Growth_Factors/dataset.json5)

:::{seealso}
**More detailed content:** [Datasets (detailed)](../../explanations/components/datasets)
:::
