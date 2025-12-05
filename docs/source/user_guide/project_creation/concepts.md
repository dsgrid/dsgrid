# Project Concepts

A dsgrid project is a distributed dataset, meaning that it is made up of one or more independently registered datasets. For example, a dsgrid project may be made up of many sector model datasets that are compiled together to represent a holistic dataset on energy demand across multiple sectors.

We call these **input datasets** and they are registered to dsgrid by sector modelers (also called dsgrid data contributors). Each input dataset has its own set of dimension definitions and its own Parquet file paths (hence the "distributed dataset"). When you query dsgrid for a published dataset, you are really querying many datasets registered with a dsgrid project, e.g., `dsgrid_standard_scenarios_2021`.

## Dimension Categories

Dimensions are used in dsgrid to define expectations of the project, allowable dimension transformations, and dimension properties of underlying datasets. As such, dimensions are defined both at the project and dataset levels. More specifically, there are three categories of dimensions used in dsgrid:

- **Base Dimension** - Base dimensions are the core dimensions of a project. They define the project's dimension expectations. All datasets must adhere to the project's base dimension definition or provide an appropriate dimension mapping to the base dimension.

- **Supplemental Dimension** - These are supplemental dimensions defined by the project to support different queries and data aggregations or disaggregations.

## Dimension Requirements

dsgrid projects define per-dataset requirements that specify what records must be present for each dimension. This allows datasets to exclude non-applicable dimensions. For example, a residential dataset will not have transportation energy use data.

## Project Repository

Every dsgrid project needs a GitHub repository to store all configs and miscellaneous scripts and to collaborate on project decisions. An example dsgrid project repository is the [dsgrid-project-StandardScenarios](https://github.com/dsgrid/dsgrid-project-StandardScenarios) repo.

### Project Repo Organization

We recommend that dsgrid project repositories use the following directory organization structure:

```text
.
├── dsgrid_project
    ├── datasets
    │   ├── benchmark
    │   ├── historical
    │   └── modeled
    │       ├── comstock
    │       │   ├── dimension_mappings
    │       │   ├── dimensions
    │       │   ├── dataset.json5
    │       │   ├── dimension_mappings.json5
    │       └── ...
    ├── dimension_mappings
    ├── dimensions
    └── project.json5
```

In the directory structure above, all project files are stored in the `dsgrid_project` root directory. Within this root directory we have:

- **datasets** - This is where we define input datasets, organized first by type (i.e., `datasets/historical`, `datasets/benchmark`, `datasets/modeled`) and then by source (e.g., `datasets/modeled/comstock`) for the dsgrid project
- **dimension_mappings** - This is where we store *project-level* dimension mapping records (CSV or JSON)
- **dimensions** - This is where we store *project-level* dimension records (CSV or JSON)
- **project.json5** - This is the main project configuration file

Within each `datasets/{type}/{source}` sub-folder are the following files:

- **dimension_mappings** - This is where we store *dataset-level* dimension mapping records (CSV or JSON)
- **dimensions** - This is where we store *dataset-level* dimension records (CSV or JSON)
- **dataset.json5** - This is the dataset configuration file
- **dimension_mappings.json5** - *Dataset-level* dimension mappings configs for new dimension mappings that need to be registered

## Project Config

All details of a project are defined by its project configuration. Please refer to the [StandardScenarios project config](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/project.json5) for an example.

### Dimensions and Mappings

A project config contains dimensions and base-to-supplemental dimension mappings. The dimensions and mappings can be references to already-registered dimension/mapping IDs or unregistered configs.

The most primitive workflow is to register individual dimensions and mappings and then include those in the project config. That process is tedious and also prone to error. If you make a mistake with the records, you may not find out until later and then be forced to delete the configs and start over.

dsgrid provides a streamlined workflow where you specify the dimension and mapping configs inside the project config. Then, when you register the project, dsgrid will automatically register the dimensions and mappings and add the IDs to the project config. If anything fails, dsgrid will rollback all changes. Either the project and all dimensions and mappings will get registered or none of them will.

## Next Steps

- See the [Project Data Model](../../software_reference/data_models/project_model.md) for complete specifications
- Follow the [Create a Project Tutorial](../tutorials/project_creation.md) for hands-on practice
- Learn [how to create base dimensions](create_base_dimensions.md) and [supplemental dimensions](create_supplemental_dimensions.md)
