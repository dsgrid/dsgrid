# Create a Project

In this tutorial you will learn how to create a dsgrid project by following the example of [dsgrid-project-StandardScenarios](https://github.com/dsgrid/dsgrid-project-StandardScenarios).

Project attributes, dimensions, and base-to-supplemental dimension mappings must be defined in a project config as defined by [project config](../../software_reference/data_models/project_model). This tutorial will give step-by-step instructions on how to assign values in this file.

## Step 1: Create a Repository

Create a repository to store your configuration and record files. This project follows the directory structure recommendation from [project repository organization](../project_creation/concepts.md#project-repo-organization). We recommend that you use a source control management system like git.

## Step 2: Create the Project Configuration

Create a `project.json5` with these fields (but choose your own values):

```javascript
{
  project_id: "dsgrid_conus_2022",
  name: "dsgrid CONUS 2022",
  description: "Dataset created for the FY21 dsgrid Load Profile Tool for Grid Modeling project",
  datasets: [
  ],
  dimensions: {
    base_dimensions: [
    ],
    supplemental_dimensions: [
    ],
  },
  dimension_mappings: {
    base_to_supplemental: [
    ],
  },
}
```

## Step 3: Identify Datasets

Identify the datasets that will comprise the project. This project uses the ComStock, ResStock, and TEMPO datasets for the commercial, residential, and transportation sectors, respectively. The TEMPO dataset contains projected electricity load projects through 2050. ComStock and ResStock contain a single year and so the AEO 2021 Reference Case Commercial and Residential Energy End Use Annual Growth Factors datasets are used to make projections through 2050.

Fill in the basic fields for each dataset. Leave the dimension information out for now.

```javascript
datasets: [
  {
    dataset_id: 'comstock_reference_2022',
    dataset_type: 'modeled',
  },
  {
    dataset_id: 'aeo2021_reference_commercial_energy_use_growth_factors',
    dataset_type: 'modeled',
  },
  {
    dataset_id: 'resstock_conus_2022_reference',
    dataset_type: 'modeled',
  },
  {
    dataset_id: 'aeo2021_reference_residential_energy_use_growth_factors',
    dataset_type: 'modeled',
  },
  {
    dataset_id: 'tempo_conus_2022',
    dataset_type: 'modeled',
  },
],
```

## Step 4: Inspect Dataset Dimensionality

Inspect the datasets' dimensionality to determine the project's base dimension records.

:::{note}
**TODO**: Add detailed instructions for analyzing dataset dimensions to determine base dimension requirements.
:::

## Step 5: Add Supplemental Dimensions

Add supplemental dimensions based on expected user demand.

:::{note}
**TODO**: Add guidance on identifying and creating supplemental dimensions based on analysis requirements.
:::

## Step 6: Create Base-to-Supplemental Mappings

Create base-to-supplemental dimension mappings.

:::{note}
**TODO**: Add examples of base-to-supplemental dimension mapping configurations.
:::

## Step 7: Add Dataset Dimension Requirements

Go back through each dataset and add dimension requirements. ResStock is only expected to have residential-related dimension records, and so all others can be excluded.

```javascript
{
  dataset_id: 'resstock_conus_2022_reference',
  dataset_type: 'modeled',
  required_dimensions: {
    single_dimensional: {
      sector: {
        base: ['res'],
      },
      model_year: {
        base: ['2018'],
      },
      subsector: {
        supplemental: [
          {
            name: 'Subsectors by Sector Collapsed',
            record_ids: ['residential_subsectors'],
          },
        ],
      },
      metric: {
        supplemental: [
          {
            name: 'residential-end-uses-collapsed',
            record_ids: ['residential_end_uses'],
          },
        ],
      },
    },
  },
}
```

## Step 8: Register the Project

Register the project with the dsgrid registry:

```bash
dsgrid registry projects register --log-message "my log message" project.json5
```

## Next Steps

- Learn how to [create and submit a dataset](create_and_submit_dataset) to your project
- Understand [project concepts](../project_creation/concepts) in more detail
- Explore [querying project data](query_project)
