*****************
Create a Project
*****************
In this tutorial you will learn how to create a dsgrid project by following the example of
`dsgrid-project-StandardScenarios <https://github.com/dsgrid/dsgrid-project-StandardScenarios>`_.

Project attributes, dimensions, and base-to-supplemental dimension mappings must be defined in a
project config as defined by :ref:`project-config`. This tutorial will give step-by-step
instructions on how to assign values in this file.

1. Create a repository to store your configuration and record files. This project project follows
   the directory structure recommendation from :ref:`project-repo-organization`. We recommend that
   you use a source control management system like git.

2. Create a ``project.json5`` with these fields (but choose your own values):

.. code-block:: JavaScript

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

3. Identify the datasets that will comprise the project. This project uses the ComStock, ResStock,
   and TEMPO datasets for the commercial, residential, and transportation sectors, respectively.
   The TEMPO dataset contains projected electricity load projects through 2050. ComStock and
   ResStock contain a single year and so the AEO 2021 Reference Case Commercial and Residential
   Energy End Use Annual Growth Factors datasets are used to make projections through 2050.

   TODO: other datasets

   Fill in the basic fields for each dataset. Leave the dimension informat out for now.

.. code-block:: JavaScript

    datasets: [
      {
        dataset_id: 'comstock_reference_2022',
        dataset_type: 'modeled',
      },
      {
        dataset_id: 'aeo2021_reference_comidential_energy_use_growth_factors',
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

4. Inspect the datasets' dimensionality to determine the project's base dimension records. TODO

5. Add supplemental dimensions based on expected user demand. TODO

6. Create base-to-supplemental dimension mappings. TODO

7. Go back through each dataset and add dimension requirements. ResStock is only expected to have
   residential-related dimension records, and so all others can be excluded.

.. code-block:: JavaScript

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

8. Register the project.

.. code-block:: console

    $ dsgrid registry projects register --log-message "my log message" project.json5
