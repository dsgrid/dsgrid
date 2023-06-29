********
Projects
********
A dsgrid project is a distributed dataset, meaning that it is made up of one or more independently
registered datasets. For example, a dsgrid dataset may be made up of many sector model
datasets that are compiled together to represent a holistic dataset on energy demand across
multiple sectors. We call these input datasets and they are registered to dsgrid by sector modelers
(also called dsgrid data contributors). Each input dataset has its own set of dimension definitions
and its own parquet file paths (hence the "distributed dataset"). When you query dsgrid for a
published dataset, you are really querying many datasets registered with a dsgrid project, e.g.,
``dsgrid_standard_scenarios_2021``.

Dimension Categories
====================
Dimensions are used in dsgrid to define expectations of the project, allowable dimension
transformations, and dimenison properties of underlying data sets. As such, dimensions are
defined both at the project and dataset levels. More specifically, there are three categories of
dimensions used in dsgrid.

- ``Base Dimension``: Base Dimensions are the core dimensions of a project. They define the
  project's dimension expectations. All datasets must adhere to the projet's base dimension
  definition or provide an appropriate :ref:`dimension_mapping_overview` to the base dimension.
- ``Supplemental Dimension``: These are supplemental dimensions defined by the project to support
  different queries and data aggregations or disaggregations.

Dimension Requirements
======================
dsgrid projects define define per-dataset requirements that specify what records must be present
for each dimension. This allows datasets to exclude non-applicable dimensions. For example, a
residential dataset will not have transportation energy use data.

Project Repository
==================
Every dsgrid project needs a github repository to store all configs and misc. scripts and to
collaborate on project decisions. An example dsgrid project repository is the
`dsgrid-project-StandardScenarios <https://github.com/dsgrid/dsgrid-project-StandardScenarios>`_
repo.

.. _project-repo-organization:

Project Repo Organization
-------------------------
We recommend that dsgrid project repositories use the following directory organization structure:

    .. code-block::

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

In the directory structure above, all project files are stored in the ``dsgrid_project`` root
directory. And within this root directory we have:

    * ``datasets``: This is where we define input datasets, oranized first by type (i.e.,
      ``datasets/historical``, ``datasets/benchmark``, ``datasets/modeled``) and then by source
      (e.g., ``datasets/modeled/comstock``) for the dsgrid project
    * ``dimension_mappings``: This where we store :under:`project-level` dimension mapping records
      (csv or json)
    * ``dimensons``: This is where we store :under:`project-level` dimension records (csv or json)
    * ``project.json5``: This is the main project configuration file. See ::ref

    Within each `datasets/{type}/{source}` sub-folder are the following files:

        * ``dimension_mappings``:  This where we store :under:`dataset-level` dimension mapping
          records (csv or json)
        * ``dimensions``: This is where we store :under:`dataset-level` dimension records (csv or
          json)
        * ``dataset.json5``: This is the dataset configuration file
        * ``dimension_mappings.json5``: :under:`Dataset-level`  dimension mappings configs for new
          dimension mappings that need to be registered

Project Config
==============
All details of a project are defined by its :ref:`project-config`. Please refer to the
`StandardScenarios project config
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/project.json5>`_
for an example.

.. _dimensions-and-mappings:

Dimensions and Mappings
-----------------------
A project config contains dimensions and base-to-supplemental dimension mappings. The dimensions
and mappings can be references to already-registered dimension/mapping IDs or unregistered
configs.

The most primitive workflow is to register individual dimensions and mappings and then include
those in the project config. That process is tedious and also prone to error. If you make a mistake
with the records, you may not find out until later and then be forced to delete the configs and
start over.

dsgrid provides a streamlined workflow where you specify the dimension and mapping configs inside
the project config. Then, when you register the project, dsgrid will automatically register the
dimensions and mappings and add the IDs to the project config. If anything fails, dsgrid will
rollback all changes. Either the project and all dimensions and mappings will get registered or
none of them will.

An unregistered dimension config must comply with :ref:`dimension-config`. A registered dimension
reference must comply with :ref:`dimension-reference`.

An unregistered dimension mapping config must comply with :ref:`unregistered-dimension-mappings`. A
registered dimension mapping reference must comply with :ref:`dimension-mapping-reference`.
