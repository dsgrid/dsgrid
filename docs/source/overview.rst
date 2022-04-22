Overview
=========

What is dsgrid?
---------------

dsgrid is a software package and modeling effort for gathering individual datasets
that describe different aspects of electricity (or other energy) demand
at different levels of resolution and then assembling them into coherent, highly
resolved descriptions of demand suitable for energy system modeling. See
`https://www.nrel.gov/analysis/dsgrid.html <https://www.nrel.gov/analysis/dsgrid.html>`_
for more descriptive information, including past projects.

.. _Key Terminology Overview:

Key dsgrid terminology
----------------------

.. _Project Overview:

Project
~~~~~~~

A dsgrid project is a collection of datasets and queries of those datasets
that describe energy demand for a specific region over a specific timeframe.
dsgrid projects are assembled at specific points in time by specific modeling
groups, to satisfy needs of specific data off-takers. For example, hourly
electricity demand for the contiguous United States, 2010-2050, developed using
EIA and other data from 2021, with derived datasets developed for capacity
expansion and production cost grid modelers using 2012 weather patterns (to
match the models' wind and solar data), and detail available on particularly
flexible/schedulable end-uses.

Simply put, the dsgrid project defines the requirements of expected datasets and queries.


.. _Dataset Overview:

Dataset
~~~~~~~

A dsgrid dataset describes energy use or related quantities (e.g., population,
stock of certain assets, delivered energy service, growth rates) broken down
over a number of different dimensions, (e.g., scenario, geography, time, etc.).
See :ref:`Dimension Types Overview` for a full list of required dataset dimensions.

Not all dataset dimensions have to be significant, e.g., historical data will
generally have a trivial (i.e., one-element) scenario dimension, and not all
data sources provide information at the end-use level. We call these one-element dimensions
``trivial dimensions``. For space saving reasons, these trivial dimensions do not need to exist
in the dataset .parquet files, however, they must be declared as trivial dimensions in the
:ref:`Dataset Config`.

Datasets are fully documented via the dimensions they use, as well as the
dimension and dataset config files. Datasets are submitted to and stored by
dsgrid in specific schemas that use the .parquet data format (See :ref:`dsgrid Data Structure`).

There are three types of dsgrid datasets:

1. ``Benchmark``
2. ``Historical``
3. ``Sector Model``

Datasets can be registered outside of a project, however, to be used by a project, they must be
submitted to the project and pass all project validations/meet all project expectations.




.. _Dimension Overview:

Dimension
~~~~~~~~~

dsgrid data (and projects) are multi-dimensional. Because of their multi-dimensionality, it is
important to clearly specify what dimensions data and projects are defined over, make sure data
for all expected dimension elements are present, and define how dimensions of the same type, but
with different elements, map to each other.


.. _Dimension Types Overview:

Dimension Types
+++++++++++++++

dsgrid specifies the allowable dimension types. From previous work, the dsgrid
team has found it important to clearly differentiate datasets and data based on:

- ``data source`` - What data product or energy model the data comes from
- ``scenario`` - What scenario is being described (if any)
- ``model_year`` - What historical or future year is being described
- ``weather_year`` - What year's weather patterns are being used; also analogous with calendar year
- ``geography`` - What geographic entity is being summarized
- ``time`` - Timestamps, indicator of annual data, or other descriptor of how within-year data
  is provided
- ``sector`` - Commerical, Residential, Industrial, Transportation
- ``subsector`` - Specific building types, industries, transportation modes, etc.
- ``metric`` - (including energy differentiated by end-use) - Further breakdowns of energy use,
  energy service, population, etc.
- ``end_use`` - Specific function one may apply energy to achieve, e.g., space cooling, water
  heating, fluid pumping


.. _Dimension Categories Overview:

Dimension Categories
++++++++++++++++++++
Dimensions are used in dsgrid to define expectations of the project, allowable dimension
transformations, and dimenison properties of underlying data sets. As such, dimensions are
defined both at the project and dataset levels. More specifically, there are three categories of
dimensions used in dsgrid.

- ``Base Dimension`` (defined by project): Base Dimensions are the core dimensions of a project.
  They define the project's dimension expectations. All datasets must adhere to the projet's
  base dimension definition or provide an appropriate :ref:`Dimension Mapping Overview` to the
  base dimension.
- ``Supplemental Dimension`` (defined by the project): These are supplmenetal dimensions defined
  by the project to support different queries and data aggregations or disaggregations.
- ``Dataset Dimension`` (defined by the dataset): These are dimensions defined by the dataset. If
  the dataset dimensions are different from the project's base dimensions, then a
  :ref:`Dimension Mapping Overview` is required upon submitting a dataset to a project.

Dimension Configs and Records
+++++++++++++++++++++++++++++

Specific instances of a dimension type are defined by a :ref:`Dimensions Config` (metadata
description provided in a .toml file) and, in most cases, a .csv of ``dimension
records``. Dimension records csvs have a header row and a row for each record, i.e.,
for each individual element in a given dimension, to which data points can be
assigned.


.. _Dimension Mapping Overview:

Dimension Mapping
~~~~~~~~~~~~~~~~~

While many data sources provide information by, e.g., scenario, geographic place,
sector, and/or subsector, different data sources often define such dimensions
differently and/or simply report out at a different level of resolution. Because
dsgrid joins many datasets together to create a coherent description of energy
for a specific place over a spectific timeframe, we need a mechanism for
reconciling these differences. For example:

- How should census division data be downscaled to counties?
- What's the best mapping between EIA AEO commercial building types and NREL ComStock commercial
  building types?
- `Residential`, `res`, and `Res.` should all be interpreted the same way, as referring to
  residential energy use or housing stock, etc.

The mappings that answer these questions are explicitly registered with dsgrid
as dimension mappings. This way they are clearly documented and usable in automated
queries. Explicit, programmatically checked and used dimensions and dimension
mappings are key features that help dsgrid efficiently and reliably assemble
detailed datasets of energy demand from a combination of historical and modeled
data.

dsgrid supports two different types of mappings:

1. ``Dataset-to-Project``: These are mappings from the dimensions defined by a dataset to
   the dimensions defined by a project that are of the same dimension type. They get declared when
   submitting a dataset to a project.
2. ``Base-to-Supplemental``: These are mappings from the project's base dimensions to its
   supplemental dimensions used for queries. These get defined when registering a project.

.. _Dimension Association Overview:

Dimension Association
~~~~~~~~~~~~~~~~~~~~~

Similar to dimension mappings, dimension associations help to define associations across
dimensions of different dimention types. For example:

- What end uses are associated with which sectors?
- What sectors and subsectors are associated with what data sources?

These dimension associations are defined at the project level and they help to provide clarity on
allowed dimension permutations for datasets.
