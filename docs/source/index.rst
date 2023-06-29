####################
dsgrid documentation
####################

dsgrid is a Python API for contributing to and accessing demand-side
grid model (dsgrid) datasets.

⚠️ **dsgrid is under active development and does not yet have a formal
package release.** ⚠️

Details listed here are subject to change. Please reach out to the
dsgrid coordination team with any questions or other feedback.

What is dsgrid?
===============
dsgrid is a software package and modeling effort for gathering individual datasets
that describe different aspects of energy demand
at different levels of resolution and then assembling them into coherent, highly
resolved descriptions of demand suitable for energy system modeling. See
`https://www.nrel.gov/analysis/dsgrid.html <https://www.nrel.gov/analysis/dsgrid.html>`_
for more descriptive information, including past projects.

Projects
========
A dsgrid project is a collection of datasets and queries of those datasets
that describe energy demand for a specific region over a specific timeframe.
dsgrid projects are assembled at specific points in time by specific modeling
groups, to satisfy needs of specific data off-takers. For example, hourly
electricity demand for the contiguous United States, 2010-2050, developed using
EIA and other data from 2021, with derived datasets developed for capacity
expansion and production cost grid modelers using 2012 weather patterns (to
match the models' wind and solar data), and detail available on particularly
flexible/schedulable end-uses.

Simply put, the dsgrid project defines the requirements of expected datasets.

Registration
------------
TODO: describe the project coordinator workflow

TODO: describe the dataset registration workflow

.. _dataset_overview:

Datasets
========
A dsgrid dataset describes energy use or related quantities (e.g., population,
stock of certain assets, delivered energy service, growth rates) broken down
over a number of different dimensions, (e.g., scenario, geography, time, etc.).
See :ref:`dimension-types` for a full list of required dataset dimensions.

A dataset can have different dimensions than the project. If they are different from the project's
base dimensions then a :ref:`dimension_mapping_overview` is required.

Once a dataset is submitted to a project dsgrid can run a mapping operation to transform
the dataset into the project's dimensions. Multiple datasets can thus be concatentated together to
form derived datasets.

Dimensions
==========
dsgrid data (and projects) are multi-dimensional. Because of their multi-dimensionality, it is
important to clearly specify what dimensions data and projects are defined over, make sure data
for all expected dimension elements are present, and define how dimensions of the same type, but
with different elements, map to each other.


.. _dimension_mapping_overview:

Dimension Mappings
==================
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

Queries
=======
TODO

Current development status
==========================

-  Registering (standalone) datasets - *Functional,*
-  Submitting datasets to projects - *Functional*
-  Basic queries - *Functional*
-  dsgrid project-specific key derived and cached datasets -
   *In progress*
-  dsgrid publishing process - *Forthcoming*


.. toctree::
   :maxdepth: 4
   :caption: Contents:

   how_tos/index
   tutorials/index
   explanation/index
   reference/index
   spark_overview


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
