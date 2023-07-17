####################
dsgrid documentation
####################

What is dsgrid?
===============

The dsgrid Python package is the central tool for creating, managing, contributing to, and 
accessing demand-side grid (dsgrid) toolkit projects. The dsgrid toolkit enables the compilation 
of high-resolution load datasets suitable for forward-looking power system and other analyses. 
For more information and completed work products, please see 
`https://www.nrel.gov/analysis/dsgrid.html <https://www.nrel.gov/analysis/dsgrid.html>`_.

Documentation Overview
======================

If you are new to dsgrid, you'll likely want to start by reading the rest of this page, reading 
the how-to guide on installation, and then choosing a tutorial that corresponds to how you expect 
to be using dsgrid in the near future. 

For general use, the documentation is organized into:

- :ref:`tutorials`, which provide step-by-step instructions (via examples) for common high-level 
  tasks;
- :ref:`how-to-guides`, which provide quick reminder recipes for key workflows;
- :ref:`explanations`, which describe concepts and answer "why" questions to facilitate deeper 
  understanding; and
- :ref:`reference`, which provides complete information on various interfaces (e.g., command line, 
  data formats, data models, public Python API).

Please note that for now:

⚠️ **dsgrid is under active development and does not yet have a formal package release.** ⚠️

and **details listed here are subject to change**. Please reach out to the dsgrid coordination 
team with any questions or other feedback.

dsgrid Overview
===============

dsgrid is a tool for collecting and aligning datasets containing timeseries information describing 
future energy use, especially electricity load, to be used in planning studies. `Datasets`_ are 
defined over specific scenario, model year, weather year, geography, time, sector, subsector, and 
metric `Dimensions`_ and can range in size from less than 1 megabyte to over 1 terabyte. Typically, 
datasets are organized into `Projects`_ with specific base dimensions with the help of 
`Dimension Mappings`_. Projects use `Queries`_ to consolidate information into 
`Derived Datasets`_ that, together with the standalone datasets, eventually enable a comprehensive 
description of the electricity load or other energy use being modeled. Projects can also be 
queried to produce output data for ingestion into another model or for direct analysis.

The people who interact with dsgrid are typically:

- `Project Coordinators`_, who construct, analyze and publish projects;
- `Dataset Contributors`_, who register datasets with dsgrid and and submit them to projects; or
- `Data Users`_, who access already-queried data or write and run their own custom queries.

Because the dsgrid data can be quite large (on the order of terabytes), and are compiled from a 
variety of data sources, dsgrid uses two key technologies to facilitate its workflows:

- A graph database (currently ArangoDB) to hold dsgrid registries (metadata on and relationships 
  between dsgrid components, e.g., dimensions, datasets, dimension mappings, projects, derived 
  datasets, queries)
- A big-data engine (currently Apache Spark) to perform database operations across a cluster of 
  computational nodes

Both of these technologies mean that some care must be taken when choosing and setting up a 
computational environment for a particular task. The dsgrid coordination team currently supports 
two computational environments:

- `Standalone`_, single-node environments e.g., personal laptops or a single server or virtual 
  machine, which is suitable for:
    
    - Small-scale development and testing
    - Submitting a single dataset to a project in offline mode
    - Very small dsgrid projects (no large datasets and little to no downscaling)

  Note that use on standalone Windows machines is especially limited.
- `NREL High Performance Computing`_, where users can:

    - Directly work on projects through the shared registry
    - Develop code and test data and workflows using their own registry

   In either case, users will need to launch Apache Spark clusters with sufficient computational, 
   memory, and disk resources.

Components
----------

Dimensions
^^^^^^^^^^
dsgrid datasets and projects are multi-dimensional, and some dimensions are defined over thousands 
of elements (e.g., counties in the United States, hours in a year). It is also typical for 
different datasets that nominally describe the same thing to use different labels (e.g., 
`ResStock building types 
<https://github.com/NREL/resstock/blob/euss.2022.1/project_national/housing_characteristics/Geometry%20Building%20Type%20RECS.tsv>`_ 
and `EIA Residential Energy Consumption Survey (RECS) building types 
<https://www.eia.gov/consumption/residential/data/2020/hc/pdf/HC%201.1.pdf>`_). 

To manage this complexity while allowing different analysts and modeling teams to use their own 
labels (to facilitate transparency, easy maintenance and debugging), dsgrid requires its users to 
explicitly define each data dimension by specifying its :ref:`dimension type <dimension-types>`, 
metadata, and a table listing each dimension record's id and name. dsgrid uses this information 
to ensure that submitted data is as-expected.

.. _dataset_overview:

Datasets
^^^^^^^^
A dsgrid dataset describes energy use or another metric (e.g., population, stock of certain 
assets, delivered energy service, growth rates) resolved over a number of different dimensions, 
e.g., scenario, geography, time, etc. When registering a dataset, the data submitter must define 
a dimension for each dsgrid :ref:`dimension type <dimension-types>`, but that does not mean that 
datasets are required to be resolved (i.e., have multiple entries) for each dimension type--any
dimension can be "trivial", in which case it is defined by a single record (e.g., 'unspecified' 
subsector, or a single '2012' weather year) and is not included in the data files.

Registering a dataset requires a dataset config file, which lists dataset and dimension metadata, 
the actual data file(s), and dimension record files (csvs) for any dimensions not already in the 
dsgrid registry. The data files must conform to one of the dsgrid :ref:`dataset-formats`, 
currently either the :ref:`one-table-format` or the :ref:`two-table-format`. Upon registration, 
dsgrid checks the data files, which contain dimension records and numerical values, for 
consistency with the specified dimensions. Inconsistent data fails registration to prevent 
compounding downstream errors.

Projects
^^^^^^^^
A dsgrid project is a collection of datasets that describe energy demand for a specific region 
over a specific timeframe. Because datasets describing different sectors' energy use are defined 
in different ways, the key task of a dsgrid project is to enable and perform mappings from 
datasets' dimensions into the **project base dimensions**. Project base dimensions are defined
in the same way as dataset dimensions (i.e., there is a project base dimension for each dimension 
type), however, they are used differently. Whereas dataset dimensions are *descriptive*--they 
describe what you will find if you look in a dataset's data files, project base dimensions are 
*prescriptive*--they define what dataset submitters must map their dimensions into. dsgrid projects
are also highly prescriptive about what datasets they expect to be submitted, and what data 
dimensions they are expecting each dataset to provide (post-mapping, see 
:ref:`dimension_mapping_overview`). dsgrid uses all of the prescribed information to check that 
submitted datasets are as expected, and throws errors if they are not.

dsgrid projects are also the starting point for `Queries`_. Queries are the process whereby 
datasets are actually mapped into the project base dimensions, concatenated, and further 
transformed. The two straightforward applications of queries are:

1. Output data suitable for use in another model, and 
2. Analyze project data.

Either way, users generally do not want full detail along all dimensions. dsgrid supports this 
by letting users specify what *supplemental dimensions* they want their query results in, as well 
as if they want any dimension records filtered out and what mathematical operations they want to
use for aggregations. For example, a query can be written to filter out non-electricity energy 
use; sum electricity use over all sectors, subsectors, and end-uses; and map to a specific 
power system model's geography to create load data for capacity expansion or production cost 
modeling.

.. _dimension_mapping_overview:

Dimension Mappings
^^^^^^^^^^^^^^^^^^
While many data sources provide information by, e.g., scenario, geographic place, and sector, 
different data sources often define such dimensions differently and/or simply report out at a 
different level of resolution. Because dsgrid joins many datasets together to create a coherent 
description of energy for a specific place over a spectific timeframe, we need a mechanism for
reconciling these differences. For example:

- How should census division data be downscaled to counties?
- What's the best mapping between EIA AEO commercial building types and NREL ComStock commercial
  building types?
- `Residential`, `res`, and `Res.` should all be interpreted the same way, as referring to
  residential energy use or housing stock, etc.

The mappings that answer these questions are explicitly registered with dsgrid as dimension 
mappings. This way they are clearly documented and usable in automated queries. Explicit, 
programmatically checked and used dimensions and dimension mappings are key features that help 
dsgrid efficiently and reliably assemble detailed datasets of energy demand from a combination of 
historical and modeled data.

Currently, dsgrid supports two different types of mappings:

1. ``Dataset-to-Project``: These are mappings from a dataset's dimension to a project's base 
   dimension of the same dimension type. They are declared and registered when a dataset is 
   submitted to a project.
2. ``Base-to-Supplemental``: These are mappings from a project's base dimensions to its
   supplemental dimensions, which are the alternate data resolutions available for use in queries. 
   Base-to-supplemental dimensions are defined when registering a project.

Queries
^^^^^^^
TODO

Derived Datasets
^^^^^^^^^^^^^^^^
During the creation of a dsgrid project, one of the key tasks is to use queries to create 
derived datasets. As their name implies, derived datasets are dsgrid datasets and must 
meet all requirements that status implies, but they are created by combining multiple datasets 
already in the project. Because they are formed from datasets already mapped to the project base 
dimensions, defining the dimensions of derived datasets is typically straightforward and is either 
fully or mostly automated by dsgrid. Derived datasets are the mechanism dsgrid provides to do 
things like apply growth rates or calculate residuals. Registering derived datasets with the 
project enables dsgrid modelers to bootstrap data into providing a complete and straightforward 
accounting of a region's energy demand over a specified timeframe.

Published Projects
^^^^^^^^^^^^^^^^^^
TODO

Tasks
-----

Project Coordinators
^^^^^^^^^^^^^^^^^^^^
dsgrid project coordinators :ref:`create projects <tutorial_create_a_project>`, collaborate with 
dataset contributors to get datasets added to the project, :ref:`create derived datasets 
<tutorial_create_a_derived_dataset>`, :ref:`write queries <tutorial_query_a_project>`, analyze and
publish data. 

Dataset Contributors
^^^^^^^^^^^^^^^^^^^^
The role of dataset contributors is primarily to :ref:`create, register, and submit datasets 
<tutorial_create_and_submit_a_dataset>`. Of course, dataset contributors might also be project 
coordinators and/or data users.

Data Users
^^^^^^^^^^
Data users might access data provided by a project coordinator through formal publication or other
dissemination channels, or they might :ref:`write their own queries <tutorial_query_a_project>`.

Computational Environment
-------------------------

dsgrid is cross-platform software that can be used with any datasets that conform to the metadata 
and data formatting requirements. However, the typical dsgrid project involves large, simulated 
datasets that are "exploded" to align across multiple dimension types and to produce the variety 
of different views needed by different data users and analysts. Thus while some development, 
testing, and work with single datasets or very small projects can be performed on `Standalone`_ 
machines, currently a lot of dsgrid work requires `NREL High Performance Computing`_.

Standalone
^^^^^^^^^^

TODO: List the key set-up tasks (spin up and connect to ArangoDB, spin up and connect to 
Apache Spark cluster, and generally configure dsgrid to be pointing to all the right places) 
and link to the appropriate how-tos

NREL High Performance Computing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO: List the key set-up tasks (spin up and connect to ArangoDB, spin up and connect to 
Apache Spark cluster, and generally configure dsgrid to be pointing to all the right places) 
and link to the appropriate how-tos

Indices, Tables, and Contents
=============================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. TODO: Search page seems to be where search results land, but if you just click on it 
   it's blank. Can the default page have a search bar on it or some sort of instruction?

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tutorials/index
   how_tos/index
   explanations/index
   reference/index
   spark_overview
