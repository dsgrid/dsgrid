Overview
=========

What is dsgrid?
---------------

dsgrid is a software package and modeling effort for gathering individual datasets
that describe different aspects of electricity (or other energy) demand 
at different levels of resolution and then assembling them into coherent, highly 
resolved descriptions of demand suitable for energy system modeling. See 
`https://www.nrel.gov/analysis/dsgrid.html <https://www.nrel.gov/analysis/dsgrid.html>` 
for more descriptive information, including past projects.


Key dsgrid terminology
----------------------

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

Dataset
~~~~~~~

A dsgrid dataset describes energy use or related quantities (e.g., population, 
stock of certain assets, delivered energy service, growth rates) broken down 
over a number of different dimensions, e.g., 

- scenario
- model_year
- geography
- time
- sector
- subsector
- end-use

Not all dimensions have to be significant, e.g., historical data will 
generally have a trivial (i.e., one-element) scenario dimension, and not all 
data sources provide information at the end-use level. 

Datasets are fully documented via the dimensions they use, as well as the 
dimension and dataset config files. Datasets are submitted to and stored by 
dsgrid in specific schemas that use the .parquet data format.

Dimension
~~~~~~~~~

Because dsgrid datasets tend to be highly multi-dimensional, it is important to 
clearly specify what dimensions data are defined over, make sure data for all 
expected dimension elements are present, and define how dimensions of the same 
type, but with different elements, map to each other.

Dimension Types
+++++++++++++++

dsgrid specifies the allowable dimension types. From previous work, the dsgrid 
team has found it important to clearly differentiate datasets and data based on:

- data source - What data product or energy model the data comes from
- scenario - What scenario is being described (if any)
- model_year - What historical or future year is being described
- weather_year - What year's weather patterns are being used
- geography - What geographic entity is being summarized
- time - Timestamps, indicator of annual data, or other descriptor of how within-year data is provided
- sector - Commerical, Residential, Industrial, Transportation
- subsector - Specific building types, industries, transportation modes, etc.
- metric (including energy differentiated by end-use) - Further breakdowns of energy use, energy service, population, etc.

Dimension Configs and Records
+++++++++++++++++++++++++++++

Specific instances of a dimension type are defined by a dimension config (metadata 
description provided in a .toml file) and, in most cases, a .csv of dimension 
records. Dimension records csvs have a header row and a row for each record, i.e., 
for each individual element in a given dimension, to which data points can be 
assigned. 

Dimension Mapping
~~~~~~~~~~~~~~~~~

While many data sources provide information by, e.g., scenario, geographic place, 
sector, and/or subsector, different data sources often define such dimensions 
differently and/or simply report out at a different level of resolution. Because 
dsgrid joins many datasets together to create a coherent description of energy 
for a specific place over a spectific timeframe, we need a mechanism for 
reconciling these differences. For example:

- How should census division data be downscaled to counties?
- What's the best mapping between EIA AEO commercial building types and NREL ComStock commercial building types?
- `Residential`, `res`, and `Res.` should all be interpreted the same way, as referring to residential energy use or housing stock, etc.

The mappings that answer these questions are explicitly registered with dsgrid 
as dimension mappings. This way they are clearly documented and usable in automated 
queries. Explicit, programmatically checked and used dimensions and dimension 
mappings are key features that help dsgrid efficiently and reliably assemble 
detailed datasets of energy demand from a combination of historical and modeled 
data.
