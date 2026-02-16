**********
Dimensions
**********

.. _dimension-types:

Dimension Types
===============
dsgrid specifies the allowable dimension types. From previous work, the dsgrid team has found it
important to clearly differentiate datasets and data based on:

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

Dimension Configs and Records
=============================
Specific instances of a dimension type are defined by a :ref:`dimension-config` and, in most cases,
a table of ``dimension records`` (usually a CSV file). Dimension records have a row for each
record, i.e., for each individual element in a given dimension, to which data points can be
assigned.

dsgrid time dimensions typically do not have records because the values can be generated
programmatically from parameters like starting and ending timestamps and frequency. Here are the
time formats supported by dsgrid:

- :ref:`date-time-dimension-config`
- :ref:`annual-time-dimension-config`
- :ref:`representative-period-time-dimension-config`
- :ref:`no-op-time-dimension-config`
