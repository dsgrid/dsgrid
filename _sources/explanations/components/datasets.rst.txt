*********
Datasets
*********
A dataset is defined by its :ref:`dataset-config`. This data structure describes the dataset's
attributes, dimensionality, and file format characteristics.

Types
=====
1. ``Benchmark``
2. ``Historical``
3. ``Modeled``

Dimensions
==========
Similar to projects, dataset configs can contain already-registered dimension IDs or unregistered
dimension configs. Refer to :ref:`dimensions-and-mappings` for background. Dataset contributors
will likely want to define new dimensions in the dataset config using the streamlined workflow.

Trivial Dimensions
==================
Not all dataset dimensions have to be significant, e.g., historical data will generally have a
trivial (i.e., one-element) scenario dimension. We call these one-element dimensions ``trivial
dimensions``. For space saving reasons, these trivial dimensions do not need to exist in the
dataset files, however, they must be declared as trivial dimensions in the dataset config.

File Format
===========
A dataset must comply with a supported dsgrid file format. Please refer to :ref:`dataset-formats`
for options.

Submit-to-Project
=================
A dataset must be submitted to a project before it can be used in dsgrid queries. The dsgrid
submission process verifies that the dataset's dimensions either meet the project requirements or
have valid mappings.

Examples
========
The `dsgrid-StandardScenarios repository
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/dsgrid_project/datasets>`_
contains datasets that you can use as examples.

Historical
----------
`EIA 861 Utility Customer Sales (MWh) by State by Sector by Year for 2010-2020
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/historical/eia_861_annual_energy_use_state_sector/dataset.json5>`_

Modeled
-------
`ResStock eulp_final
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/resstock/dataset.json5>`_

`AEO 2021 Reference Case Residential Energy End Use Annual Growth Factors
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/aeo2021_reference/residential/End_Use_Growth_Factors/dataset.json5>`_
