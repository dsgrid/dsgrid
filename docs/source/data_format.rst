Dsgrid Data Structure
=====================

Distrbuted Data Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A dsgrid project is a distributed data format, meaning that it is made up of one or more independently registered datasets. For example, a dsgrid dataset may be made up of many sector model datasets that are compiled together to represent a holistic dataset on energy demand across multiple sectors. We call these input datasets and they are registered to dsgrid by sector modelers (also called dsgrid data contributors). Each input dataset has its own set of dimension definitions and its own parquet file paths (hence the “distributed data format”). When you query dsgrid for a published dataset, you are really querying many datasets registered with a dsgrid project, e.g., 
``dsgrid registry project dsgrid_standard_scenarios_2021``.


Dataset Registration
~~~~~~~~~~~~~~~~~~~~

Dataset registration is required before it can be ingested into
``dsgrid``. Registration is facilitated by .toml file(s) as shown below.
Registration entries are stored on S3.

-  ``dataset.toml``:

   -  configuration file that holds all other metadata details

-  ``project.toml``:

   -  defines project requirements and any mapping required to map
      datasets to Base Dimensions
   -  For mapping options, select from ``no mapping``,
      ``association table``, or
      ``association table with a scaling factor``
   -  If ``no mapping``, Base Dimensions must match dataset dimensions.
   -  Submit ``association table`` as an input .toml file for
      ``dataset-submit``.

-  ``dimension_mapping.toml``:

   -  defines ``association table`` to map dataset to Base Dimensions

Data Tables
~~~~~~~~~~~

-  ``load_data``: contains load time series by end use columns indexed
   by a dataframe index, as shown below:

::

   +-------------------+----+-------------------+--------------------+--------------------+
   |          timestamp|  id|            heating|             cooling|  interior_equipment|
   +-------------------+----+-------------------+--------------------+--------------------+
   |2012-01-01 00:00:00|9106| 0.2143171631469727|0.001987764734408426|0.051049410357755676|
   |2012-01-01 01:00:00|9106| 0.3290653818000351|9.035294172606012E-5|0.051049410357755676|
   |2012-01-01 02:00:00|9106|0.36927244565896444|                 0.0| 0.06622870555383997|
   |2012-01-01 03:00:00|9106| 0.3869816717726735|                 0.0| 0.06622870555383997|
   |2012-01-01 04:00:00|9106| 0.3872526947775441|                 0.0| 0.06622870555383997|
   +-------------------+----+-------------------+--------------------+--------------------+

-  ``load_data_lookup``: contains the mapping from data index to
   subsector and geographic indices, as shown below:

::

   +---------+--------------------+-----------------+-------+
   |geography|           subsector|     scale_factor|   id  |
   +---------+--------------------+-----------------+-------+
   |    53061|com__FullServiceR...|9.035293579101562|      1|
   |    53053|com__FullServiceR...|9.035293579101562|      2|
   |    53005|com__FullServiceR...|9.035293579101562|      3|
   |    53025|com__FullServiceR...|9.035293579101562|      4|
   |    53045|com__FullServiceR...|9.035293579101562|      5|
   +---------+--------------------+-----------------+-------+

-  ``dataset_dimension_mapping`` (optional): defines the conversion
   mapping from base data file dimensions to dataset dimensions (e.g.,
   ComStock locational multipliers)
-  ``Project_dimension_mapping`` (optional): defines the conversion
   mapping from dataset dimensions to Base Dimensions (is this a type of
   association table?) (e.g., mapping to convert from dataset spatial
   resolution to project spatial resolution)
-  ``scaling_factor_table(s)`` (optional): store scaling factors for
   data disaggregation from one dimension to another

Data Partitioning
~~~~~~~~~~~~~~~~~

-  Data tables are stored as partitioned snappy parquet files
-  Default partitioning is xxx MB/file before compression
-  Example ``load_data`` and ``load_data_lookup`` parquet files
   structure:

::

   .
   └── nrel-dsgrid-scratch
       └── dsgrid_v2.0.0
           └── commercial
               ├── apply_scale_factor.log
               ├── convert_dsg.log
               ├── dimensions.json
               ├── enduse.csv
               ├── geography.csv
               ├── load_data.parquet
               │   ├── _SUCCESS
               │   ├── part-00000-2c65bf32-8873-4936-a9ba-946a2c32c2d9-c000.snappy.parquet
               │   ├── part-00000-e7b9b687-e2e8-4f7d-a196-02cd97e7bb87-c000.snappy.parquet
               │   ├── part-00001-2c65bf32-8873-4936-a9ba-946a2c32c2d9-c000.snappy.parquet
               │   ├── part-00001-e7b9b687-e2e8-4f7d-a196-02cd97e7bb87-c000.snappy.parquet
               │   ├── ...     
               ├── load_data_lookup.parquet
               │   ├── _SUCCESS
               │   ├── part-00000-7c563524-3af3-46be-8dec-0af3c6a28dbb-c000.snappy.parquet
               │   └── part-00000-ed457571-2c66-4fcd-89fa-da7119da1645-c000.snappy.parquet
               ├── sector.csv
               ├── subsector.csv
               └── time.csv

Data Binning
~~~~~~~~~~~~

Metadata option for scaling factors still valid?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Stores sectoral scaling factors as single numbers and other scaling
   factors of similar nature
-  Can be looked up by xxx

Time zones
~~~~~~~~~~
Both timezone-aware and timezone-unaware timestamps should be converted to UTC when written to the Parquet format. Spark implicitly interprets timestamps in the timezone of the current SQL session and converts them to UTC when writing dataframes to Parquet.

This behavior is straightforward with timezone-aware timestamps. ``dsgrid`` can interpret the proper time by looking up the time dimension. Timezone-unaware timestamps that will be interpreted as local time should be written as UTC timestamps (i.e., 12pm with no timezone should be written as 12pm UTC).

