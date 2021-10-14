*********************
dsgrid Data Structure
*********************

Distrbuted Data Format
======================

A dsgrid project is a distributed data format, meaning that it is made up of one or more
independently registered datasets. For example, a dsgrid dataset may be made up of many sector
model datasets that are compiled together to represent a holistic dataset on energy demand across
multiple sectors. We call these input datasets and they are registered to dsgrid by sector modelers
(also called dsgrid data contributors). Each input dataset has its own set of dimension definitions
and its own parquet file paths (hence the “distributed data format”). When you query dsgrid for a
published dataset, you are really querying many datasets registered with a dsgrid project, e.g.,
``dsgrid registry project dsgrid_standard_scenarios_2021``.


Dataset Registration
====================

Dataset registration is required before it can be ingested into
``dsgrid``. Registration is facilitated by .toml file(s) as shown below.
Registration entries are stored on S3.

- ``dataset.toml``:

   - Configuration file that holds all other metadata details

- ``project.toml``:

   - Defines project requirements and any mapping required to map
     datasets to Base Dimensions.
   - For mapping options, select from ``no mapping``,
     ``association table``, or
     ``association table with a scaling factor``
   - If ``no mapping``, Base Dimensions must match dataset dimensions.
   - Submit ``association table`` as an input .toml file for
      ``dataset-submit``.

- ``dimension_mapping.toml``:

   - Defines ``association table`` to map dataset to Base Dimensions.


dsgrid Standard Format
======================
The standard format requires the user to supply all data tables in the format described below.

Data Tables
-----------

- ``load_data``: Contains load time series by the columns of one dimension.

  - The column dimension must be specified in the ``dataset.toml``. The example below uses the
    ``metric`` dimension.
  - The ``id`` column represents one unique time array.

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

- ``load_data_lookup``: Contains mappings from time array index to other dimensions.

::

    +---------+------+---------------+-------+
    |geography|sector|      subsector|     id|
    +---------+------+---------------+-------+
    |    53061|   com|FullServiceR...|      1|
    |    53053|   com|FullServiceR...|      2|
    |    53005|   com|FullServiceR...|      3|
    |    53025|   com|FullServiceR...|      4|
    |    53045|   com|FullServiceR...|      5|
    +---------+---+------------------+-------+

- ``Trivial dimensions``: 1-element dimensions that are not present in the data files. Dsgrid
  dynamically adds them to ``load_data_lookup`` as an alias column. These dimensions must be
  defined in the ``dataset.toml``.

- ``dataset_dimension_mapping`` (optional): defines the conversion mapping from base data file
  dimensions to dataset dimensions (e.g., ComStock locational multipliers)
- ``project_dimension_mapping`` (optional): defines the conversion mapping from dataset dimensions
  to Base Dimensions (is this a type of association table?) (e.g., mapping to convert from dataset
  spatial resolution to project spatial resolution)
- ``scaling_factor_table(s)`` (optional): store scaling factors for data disaggregation from one
  dimension to another

File Format
-----------

- Data tables must be stored in Parquet files.
- Recommended compression type is snappy.
- Recommended partition size is 128 MB per file.
- If there is no reasonable column to partition on then consider using the Spark bucket feature.
- Example ``load_data`` and ``load_data_lookup`` Parquet file structure:

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

Metadata option for scaling factors still valid?
------------------------------------------------

-  Stores sectoral scaling factors as single numbers and other scaling
   factors of similar nature
-  Can be looked up by xxx

Time Formats
============

DateTime
--------
Load data contains one or more ranges of time series data with a fixed frequency.
All time arrays within the load data must have identical ranges.

::

    # Hourly data for one year
    [01-01-2020 00:00:00, 01-01-2020 01:00:00, 01-01-2020 02:00:00, ... 12-31-2020 11:45:00]

Time zones
^^^^^^^^^^
Both time-zone-aware and time-zone-unaware timestamps should be converted to UTC when written to
the Parquet files.

We recommend that you use Spark to create the Parquet files, but that is not required.
If you do use Spark, note the following:

- Spark implicitly interprets timestamps in the time zone of the current SQL session and converts
  them to UTC when writing dataframes to Parquet.
- You can override the SQL session time zone programmatically or in your Spark configuration file.
  The setting is ``spark.sql.session.timeZone``.

Time zone aware timestamps
^^^^^^^^^^^^^^^^^^^^^^^^^^
``dsgrid`` can convert timestamps in data tables to the proper time zone looking up the time
dimension.

Time zone unaware timestamps
----------------------------
Time-zone-unaware timestamps that will be interpreted as local time should be written as UTC
timestamps (i.e., 12pm with no time zone should be written as 12pm UTC).

Annual
------
Load data contains one value per model year.

::

    [2020, 2021, 2022]

Representative Period
---------------------
Load data contains timestamps that represent multiple periods. dsgrid supports
the following formats:

one_week_per_month_by_hour
^^^^^^^^^^^^^^^^^^^^^^^^^^
Each time array contains one week of hourly data (24 hours per day) that
applies to an entire month. The times represent local time (no time zone).
There are no shifts, missing hours, or extra hours for daylight savings time.

- All time columns must be integers.
- `month` is one-based, starting in January. ``Jan`` -> 1, ``Feb`` -> 2, etc.
- `day_of_week` is zero-based, starting on Monday. ``Mon`` -> 0, ``Tue`` -> 1, etc.
- `hour` is zero-based, starting at midnight.

::

    +---+-----+-----------+----+--------+
    | id|month|day_of_week|hour|dim_col1|
    +---+-----+-----------+----+--------+
    |  1|    4|          0|   0|     1.0|
    |  1|    4|          0|   1|     1.0|
    |  1|    4|          0|   2|     1.0|
    |  1|    4|          0|   3|     1.0|
    |  1|    4|          0|   4|     1.0|
    |  1|    4|          0|   5|     1.0|
    |  1|    4|          0|   6|     1.0|
    |  1|    4|          0|   7|     1.0|
    |  1|    4|          0|   8|     1.0|
    |  1|    4|          0|   9|     1.0|
    |  1|    4|          0|  10|     1.0|
    |  1|    4|          0|  11|     1.0|
    |  1|    4|          0|  12|     1.0|
    |  1|    4|          0|  13|     1.0|
    |  1|    4|          0|  14|     1.0|
    |  1|    4|          0|  15|     1.0|
    |  1|    4|          0|  16|     1.0|
    |  1|    4|          0|  17|     1.0|
    |  1|    4|          0|  18|     1.0|
    |  1|    4|          0|  19|     1.0|
    |  1|    4|          0|  20|     1.0|
    |  1|    4|          0|  21|     1.0|
    |  1|    4|          0|  22|     1.0|
    |  1|    4|          0|  23|     1.0|
    |  1|    4|          1|   0|     1.0|
    +---+-----+-----------+----+--------+

dsgrid can add support for other period formats. Please submit requests as
needed.
