.. _dataset-formats:

****************
Dataset Formats
****************

dsgrid aims to support all dataset formats that users need for efficient queries and analysis. If
you need a new format, please contact the dsgrid team to discuss it.

Requirements
=============
1. Metric data should usually be stored in Parquet files. CSV files are also supported. If you
   need or want another optimized columnar file format, please contact the dsgrid team.
2. If the data tables contain time-series data, each unique time array must contain an identical
   range of timestamps.
3. Values of dimension columns except ``model_year`` and ``weather_year`` must be strings.
   ``model_year`` and ``weather_year`` can be integers.
4. Each dimension column name except time must match dsgrid dimension types (geography, sector,
   subsector, etc.) either directly or by specifying the mapping.
5. The values in each dimension column must match the dataset's dimension records.

Recommendations
===============
1. Enable compression in all Parquet files. ``Snappy`` is preferred.
2. The recommended size of individual Parquet files is 128 MiB. Making the files too big can cause
   memory issues. Making them too small adds overhead and hurts performance.
3. Trivial dimensions (one-element records) should not be stored in the data files. They should
   instead be defined in the dataset config. dsgrid will add them dynamically at runtime.
4. Floating point data can be 64-bit or 32-bit. 64-bit floats provide more precision but require twice as much storage space as 32-bit floats.


CSV Files
=========
While not generally recommended, dsgrid does support CSV data files. By default,
dsgrid will let Spark and DuckDB attempt to infer the schema of the file. Because there may be
cases of type ambiguities, such as integer vs string, integer vs float, and timestamps with time
zones, dsgrid provides a mechanism for defining column data types directly in the dataset
configuration.

Consider this example that uses county FIPS codes to identify the geography of each data point::

    +----------------------+---------+------------------+--------------------+-------+
    |             timestamp|geography|          scenario|           subsector|  value|
    +----------------------+---------+------------------+--------------------+-------|
    |2011-12-31 22:00:00-07|    01001|      efs_high_ldv|full_service_rest...| 1.234 |
    |2011-12-31 22:00:00-07|    01001|      efs_high_ldv|      primary_school| 2.345 |
    +----------------------+---------+------------------+--------------------+-------|

The default behavior of IO libraries like Pandas, Spark, and DuckDB is to infer data types by
inspecting the data. They will all decide that the geography column contains integers and drop
the leading zeros. This will result in an invalid geography column, which is required to be a
string data type, and will not match the project's geography dimension (assuming that the project is
also defined over county FIPS codes).

Secondly, you may want to specify the minimum required size for each number. For example,
if you don't need the precision that comes with 8-byte floats, choose ``FLOAT`` and
Spark/DuckDB will store all values in 4-byte floats, halving the required storage size.

.. _csv-data-types:

Specifying Column Data Types
----------------------------
To specify column data types, add a ``columns`` field to the ``data_file`` (or ``lookup_data_file``)
section in your dataset configuration. You can specify types for all columns or just a subset -
columns without explicit types will have their types inferred.

The supported data types are (case-insensitive):

    - BOOLEAN: boolean
    - INT: 4-byte integer
    - INTEGER: 4-byte integer
    - TINYINT: 1-byte integer
    - SMALLINT: 2-byte integer
    - BIGINT: 8-byte-integer
    - FLOAT: 4-byte float
    - DOUBLE: 8-byte float
    - STRING: string
    - TEXT: string
    - VARCHAR: string
    - TIMESTAMP_TZ: timestamp with time zone
    - TIMESTAMP_NTZ: timestamp without time zone

Example dataset configuration with column data types:

.. code-block:: JavaScript

    data_layout: {
      table_format: "one_table",
      value_format: {
        format_type: "stacked",
      },
      data_file: {
        path: "load_data.csv",
        columns: [
          {
            name: "timestamp",
            data_type: "TIMESTAMP_TZ",
          },
          {
            name: "geography",
            data_type: "STRING",
          },
          {
            name: "scenario",
            data_type: "STRING",
          },
          {
            name: "subsector",
            data_type: "STRING",
          },
          {
            name: "value",
            data_type: "FLOAT",
          },
        ],
      },
    }

You can also specify types for only the columns that need explicit typing:

.. code-block:: JavaScript

    data_file: {
      path: "load_data.csv",
      columns: [
        {
          name: "geography",
          data_type: "STRING",  // Prevent FIPS codes from being read as integers
        },
      ],
    }


Custom Column Names
===================
By default, dsgrid expects data files to have columns named after the standard dimension types
(``geography``, ``sector``, ``subsector``, ``metric``, etc.). Data files with value format "stacked" are also expected to have a `value` column. However, your data files may use
different column names. dsgrid provides a mechanism to map custom column names to the expected
dimension types.

To rename columns, add the ``dimension_type`` field to the column definition. This tells dsgrid
what dimension the column represents, and dsgrid will automatically rename it at runtime.

This feature works for all file formats (Parquet, CSV), not just CSV files.

Example with custom column names:

.. code-block:: JavaScript

    data_file: {
      path: "load_data.parquet",
      columns: [
        {
          name: "county",           // Actual column name in the file
          dimension_type: "geography",  // Will be renamed to "geography"
        },
        {
          name: "end_use",          // Actual column name in the file
          dimension_type: "metric",     // Will be renamed to "metric"
        },
        {
          name: "building_type",    // Actual column name in the file
          dimension_type: "subsector",  // Will be renamed to "subsector"
        },
      ],
    }

You can combine ``dimension_type`` with ``data_type`` when using CSV files:

.. code-block:: JavaScript

    data_file: {
      path: "load_data.csv",
      columns: [
        {
          name: "fips_code",
          data_type: "STRING",
          dimension_type: "geography",
        },
        {
          name: "value",
          data_type: "DOUBLE",
        },
      ],
    }


Ignoring Columns
================
Data files may contain columns that are not needed for dsgrid processing. Rather than
modifying your source files, you can tell dsgrid to ignore (drop) specific columns when
reading the data.

To ignore columns, add an ``ignore_columns`` field to the ``data_file`` (or ``lookup_data_file``)
section in your dataset configuration. This field accepts a list of column names to drop.

This feature works for all file formats (Parquet, CSV).

Example with ignored columns:

.. code-block:: JavaScript

    data_file: {
      path: "load_data.parquet",
      ignore_columns: ["internal_id", "notes"],
    }

You can combine ``ignore_columns`` with ``columns`` for type overrides and renaming:

.. code-block:: JavaScript

    data_file: {
      path: "load_data.csv",
      columns: [
        {
          name: "county",
          data_type: "STRING",
          dimension_type: "geography",
        },
        {
          name: "value",
          data_type: "DOUBLE",
        },
      ],
      ignore_columns: ["intenal_id", "notes"],
    }

Note that a column cannot appear in both ``columns`` and ``ignore_columns`` - dsgrid will
raise an error if there is any overlap.

Time
====

Time zones
----------
Timestamps must be converted to UTC when written to the Parquet files. Do not use the Pandas
feature where it records time zone information into the Parquet metadata.

We recommend that you use Spark to create the Parquet files, but that is not required.
If you do use Spark, note the following:

- Spark implicitly interprets timestamps in the time zone of the current SQL session and converts
  them to UTC when writing dataframes to Parquet.
- You can override the SQL session time zone programmatically or in your Spark configuration file.
  The setting is ``spark.sql.session.timeZone``.

Time zone aware timestamps
~~~~~~~~~~~~~~~~~~~~~~~~~~
``dsgrid`` can convert timestamps in data tables to the proper time zone looking up the time
dimension.

Time zone unaware timestamps
----------------------------
Time-zone-unaware timestamps that will be interpreted as local time should be written as UTC
timestamps (i.e., 12pm with no time zone should be written as 12pm UTC).


Data Layout
===========
The ``data_layout`` section of a dataset configuration defines the data file locations and
table format. It has the following structure:

.. code-block:: JavaScript

    data_layout: {
      table_format: "two_table",       // or "one_table"
      value_format: {
        format_type: "pivoted",        // or "stacked"
        pivoted_dimension_type: "metric",  // required if pivoted
      },
      data_file: {
        path: "load_data.parquet",
        columns: [                     // optional
          {
            name: "column_name",       // actual name in the file
            data_type: "STRING",       // optional, for type override
            dimension_type: "geography",  // optional, for column renaming
          },
        ],
        ignore_columns: ["col1", "col2"],  // optional, columns to drop
      },
      lookup_data_file: {              // required for "two_table" format
        path: "load_data_lookup.parquet",
        columns: [],                   // optional, same structure as data_file
        ignore_columns: [],            // optional, same as data_file
      },
      missing_associations: [          // optional, list of paths
        "missing_associations.parquet",
        "additional_missing",
      ],
    }

Fields:

- ``table_format``: Defines the table structure - ``"two_table"`` or ``"one_table"``.
- ``value_format``: Defines whether values are pivoted or stacked.
- ``data_file``: Main data file configuration (required).

  - ``path``: Path to the data file. Can be absolute or relative to the config file. You can also use the ``--data_base_dir`` CLI option to specify a different base directory for resolving relative paths.
  - ``columns``: Optional list of column definitions for type overrides and renaming.

    - ``name``: The actual column name in the file (required).
    - ``data_type``: Data type override (optional). See :ref:`csv-data-types` for supported types.
    - ``dimension_type``: The dsgrid dimension type this column represents (optional).
      When specified, the column will be renamed to match the dimension type.

  - ``ignore_columns``: Optional list of column names to drop when reading the file.
    Cannot overlap with columns defined in ``columns``.

- ``lookup_data_file``: Lookup file configuration (required for ``two_table`` format).
  Has the same structure as ``data_file``.
- ``missing_associations``: List of paths to files or directories defining missing dimension
  combinations (optional, see :ref:`missing-associations`). Paths can be absolute or relative
  to the config file. You can also use the ``--missing-associations-base-dir`` CLI option to
  specify a different base directory for resolving relative paths.


Formats
=======
Input datasets can use a one-table or two-table format as described below.

Both formats support pivoting the record IDs of one dimension as an option.

- ``Pivoted``: All dimensions except the pivoted dimension are columns in the table. The record IDs
  of the pivoted dimension are columns in the table. Several dsgrid datasets
  pivot the metric dimension in order to avoid many repeated rows of other dimensions. This saves
  storage space but can make queries more complicated. dsgrid handles that complexity on the back
  end, but this point can still apply to users that inspect the raw datasets.
- ``Stacked``: The table has one column per dimension (except time, which might have more than
  one column) and a column called ``value`` that contains the data values. This format
  makes queries simpler. It is also good for cases when there is not a sensible dimension to pivot.

.. _one-table-format:

One Table Format
----------------
All metric data and dimension records are stored in one Parquet file.

::

    +-------------------+---------+------------------+--------------------+
    |          timestamp|geography|          scenario|           subsector|
    +-------------------+---------+------------------+--------------------+
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|full_service_rest...|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|      primary_school|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|quick_service_res...|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|   retail_standalone|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|    retail_stripmall|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|         small_hotel|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|        small_office|
    |2011-12-31 22:00:00|    01001|      efs_high_ldv|           warehouse|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|full_service_rest...|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|      primary_school|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|quick_service_res...|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|   retail_standalone|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|    retail_stripmall|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|         small_hotel|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|        small_office|
    |2011-12-31 22:00:00|    01001|ldv_sales_evs_2035|           warehouse|
    |2011-12-31 22:00:00|    01001|         reference|full_service_rest...|
    |2011-12-31 22:00:00|    01001|         reference|      primary_school|
    |2011-12-31 22:00:00|    01001|         reference|quick_service_res...|
    |2011-12-31 22:00:00|    01001|         reference|   retail_standalone|
    +-------------------+---------+------------------+--------------------+

.. _two-table-format:

Two Table Format
----------------
Two Parquet files comprise the dataset:

- ``load_data.parquet``: Metric data, usually with time-series data. This example pivots the metric
  dimension records.

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


- ``load_data_lookup.parquet``: Metadata that connects dimension records with the metric data. Must
  include a row with a null ``id`` for every combination of required dimensions that does not exist
  in ``load_data``.

::

    +---------+------+----------+-------+
    |geography|sector| subsector|     id|
    +---------+------+----------+-------+
    |    53061|   com|  Hospital|      1|
    |    53053|   com|  Hospital|      2|
    |    53005|   com|  Hospital|      3|
    |    53025|   com|  Hospital|      4|
    |    53045|   com|  Hospital|      5|
    +---------+------+----------+-------+

Each unique time array in ``load_data`` must be denoted with an ID that corresponds to a record in
``load_data_lookup``. The ID is user-defined. Users may want to use a sequentially-increasing
integer or encode other information into specific bytes of each integer.

The table may optionally include the column ``scaling_factor`` to account for cases where the value
columns need to multiplied by a scaling factor. If ``scaling_factor`` does not apply, the value in
the row can be ``1.0`` or ``null``.

This format minimizes file storage because

1. Time arrays can be shared across combinations of dimension records, possibly with different
   scaling factors.
2. Dimension information is not repeated for every timestamp. (This could be minimal because of
   compression inside the Parquet files.)

Time Formats
============

DateTime
--------
The load data table has one column representing time, typically called ``timestamp``. When written
to Parquet files the type should be the ``TIMESTAMP`` logical type (integer, not string) and be
adjusted to UTC. When read into Spark the type should be ``TimestampType`` (not
``TimestampNTZType``).

Handling of no-time-zone timestamps (Spark type ``TimestampNTZType``) is possible. Contact the
dsgrid team if you need this.

Annual
------
Load data contains one value per model year.

::

    [2020, 2021, 2022]

Representative Period
---------------------
Metric data contains timestamps that represent multiple periods. dsgrid supports the following
formats:

one_week_per_month_by_hour
~~~~~~~~~~~~~~~~~~~~~~~~~~
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

.. _missing-associations:

Missing Associations
====================
Datasets may have missing dimension combinations (associations) - for example, a building model
might not have data for certain geography-subsector combinations because those building types
don't exist in those regions.

dsgrid validates that datasets provide data for all expected dimension combinations. When a
dataset legitimately lacks data for certain combinations, you must explicitly declare these
missing associations.

Declaring Missing Associations
------------------------------
Specify missing associations in the ``data_layout`` section of your dataset config using
the ``missing_associations`` field. This field accepts a list of paths to files or directories:

.. code-block:: JavaScript

    data_layout: {
      table_format: "one_table",
      value_format: { format_type: "stacked" },
      data_file: { path: "load_data.parquet" },
      missing_associations: [
        "missing_associations.parquet",
        "additional_missing",
      ],
    }

Each entry in the list can be:

1. **A single file** (CSV or Parquet) containing missing combinations
2. **A directory** containing multiple files, each for different dimension combinations

Paths can be absolute or relative. Relative paths are resolved relative to the dataset
configuration file by default. Alternatively, a different base directory can be specified using the ``--missing-associations-base-dir`` CLI option.

File Format
~~~~~~~~~~~
Missing association files can be in CSV or Parquet format. They should contain columns for
dimension types (all types except time). Each row represents a combination of dimension
records that legitimately has no data.

A file can contain any subset of the non-time dimension columns. During validation, dsgrid
filters out rows from the expected associations that match the missing associations
listed in the file.

Example ``missing_associations.parquet`` with all non-time dimensions::

    +---------+------+-----------+--------+----------+------------+
    |geography|sector|  subsector|  metric|model_year|weather_year|
    +---------+------+-----------+--------+----------+------------+
    |    01001|   com|large_hotel|heating |      2020|        2018|
    |    01001|   com|  warehouse|cooling |      2020|        2018|
    |    01003|   com|large_hotel|heating |      2020|        2018|
    +---------+------+-----------+--------+----------+------------+

Example ``missing_associations.csv`` with only two dimensions::

    +---------+-----------+
    |geography|  subsector|
    +---------+-----------+
    |    01001|large_hotel|
    |    01001|  warehouse|
    |    01003|large_hotel|
    +---------+-----------+
In this case, all metrics, model_years, and weather_years are expected to be missing for these combinations of (geography, subsector).
Directory Format
~~~~~~~~~~~~~~~~
When using a directory, create separate files for different dimension combinations.

Example directory structure::

    missing_associations/
    ├── geography__subsector.csv
    ├── geography__metric.csv
    └── subsector__metric.parquet

Each file contains the relevant dimension columns::

    # geography__subsector.csv
    geography,subsector
    01001,large_hotel
    01001,warehouse

Using Custom Base Directories
-----------------------------
When registering a dataset, you can specify a custom base directory for resolving missing
associations paths using the ``--missing-associations-base-dir`` (or ``-M``) option:

.. code-block:: bash

    dsgrid registry datasets register dataset.json5 \
      -l "Register my dataset" \
      -M /path/to/missing/files

When this option is provided, any relative paths in the ``missing_associations`` list will
be resolved relative to the specified directory instead of the dataset configuration file's
directory.

Similarly, you can use ``--data-base-dir`` (or ``-D``) to specify a base directory for
data files:

.. code-block:: bash

    dsgrid registry datasets register dataset.json5 \
      -l "Register my dataset" \
      -D /path/to/data/files \
      -M /path/to/missing/files

These options are also available for the ``register-and-submit-dataset`` command:

.. code-block:: bash

    dsgrid registry projects register-and-submit-dataset \
      -c dataset.json5 \
      -p my-project-id \
      -l "Register and submit dataset" \
      -D /path/to/data/files \
      -M /path/to/missing/files

Iterative Workflow for Identifying Missing Associations
-------------------------------------------------------
If you don't know which dimension combinations are missing in your dataset, dsgrid provides
an iterative workflow to help you identify them:

1. **Run registration without missing associations**

   Attempt to register your dataset without specifying ``missing_associations``. If there are
   missing combinations, registration will fail.

2. **Review generated outputs**

   When registration fails due to missing associations, dsgrid writes a Parquet file named
   ``<dataset_id>__missing_dimension_record_combinations.parquet`` to the current directory.
   This file contains all the missing dimension combinations with all dimensions. This file can
   contain huge numbers of rows.

   dsgrid also analyzes the missing data to identify minimal patterns that explain the gaps.
   These patterns are logged and can help you understand *why* data is missing. For example,
   you might see::

       Pattern 1: geography | subsector = 01001 | large_hotel (150 missing rows)
       Pattern 2: subsector = warehouse (3000 missing rows)

   This tells you that all combinations involving county 01001 and large_hotel are missing,
   and all combinations involving warehouse are missing.

   dsgrid records these minimal patterns in the  ``./missing_associations/`` directory,
   in dimension-specific combination files such as ``geography__subsector.csv`` and
   ``sector__subsector.csv``.

3. **Choose which output to use and revise as appropriate**

   You have several options for declaring missing associations:

   - **Use the all-inclusive Parquet file**: Reference the generated
     ``<dataset_id>__missing_dimension_record_combinations.parquet`` file directly. This
     contains every missing combination but may be very large.

   - **Use the per-dimension CSV files**: Reference the ``./missing_associations/`` directory
     containing the minimal pattern files. This is more compact and easier to review.

   - **Create your own files**: Create custom CSV or Parquet files based on your understanding
     of the data. This gives you full control over what is declared as missing.

   No matter which option you select, you may want to:

   - Fix data errors revealed by the missing data analysis
   - Remove rows corresponding to data errors that you fix
   - Pick and choose or reorganize the information
   - Combine multiple sources if needed.

4. **Re-run registration with missing associations**

   Add the ``missing_associations`` field to your ``data_layout`` pointing to the files or
   directories:

   .. code-block:: JavaScript

       data_layout: {
         table_format: "one_table",
         value_format: { format_type: "stacked" },
         data_file: { path: "load_data.parquet" },
         // Option 1: Use the all-inclusive Parquet file
         missing_associations: ["./my_dataset__missing_dimension_record_combinations.parquet"],

         // Option 2: Use the per-dimension directory
         // missing_associations: ["./missing_associations"],

         // Option 3: Combine multiple sources
         // missing_associations: [
         //   "./missing_associations",
         //   "./additional_missing.parquet",
         // ],
       }

   Run registration again. If successful, the missing associations will be stored in the
   registry alongside your dataset.

Validation Behavior
-------------------
During dataset registration, dsgrid checks that:

1. All dimension combinations in the data files are valid (records match dimension definitions).
2. All expected combinations either have data or are declared as missing.

If dsgrid finds unexpected missing combinations, it will report an error and write the
missing combinations to files as described above.
