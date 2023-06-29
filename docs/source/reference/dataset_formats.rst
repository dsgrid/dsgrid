.. _dataset-formats:

****************
Dataset Formats
****************

dsgrid aims to support all dataset formats that users need for efficient queries and analysis. If
you need a new format, please contact the dsgrid team to discuss it.

Requirements
=============
1. Metric data must be stored in Parquet files. If you need or want another optimized columnar file
   format, please contact the dsgrid team.
2. If the data tables contain time-series data, each unique time array must contain an identical
   range of timestamps.
3. Tables must be pivoted with the records of one dimension. This is usually much more efficient
   than a "long-narrow" format with a single "metric value" column. Support could be added if
   necessary.
4. Values of dimension columns must be strings. This includes ``model_year`` and ``weather_year``.
5. Each dimension column name except time must match dsgrid dimension types (geography, sector,
   subsector, etc.).
6. The values in each dimension column must match the dataset's dimension records.

Recommendations
===============
1. Enable compression in all Parquet files. ``Snappy`` is preferred.
2. The recommended size of individual Parquet files is 128 MiB. Making the files too big can cause
   memory issues. Making them too small adds overhead and hurts performance.
3. Trivial dimensions (one-element records) should not be stored in the data files. They should
   instead be defined in the dataset config. dsgrid will add them dynamically at runtime.

.. warning:: Currently, the pivoted dimension must be the metric dimension. This limitation is
   expected to be fixed soon.

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


Formats
=======
Input datasets can use the formats below. dsgrid uses the one table format for derived datasets.

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


Two Table Format (Standard)
----------------------------
Two Parquet files comprise the dataset:

- ``load_data.parquet``: Metric data, usually with time-series data. This example pivots the metric
  dimensions.

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
    +---------+---+------------------+-------+

Each unique time array in ``load_data`` must be denoted with an ID that corresponds to a record in
``load_data_lookup``. The ID is user-defined. Users may want to use a sequentially-increasing
integer or encode other information into specific bytes of each integer.

This format minimizes file storage because

1. Time arrays can be shared across combinations of dimension records.
2. Dimension information is not repeated for every timestamp. (This could be minimal because of
   compression inside the Parquet files.)

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
