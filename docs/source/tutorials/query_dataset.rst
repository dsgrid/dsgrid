
.. _tutorial_query_a_dataset:

***************************
Query a Dataset with Python
***************************
In this tutorial you will learn how to query data from a registered dsgrid dataset.
use data from the dsgrid registry stored on NREL's HPC Kestrel cluster, and the same data
stored on `OEDI <https://data.openei.org/>`_.

This tutorial will use the `state_level_simplified` dataset from the `tempo project <https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/tempo_project>`_ as an example.

Query objectives
================
This query will accomplish the following:

- Read data from Kestrel filesystem and OEDI s3 bucket
- Filter data for given model years, geography, scenario, and other dimensions
- Export the query results to a pandas dataframe or a csv

Required Knowledge
==================

- How to setup a python virtual environment
- How to install python modules
- How to use a jupyter notebook or python interpreter

Setup Environment
=================

1. Setup python environment

from a terminal run:

.. code-block:: bash

   $ module load python  # only if running on Kestrel

   $ python -m venv dsgrid-tutorial
   $ source dsgrid-tutorial/bin/activate

2. Install duckdb and pandas

.. code-block:: bash

   $ pip install duckdb
   $ pip install pandas


Load data from Kestrel
======================

1. Enter a python interpreter

.. code-block:: bash

   $ python

2. Load .parquet files from Kestrel into a table

.. code-block:: python

   import duckdb

   tablename = "tbl"
   data_dir = "/datasets/dsgrid/dsgrid-tempo-v2022"
   dataset_name = "state_level_simplified"
   filepath = f"{data_dir}/{dataset_name}"

   duckdb.sql(f"""CREATE VIEW {tablename} AS SELECT * 
                FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
                hive_partitioning=true, hive_types_autocast=false)""")

   duckdb.sql(f"DESCRIBE {tablename}")  # shows columns and types
   duckdb.sql(f"SELECT * FROM {tablename} LIMIT 5").todf()  # shows first 5 rows


Load data from OEDI
===================

1. Enter python interpreter the same as above
   
2. Load .parquet files from OEDI into a table
   This step is identical to the previous loading step, except the file path is different, and we will create a table instead of a view

.. code-block:: python

   import duckdb

   tablename = "tbl"
   data_dir = "s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0/"
   dataset_name = "state_level_simplified"
   filepath = f"{data_dir}/{dataset_name}"

   duckdb.sql(f"""CREATE TABLE {tablename} AS SELECT * 
                FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
                hive_partitioning=true, hive_types_autocast=false""")

   duckdb.sql(f"DESCRIBE {tablename}")  # shows columns and types
   duckdb.sql(f"SELECT * FROM {tablename} LIMIT 5").df()  # shows first 5 rows

Filter data with duckdb
=======================

One of the main advantages to using duckdb is the ability to filter
data while loading. If a table is created with a filter, duckdb will
not have to read all of the data to generate the requested table. This
can make queries much more efficient.

Using the same tablename and filepath from the sections above

.. code-block:: python

   duckdb.sql("""CREATE TABLE {tablename} AS SELECT *
                FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
                hive_partitioning=true, hive_types_autocast=false
                WHERE state='MI' AND scenario='efs_high_ldv'
             """)

Aggregation and metadata
========================

This example will cover 2 distinct topics:
 - aggregation with duckdb
 - how to use dsgrid metadata in a query

 dsgrid datasets contain a metadata.json file that specifies dimensions, their column names and query_names, and the value column of the dataset. Metadata can be loaded to generate information about each dimension in a variable called columns_by_type, and the value_column, in the following code:

 .. code-block:: python

   import json

   dataset_path = "/datasets/dsgrid/dsgrid-tempo-v2022/state_level_simplified"
   metadata_path = f"{dataset_path}/metadata.json"
   with open(metadata_path, 'r') as fs:
      metadata = json.load(fs)

   assert metadata["table_format"]["format_type"] == "unpivoted", "expecting an unpivoted table"
   value_column = metadata["table_format"]["value_column"]
   columns_by_type = {dim_type: metadata["dimensions"][dim_type][0]["column_names"][0] 
                   for dim_type in metadata["dimensions"] if metadata["dimensions"][dim_type]}

These metadata columnss_by_type and value_column can be used to write queries that would apply to different datasets. The following example will query the `state_level_simplified` datasets, and aggregate the results by: model_year, scenario, geography and subsector with a column for the value summed up across groups.

.. code-block:: python

   duckdb.sql(f"""CREATE TABLE {tablename} AS 
                  SELECT 
                      SUM({value_column}) AS value_sum,
                      {columns_by_type['model_year']},
                      {columns_by_type['scenario']},
                      {columns_by_type['geography']},
                      {columns_by_type['subsector']}
                  FROM read_parquet('{filepath}/table.parquet/**/*.parquet')
                  GROUP BY 
                      {columns_by_type['model_year']},
                      {columns_by_type['scenario']},
                      {columns_by_type['geography']},
                      {columns_by_type['subsector']}
                      """)
                  
This query would also work on the `full_dataset` by using metadata for dimensions, but that query could take hours, or fail because of memory limitations.
                
Export Data
===========

1. Create a pandas dataframe after loading, and possibly filtering, from the previous steps

.. code-block:: python

   dataframe = duckdb.sql("SELECT * FROM {tablename}").df()

2. Export dataframe to csv after creating dataframe

.. code-block:: python

   dataframe.to_csv('mydata.csv')

