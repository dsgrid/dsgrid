
.. _tutorial_query_a_dataset:

***************************
Query a Dataset with Python
***************************
In this tutorial you will learn how to query data from a registered dsgrid dataset.
use data from the dsgrid registry stored on NREL's HPC Kestrel cluster, and the same data
stored on `OEDI <https://data.openei.org/>`_.

This tutorial will use the `tempo dataset <https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/tempo_project>`_ as an example

Query objectives
================
This query will accomplish the following:

- Read data from Kestrel filesystem and OEDI s3 bucket
- Filter data for given model years, state/country, and scenario
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

   $ module load python  # only if running on kepler

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

   duckdb.sql(f"""CREATE TABLE {tablename} AS SELECT * 
                FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
                hive_partitioning=true, hive_types_autocast=false""")

   duckdb.sql(f"DESCRIBE {tablename}")  # shows columns and types
   duckdb.sql(f"SELECT * FROM {tablename} LIMIT 5").df()  # shows first 5 rows


Load data from OEDI
===================

1. Enter python interpreter the same as above
   
2. Load .parquet files from OEDI into a table
   This step is identical to the previous loading step, except the file path is different

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
                
Export Data
===========

1. Create a pandas dataframe after loading, and possibly filtering, from the previous steps

.. code-block:: python

   dataframe = duckdb.sql("SELECT * FROM {tablename}").df()

2. Export dataframe to csv after createing dataframe

.. code-block:: python

   dataframe.to_csv('~/mydata.csv')

