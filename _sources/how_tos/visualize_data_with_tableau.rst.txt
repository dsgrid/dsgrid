***************************
Visualize Data with Tableau
***************************
`Tableau <https://www.tableau.com/>`_ is a commercial tool for exploring and visualizing
tabular data.

In addition to making visualizations, Tableau makes it easy to select, filter, group, and describe
your data in tables. This can be easier than the same operations in a Python REPL with ``pyspark``
or ``pandas``.

This page describes various ways to connect Tableau to dsgrid data after you've installed Tableau
Desktop on your local computer.

Install Tableau
===============
Licenses are available to NREL employees. Go to theSOURCE, then IT Service Portal, Service
Catalog, search for Tableau, and submit a ticket to get ``Tableau Creator`` installed (IT will
install ``Tableau Desktop``).

Parquet files on a local computer
=================================
This can be accomplished by connecting Tableau to DuckDB.

1. Copy the Parquet files to your computer.

2. Install `DuckDB <https://duckdb.org/docs/installation/>`_. You want the ``Command line``
   ``Environment``.

3. Install a JDBC driver and connect Tableau to DuckDB by following `DuckDB's
   <https://duckdb.org/docs/guides/data_viewers/tableau>`_ instructions.

4. Create a view of your data as noted `here
   <https://duckdb.org/docs/guides/data_viewers/tableau#database-creation>`_. You can also import
   your data from Parquet files to a DuckDB database file if you prefer.

5. Use Tableau with your DuckDB data source.

Parquet files on an HPC
========================
This can be accomplished by connecting Tableau to a Spark cluster on the HPC.

Follow the
`Spark-on-HPC instructions <https://github.com/daniel-thom/HPC/blob/kestrel-update/applications/spark/README.md#visualize-data-with-tableau>`_

CSV files on a local computer
=============================
1. Export the dsgrid data in Parquet files to CSV.
2. Load the CSV files directly in Tableau.
