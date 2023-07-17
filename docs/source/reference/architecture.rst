************
Architecture
************
This page describes the dsgrid software architecture.

.. toctree::
   :maxdepth: 3
   :hidden:

.. todo:: diagrams

Overview
========
- Store project and dataset metadata in ArangoDB.
- Store dimension and dimension-mapping metadata and records in ArangoDB.
- Store dataset time-series data in Parquet files on a shared filesystem (e.g., Lustre, S3).
- Store dependencies between registry components in an ArangoDB graph.
- Store version history of registry components in an ArangoDB graph.
- Load all data tables in Apache Spark and use its DataFrame API for queries. Convert to Pandas
  DataFrames for post-processing and visualizations.

APIs
----

Python
~~~~~~
dsgrid applications use the dsgrid Python package to perform all operations. The package uses the
ArangoDB Python driver to communicate with the database.

HTTP
~~~~
There is currently a HTTP API to run a limited number of dsgrid operations. This will expand in the
future and may become the primary interface for user applications. That will require a persistent
dsgrid server.

Applications
============
Users use these applications to run dsgrid operations.

CLI Toolkit
-----------
This is the primary user interface to dsgrid. Users will run CLI commands it to register dsgrid
components and run queries. Consumes the Python API.

Project Viewer
--------------
Web UI based on Plotly Dash. Allows the user to browse and filter project and dataset components.
Consumes the HTTP API.

Current Workflow
=================
Future workflows may change significantly. We may have a persistent database and dsgrid API server
running in the cloud with on-demand Spark clusters. For the foreseeable future, this is what we
anticipate the user workflow to be:

1. User starts a Spark cluster on one or more compute nodes.
2. User connects to an existing dsgrid registry database or starts their own.
3. User runs dsgrid CLI commands from a compute node with access to the registry database, registry
   data, and Spark compute nodes. When running on an HPC this compute node is usually the Spark
   master node.
