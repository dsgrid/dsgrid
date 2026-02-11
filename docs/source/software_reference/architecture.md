# Software Architecture

This page describes the dsgrid software architecture.

:::{todo}
Add architecture diagrams
:::

## Overview

dsgrid is built on a distributed architecture designed to handle large-scale energy demand data:

- **Store project and dataset metadata in SQLite** - Centralized registry database
- **Store dimension and dimension-mapping metadata and records in SQLite** - Efficient lookups
- **Store dataset time-series data in Parquet files** - On shared filesystem (e.g., Lustre, S3)
- **Store dependencies between registry components in SQLite** - Version tracking and relationships
- **Store version history of registry components in SQLite** - Full audit trail
- **Load all data tables in Apache Spark** - Use DataFrame API for queries
- **Convert to Pandas DataFrames** - For post-processing and visualizations

## APIs

### Python

dsgrid applications use the dsgrid Python package to perform all operations.

The Python API provides programmatic access to:
- Registry management
- Dataset registration and mapping
- Project creation and configuration
- Query execution and result processing
- Dimension operations

See the [Python API Reference](../reference/dsgrid_api/index) for complete documentation.

### HTTP

There is currently an HTTP API to run a limited number of dsgrid operations. This will expand in the future and may become the primary interface for user applications. That will require a persistent dsgrid server.

**Current capabilities:**
- Basic registry browsing
- Project metadata retrieval
- Dataset information queries

**Future plans:**
- Full registry operations
- Query execution via HTTP
- Real-time result streaming

## Applications

Users interact with dsgrid through these applications.

### CLI Toolkit

This is the primary user interface to dsgrid. Users run CLI commands to register dsgrid components and run queries.

**Key features:**
- Hierarchical command structure
- Registry management (`dsgrid registry`)
- Query execution (`dsgrid query`)
- Configuration management (`dsgrid config`)
- Notebook installation (`dsgrid install-notebooks`)

The CLI consumes the Python API. See [CLI Reference](../reference/cli) for complete documentation.

### Project Viewer

Web UI based on Plotly Dash. Allows users to browse and filter project and dataset components.

**Capabilities:**
- Browse registered projects and datasets
- View dimension records
- Filter and search metadata
- Explore project structure

The Project Viewer consumes the HTTP API. See [Browse Registry](../user_guide/registry/browse_registry) for usage instructions.

## Current Workflow

Future workflows may change significantly. We may have a persistent database and dsgrid API server running in the cloud with on-demand Spark clusters. For the foreseeable future, this is what we anticipate the user workflow to be:

### Typical HPC Workflow

1. **Start a Spark cluster** on one or more compute nodes
   - See [Start Spark Cluster on Kestrel](../user_guide/apache_spark/spark_cluster_on_kestrel)

2. **Connect to a dsgrid registry database** or start your own
   - Existing registry: Connect via database URL
   - New registry: Initialize with `dsgrid registry create`

3. **Run dsgrid CLI commands** from a compute node with access to:
   - Registry database
   - Registry data (Parquet files)
   - Spark compute nodes

   When running on an HPC, this compute node is usually the Spark master node.

### Local Development Workflow

1. **Install dsgrid** in a conda environment
   - See [Installation](../getting_started/installation)

2. **Configure local registry** connection
   - Use `dsgrid config create` to set up database connection

3. **Run operations** directly from your local machine
   - Suitable for small datasets and testing
   - Limited by local computational resources

## Technology Stack

### Core Technologies

- **Apache Spark** - Distributed data processing
- **SQLite** - Registry metadata storage
- **Parquet** - Columnar data format for time-series
- **Pydantic** - Data validation and configuration models
- **Click** - CLI framework

### Data Processing

- **PySpark** - Python interface to Spark
- **Pandas** - Post-processing and analysis
- **PyArrow** - Efficient data interchange

### Web Technologies

- **Plotly Dash** - Interactive web applications
- **FastAPI** - HTTP API framework (planned expansion)

## Data Flow

### Dataset Registration

```
User → CLI → Python API → Registry DB
                        → Parquet Files (validation)
```

### Query Execution

```
User → CLI → Python API → Registry DB (metadata)
                        → Spark Cluster → Parquet Files (data)
                        → Result Files (Parquet)
```

### Web UI Access

```
User → Web Browser → Dash App → HTTP API → Registry DB
```

## Next Steps

- Learn about [CLI fundamentals](cli_fundamentals)
- Explore the [Python API](../reference/dsgrid_api/index)
- Understand [dataset formats](../user_guide/dataset_registration/formats)
- See [data models](data_models/index) for configuration schemas
