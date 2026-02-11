# Project Coordinators

Project coordinators define the structure of a dsgrid project: its base dimensions, supplemental dimensions, dataset requirements, and queries. They are responsible for assembling datasets from multiple contributors into a coherent, queryable whole.

## Prerequisites

- [Install dsgrid](installation) on your system, including Spark extras: `pip install "dsgrid-toolkit[spark]"`
- Access to NREL HPC (most project coordination tasks involve large datasets)
- See [How to Start a Spark Cluster on Kestrel](../user_guide/how_tos/spark_cluster_on_kestrel) for cluster setup

## Workflow Overview

1. **Design the project** — Define the dimensional structure that all datasets will map to. Read [Project Concepts](../user_guide/project_creation/concepts) for the design considerations.
2. **Create base dimensions** — Define the finest-grained dimensions the project will support. Follow [How to Create Base Dimensions](../user_guide/how_tos/how_to_base_dimensions).
3. **Create supplemental dimensions** — Define alternative aggregations for querying (e.g., counties → states). Follow [How to Create Supplemental Dimensions](../user_guide/how_tos/how_to_supplemental_dimensions).
4. **Register the project** — Create the project config and register it. See the [Create a Project](../user_guide/tutorials/create_project) tutorial.
5. **Coordinate dataset submissions** — Work with dataset submitters and mappers to register and validate their contributions.
6. **Define and run queries** — Assemble the data using queries. See [Query Concepts](../user_guide/project_queries/concepts) and [How to Filter Query Results](../user_guide/how_tos/how_to_filter).
7. **Create derived datasets** — Build derived datasets from query results for publication. See [Derived Dataset Concepts](../user_guide/project_derived_datasets/concepts).

## Key Resources

### Core Concepts

- [Project Concepts](../user_guide/project_creation/concepts)
- [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts)
- [Query Concepts](../user_guide/project_queries/concepts)
- [Derived Dataset Concepts](../user_guide/project_derived_datasets/concepts)

### How-Tos

- [How to Browse the Registry](../user_guide/how_tos/browse_registry)
- [How to Create Base Dimensions](../user_guide/how_tos/how_to_base_dimensions)
- [How to Create Supplemental Dimensions](../user_guide/how_tos/how_to_supplemental_dimensions)
- [How to Filter Query Results](../user_guide/how_tos/how_to_filter)
- [How to Run dsgrid on Kestrel](../user_guide/how_tos/run_on_kestrel)
- [How to Start a Spark Cluster on Kestrel](../user_guide/how_tos/spark_cluster_on_kestrel)

### Tutorials

- [Create a Project](../user_guide/tutorials/create_project)
- [Query a Project](../user_guide/tutorials/query_project)
- [Create a Derived Dataset](../user_guide/tutorials/create_derived_dataset)

### Software Reference

- [Project Data Model](../software_reference/data_models/project_model)
- [Dimension Data Model](../software_reference/data_models/dimension_model)
