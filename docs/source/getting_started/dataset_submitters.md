# Dataset Submitters

Dataset submitters prepare and register datasets for inclusion in a dsgrid project. This involves defining dimensions, creating configuration files, and submitting the dataset to the project's registry.

## Prerequisites

- [Install dsgrid](installation) on your system
- Access to the project registry (database URL and project ID from the project coordinator)
- Your dataset in a supported format (see [Data Formats](../user_guide/dataset_registration/formats))

## Workflow Overview

1. **Understand the fundamentals** — Read [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts) and [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts) to understand how dsgrid organizes data.
2. **Review project requirements** — Check what dimensions and formats the project expects. Use [How to Browse the Registry](../user_guide/how_tos/browse_registry) to inspect the project's base dimensions.
3. **Define your dimensions** — Create dimension configs and record files for your dataset. Follow [How to Create Dataset Dimensions](../user_guide/how_tos/how_to_dimensions).
4. **Create your dataset config** — See [Config Files](../user_guide/dataset_registration/config_files) and the [Dataset Data Model](../software_reference/data_models/dataset_model) for the full schema.
5. **Submit your dataset** — Follow the [Submission Process](../user_guide/dataset_submittal/submission_process) to register and submit.

## When You Need Apache Spark

Small datasets can be registered using the default DuckDB backend. If your dataset is large or maps onto high-resolution project dimensions (e.g., hourly × county), the submission step may require Spark for adequate performance. In that case:

- Install the Spark extras: `pip install "dsgrid-toolkit[spark]"`
- See [How to Run dsgrid on Kestrel](../user_guide/how_tos/run_on_kestrel) for running on NREL HPC

## Key Resources

### Core Concepts

- [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts)
- [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts)
- [Data Formats](../user_guide/dataset_registration/formats)
- [Requirements](../user_guide/dataset_registration/requirements)

### How-Tos

- [How to Browse the Registry](../user_guide/how_tos/browse_registry)
- [How to Create Dataset Dimensions](../user_guide/how_tos/how_to_dimensions)

### Tutorials

- [Create and Submit a Dataset](../user_guide/tutorials/create_and_submit_dataset)
