# Dataset Submitters

Dataset submitters prepare and register datasets for inclusion in a dsgrid project. The first step, *dataset registration*, involves defining dimensions, creating a dataset configuration file, and verifying the dataset's internal consistency (schema, dimensions, and data completeness). Once that is complete, the data submitter prepares for *project submittal* by creating dimension mappings and an associated mappings configuration file. When everything is submitted to the project, the project verifies the internal consistency of the dimension mappings and that the dataset provides all expected data points. Dataset registration is supported by the commands `dsgrid registry datasets generate-config` and `dsgrid registry datasets register`. The intention is for dataset submitters to go through these steps themselves, in the same computational environments they used to create the original dataset(s). Project submittal and subsequent use sometimes involves exploding out the dimensions of the dataset, in which case project submission might be performed primarily by the project coordinator using Apache Spark.

## Prerequisites

- [Install dsgrid](installation) on your system
- Create or access a dsgrid registry (a pre-populated dsgrid registry can assist with identifying dimensions)
- Your dataset in a supported format (see [Data File Formats](../user_guide/dataset_registration/data_file_formats))
- Familiarity with, or a IDE extension for, [JSON5](https://json5.org/) syntax
- Access to the project config file and optionally the project registry
  - The config file is typically available in a project-specific repository of config files
  - Iteration with the project coordinator might be needed to bring the project and dataset configurations and other details into alignment

## Workflow Overview

### Phase 1 — Dataset Registration

Registers the dataset as a standalone entity in the registry. Validates internal integrity (schema, dimensions, and data completeness). No dsgrid project is required.

1. **Understand the fundamentals** — Read [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts) and [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts) to understand how dsgrid organizes data.
2. **Create an initial draft of the config and dimension record files** — Run `dsgrid registry datasets generate-config` to auto-generate a `dataset.json5` and dimension record CSVs from your dataset file(s). The tool searches the registry for matching dimensions (prioritizing project base dimensions if `-P` is given).
3. **Refine your dataset config and dimensions** — Review and edit the generated config and dimension record files. Regarding the config file, see [Dataset Config Files](../user_guide/dataset_registration/config_files) for a basic description and the [Dataset Data Model](../software_reference/data_models/dataset_model) for the full schema. Follow [How to Create Dataset Dimensions](../user_guide/how_tos/how_to_dimensions) for guidance on dimension records.
4. **Register your dataset** — Run `dsgrid registry datasets register`. This validates internal integrity: schema, dimensions, and data completeness.
5. **Address missing dimension associations** — If registration fails with missing records, dsgrid writes the missing combinations to a Parquet file and runs pattern analysis (via `find_minimal_patterns`) to help identify root causes. Either fix the data gaps or declare expected missing associations in the dataset config using the `missing_associations` field (and use the `-M` CLI option to specify a base directory for the missing association files as needed). Iterate on steps 4–5 as needed. See [How to Handle Missing Dimension Associations](../user_guide/how_tos/how_to_missing_associations) for the full workflow.

### Phase 2 — Project Submittal

Submits the registered dataset to a specific project. Dimension mappings are usually required to align dataset dimensions with project base dimensions. Validates that dimension mappings are consistent and that the dataset provides all expected data points.

6. **Review project requirements** — Check what dimensions and data points the project expects from your dataset. Browse the project's repository of config files or use [How to Browse the Registry](../user_guide/how_tos/browse_registry) to inspect the project's base dimensions.
7. **Create dimension mappings** — Map dataset dimensions to project base dimensions.
8. **Submit your dataset to the project** — Run `dsgrid registry projects submit-dataset` (or use `register-and-submit-dataset` for a combined operation). Follow the [Dataset Submission Process](../user_guide/dataset_submittal/submission_process) for details.

## When You Need Apache Spark

Small datasets can be registered using the default DuckDB backend. If your dataset is large or maps onto high-resolution project dimensions (e.g., hourly × county), the submission step may require Spark for adequate performance. In that case:

- Install the Spark extras: `pip install "dsgrid-toolkit[spark]"`
- See [How to Start a Spark Cluster on Kestrel](../user_guide/how_tos/spark_cluster_on_kestrel) for running on NLR HPC

## Key Resources

### Core Concepts

- [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts)
- [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts)
- [Data File Formats](../user_guide/dataset_registration/data_file_formats)

### How-Tos

- [How to Browse the Registry](../user_guide/how_tos/browse_registry)
- [How to Create Dataset Dimensions](../user_guide/how_tos/how_to_dimensions)

### Tutorials

- [Create and Submit a Dataset](../user_guide/tutorials/create_and_submit_dataset)
