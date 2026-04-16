# Dataset Submitters

Dataset submitters prepare and register datasets for inclusion in a dsgrid project. The first step, *dataset registration*, involves defining dimensions, creating a dataset configuration file, and verifying the dataset's internal consistency (schema, dimensions, and data completeness). Once that is done, the data submitter prepares for *project submittal* by creating dimension mappings and a dimension mappings configuration file. When everything is submitted to the project, the project verifies the internal consistency of the dimension mappings and that the dataset provides all expected data points.

Dataset registration is supported by the commands `dsgrid registry datasets generate-config` and `dsgrid registry datasets register`. The intention is for dataset submitters to go through these steps themselves, in the same computational environments they used to create the original dataset(s). Project submittal and subsequent use sometimes involves exploding out the dimensions of the dataset, in which case project submission might be performed primarily by the project coordinator using Apache Spark. For smaller datasets and projects, the data submitter might perform this step themselves as well, using `dsgrid registry projects submit-dataset` or `dsgrid registry projects register-and-submit-dataset`.

## Prerequisites

- [Install dsgrid](installation) on your system
- [Create](installation.md#standalone-registry) or [access](installation.md#nlr-shared-registry) a dsgrid registry (a pre-populated dsgrid registry, e.g., supplied by the project coordinator, can assist with identifying dimensions)
- Your dataset in a supported format (see [Data File Formats](../user_guide/dataset_registration/data_file_formats))
- Familiarity with, or an integrated development environment (IDE) extension for, [JSON5](https://json5.org/) syntax
- Access to the project config file and optionally the project registry
  - The config file is typically available in a project-specific repository of config files (e.g., [dsgrid-project-IEF](https://github.com/dsgrid/dsgrid-project-IEF))
  - Be prepared to iterate with the project coordinator to bring the project and dataset configurations into alignment

## Workflow Overview

### Phase 1 — Dataset Registration

Registers the dataset as a standalone entity in the registry. Validates internal integrity (schema, dimensions, and data completeness). No dsgrid project is required.

1. **Understand the fundamentals** — Read [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts) and [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts) to understand how dsgrid organizes data.
2. **Create an initial draft of the config and dimension record files** — Run `dsgrid registry datasets generate-config` to auto-generate a `dataset.json5` and dimension record CSVs from your data file(s). The tool searches the registry for matching dimensions (prioritizing project base dimensions if a `-P` argument is passed).
3. **Refine your dataset config and dimensions** — Review and edit the generated config and dimension record files. Regarding the config file, see [Dataset Concepts](../user_guide/dataset_registration/dataset_concepts) for guidance and the [Dataset Data Model](../software_reference/data_models/dataset_model) for the full schema. Follow [How to Create Dataset Dimensions](../user_guide/how_tos/how_to_dimensions) for guidance on dimension records, and [How to Define a Time Dimension](../user_guide/how_tos/how_to_time_dimension) for time dimension configuration. Before registering, consider which dimension combinations are structurally valid — for example, if only certain subsectors belong to each sector, or certain building types only appear in certain geographies. Define these in `expected_associations` files and reference them in your config's `data_layout`. This narrows the set of combinations dsgrid expects, reducing runtime and improving the quality of `missing_associations` output. See the [Define Dimension Associations](../user_guide/tutorials/define_dimension_associations) tutorial for further description and a worked example.
4. **Register your dataset** — Run `dsgrid registry datasets register`. This validates internal integrity: schema, dimensions, and data completeness. If you defined `expected_associations` in the previous step, dsgrid validates against those combinations rather than the full cross-join.
5. **Refine with missing associations** — If registration fails due to missing dimension combinations, dsgrid writes a Parquet file of all missing combinations and runs pattern analysis (`find_minimal_patterns`) to identify the simplest column subsets that characterize the gaps — for example, specific geography–subsector pairs. It records these minimal patterns as CSV files in a `missing_associations/` directory. Review the output, fix any data bugs, and then reference the legitimate gaps as `missing_associations` in your config file alongside the `expected_associations`. Re-run registration (iterate as needed). See [How to Handle Dimension Associations](../user_guide/how_tos/how_to_dimension_associations) for a concise description of the full workflow.

### Phase 2 — Submit to Project

Submits the registered dataset to a specific project. Dimension mappings are usually required to align dataset dimensions with project base dimensions. Validates that dimension mappings are consistent and that the dataset provides all expected data points.

6. **Review project requirements** — Check what dimensions and data points the project expects from your dataset. The project config's `required_dimensions` entry for your dataset controls which dimension records and combinations you must provide. Use [How to Browse the Registry](../user_guide/how_tos/browse_registry) to inspect the project's base dimensions, or dump the project config with `dsgrid registry projects dump`. See [Understanding Project Requirements](../user_guide/dataset_submittal/submission_process.md#understanding-project-requirements) for details on reading the requirements.
7. **Create dimension mappings** — For each dimension type where your dataset's dimension differs from the project's base dimension, create a dimension mapping. Follow [How to Create Dimension Mappings](../user_guide/how_tos/how_to_dimension_mappings) for the step-by-step process and see [Dimension Mapping Concepts](../user_guide/dataset_mapping/dimension_mapping_concepts) for config structure and CSV format. If a dataset dimension is identical to the project's base dimension (same registered dimension), no mapping is needed for that type.
8. **Submit your dataset to the project** — Run `dsgrid registry projects submit-dataset` (or use `register-and-submit-dataset` for a combined operation). dsgrid will register any new mappings, identify the target base dimensions, and validate that the mapped dataset covers all required dimension combinations. See the [Submission Process](../user_guide/dataset_submittal/submission_process) for command options and mapping file formats, and [Submission Checks](../user_guide/dataset_submittal/submission_checks) for details on the validation checks and troubleshooting.

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
- [How to Define a Time Dimension](../user_guide/how_tos/how_to_time_dimension)
- [How to Handle Dimension Associations](../user_guide/how_tos/how_to_dimension_associations)
- [How to Create Dimension Mappings](../user_guide/how_tos/how_to_dimension_mappings)

### Tutorials

- [Create and Submit a Dataset](../user_guide/tutorials/create_and_submit_dataset)
- [Define Dimension Associations](../user_guide/tutorials/define_dimension_associations)

### Reference

- [CLI Reference](../software_reference/cli_reference)
- [Dataset Config Data Model](../software_reference/data_models/dataset_model)
- [Dimensions Data Model](../software_reference/data_models/dimension_model)
- [Dimension Record Classes](../software_reference/data_models/dimension_classes)
- [Dimension Mappings Data Model](../software_reference/data_models/dimension_mapping_model)
- [Project Config Data Model](../software_reference/data_models/project_model)

### Submission

- [Submission Process](../user_guide/dataset_submittal/submission_process)
- [Submission Checks](../user_guide/dataset_submittal/submission_checks)
- [Dimension Mapping Concepts](../user_guide/dataset_mapping/dimension_mapping_concepts)
- [Dimension Mapping Types](../user_guide/dataset_mapping/dimension_mapping_types)
