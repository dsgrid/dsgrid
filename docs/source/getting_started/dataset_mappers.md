# Dataset Mappers

Dataset mappers create dimension mappings and use them to transform datasets, for example, to compute aggregations or remap data to different dimensions using a dataset query.

Dimension mappings are also central to other dsgrid workflows, particularly when submitting datasets to projects and when running project-level queries. The mapping documentation linked from this page covers the shared concepts, types, and mechanics that apply across all of these contexts.

## Prerequisites

- [Install dsgrid](installation) on your system
- A [registered dataset](dataset_submitters) in the dsgrid registry
- Familiarity with the dataset's dimension records
- Familiarity with, or an IDE extension for, [JSON5](https://json5.org/) syntax

## Workflow Overview

1. **Understand dimension mappings** — Read [Dimension Mapping Concepts](../user_guide/dataset_mapping/dimension_mapping_concepts) to learn how dsgrid translates between dimension systems, how mapping configs are structured, and how to register mappings.
2. **Identify target dimensions** — Determine which dimensions to map to. Use [How to Browse the Registry](../user_guide/how_tos/browse_registry) to inspect available dimensions and compare them with your dataset's dimensions.
3. **Choose mapping types** — Review [Dimension Mapping Types](../user_guide/dataset_mapping/dimension_mapping_types) to select the right type for each dimension (one-to-one, many-to-one aggregation, many-to-many explicit multipliers, etc.).
4. **Create and register mappings** — Write CSV mapping files and mapping configs, then register them. Follow [How to Create Dimension Mappings](../user_guide/how_tos/how_to_dimension_mappings) for the step-by-step process.
5. **Run a dataset query** — Use `dsgrid query dataset create-query` to generate a query template, populate it with target dimensions, and run it with `dsgrid query dataset run`. See [Dataset Query Concepts](../user_guide/dataset_mapping/dataset_query_concepts) for how this works and the [Query a Dataset](../user_guide/tutorials/query_dataset) tutorial for a walkthrough.

## When You Need Apache Spark

Mapping can be computationally intensive for datasets with many records or fine-grained dimensions. If you are working with large datasets on NLR HPC:

- Install the Spark extras: `pip install dsgrid-toolkit[spark]`
- See [How to Run dsgrid on Kestrel](../user_guide/how_tos/run_on_kestrel)

## Key Resources

### Core Concepts

- [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts)
- [Dimension Mapping Concepts](../user_guide/dataset_mapping/dimension_mapping_concepts)
- [Dimension Mapping Types](../user_guide/dataset_mapping/dimension_mapping_types)
- [Dataset Query Concepts](../user_guide/dataset_mapping/dataset_query_concepts)

### How-Tos

- [How to Browse the Registry](../user_guide/how_tos/browse_registry)
- [How to Create Dimension Mappings](../user_guide/how_tos/how_to_dimension_mappings)

### Tutorials

- [Query a Dataset](../user_guide/tutorials/query_dataset)

### Software Reference

- [Dimension Mapping Data Model](../software_reference/data_models/dimension_mapping_model)
- [DimensionMappingType Enum](../software_reference/data_models/enums.md#dimensionmappingtype)
- [Dataset Query Data Models](../software_reference/data_models/dataset_query_model)
- [CLI Reference](../software_reference/cli_reference)
