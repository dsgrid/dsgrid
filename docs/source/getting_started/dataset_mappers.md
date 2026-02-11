# Dataset Mappers

Dataset mappers create the dimension mappings that align a dataset's dimensions with a project's base and supplemental dimensions. This is often done by the dataset submitter, but may be handled by a separate team member who understands both the dataset and project dimension structures.

## Prerequisites

- [Install dsgrid](installation) on your system
- Access to the project registry (to inspect base dimensions)
- Familiarity with the dataset's dimension records

## Workflow Overview

1. **Understand dimension mapping** — Read [Dimension Mapping Concepts](../user_guide/dataset_mapping/concepts) to learn how dsgrid translates between dimension systems.
2. **Inspect project dimensions** — Use [How to Browse the Registry](../user_guide/how_tos/browse_registry) to view the project's base dimension records and compare them with your dataset's dimensions.
3. **Choose mapping types** — Review [Mapping Types](../user_guide/dataset_mapping/mapping_types) (one-to-one, many-to-one, many-to-many) and decide the appropriate type for each dimension.
4. **Create mapping files** — Write CSV mapping files and mapping configs. See [Mapping Workflows](../user_guide/dataset_mapping/mapping_workflows) for the step-by-step process.
5. **Validate** — dsgrid validates mappings during registration. Review any errors and fix your mapping files.

## When You Need Apache Spark

Mapping validation and application can be computationally intensive for datasets with many records or fine-grained dimensions. If you are working with large datasets on NREL HPC:

- Install the Spark extras: `pip install "dsgrid-toolkit[spark]"`
- See [How to Run dsgrid on Kestrel](../user_guide/how_tos/run_on_kestrel)

## Key Resources

### Core Concepts

- [Dimension Mapping Concepts](../user_guide/dataset_mapping/concepts)
- [Mapping Types](../user_guide/dataset_mapping/mapping_types)
- [Dimension Concepts](../user_guide/dataset_registration/dimension_concepts)

### How-Tos

- [How to Browse the Registry](../user_guide/how_tos/browse_registry)

### Tutorials

- [Map a Dataset](../user_guide/tutorials/map_dataset)

### Software Reference

- [Dimension Mapping Data Model](../software_reference/data_models/dimension_mapping_model)
