# How to Create Dataset Dimensions

This guide walks you through creating dimensions for your dataset.

## Prerequisites

You should have already created a dataset config. See the [dataset registration tutorial](../tutorials/create_and_submit_dataset) for details.

## Steps

### 1. Identify Required Dimension Records

Identify the dimension records that the project is expecting for your dataset:

- Inspect the project's base dimensions and records
- Inspect the `required_dimensions` section of the project config for your dataset. It may specify a subset of the base dimensions
- Resolve any discrepancies with the project coordinator

### 2. Record Trivial Dimensions

Record trivial dimensions in the dataset config. See [dataset concepts](../dataset_registration/dataset_concepts.md#trivial-dimensions) for more information on trivial dimensions.

### 3. Identify Unique Records

Identify the unique records of each dimension type in your dataset. Record them in dimension record files.

### 4. Reference Existing Dimensions

For any dimensions that match the project, record the existing dimension ID in the dataset config.

### 5. Create Dimension Mappings

For any dimensions that differ from the project, create mappings. See the [dataset mapping guide](../dataset_mapping/index) for details.

### 6. Define Dimensions in Config

Define your dimensions in the dataset config. See the [dimension model reference](../../software_reference/data_models/dimension_model) for the complete schema.

## Next Steps

- Learn about [dimension mapping](../dataset_mapping/concepts)
- Follow the complete [dataset registration tutorial](../tutorials/create_and_submit_dataset)
- Understand [data file formats](../dataset_registration/data_file_formats)
