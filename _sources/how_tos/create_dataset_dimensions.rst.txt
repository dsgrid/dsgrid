********************************
How to create dataset dimensions
********************************

This page assumes that you have already created a dataset config (TODO link).

1. Identify the dimension records that the project is expecting for your dataset.

   - Inspect the project's base dimensions and records.
   - Inspect the ``required_dimensions`` section of the project config for your dataset. It may
     specify a subset of the base dimensions.
   - Resolve any discrepancies with the project coordinator.

2. Record trivial dimensions in the dataset config.
3. Identify the unique records of each dimension type in your dataset. Record them in dimension
   record files. (TODO link)
4. For any dimensions that match the project, record the existing dimension ID in the dataset
   config.
5. For any dimensions that differ from the project, create mappings (TODO link).
6. Define your dimensions in the dataset config.
