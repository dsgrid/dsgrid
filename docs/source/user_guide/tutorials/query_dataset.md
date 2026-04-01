# Query a Dataset

In this tutorial you will learn how to use `dsgrid query dataset` to map a
registered dataset to alternate dimensions. The example maps county-level data
to state-level by applying a `many_to_one_aggregation` dimension mapping.

This tutorial assumes that:

- You have a registered dataset in the dsgrid registry.
- You have populated your `~/.dsgrid.json5` with the registry location.
- The required dimension mapping is registered (or you will register it).

If you need to create and register a dimension mapping, follow
[How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings) first.

## Step 1: Identify Source and Target Dimensions

Start by inspecting the dataset's current dimensions:

```bash
dsgrid registry datasets list
dsgrid registry datasets show <dataset-id>
```

Identify which dimensions you want to remap. For this example, we want to
aggregate the geography dimension from counties to states. Check what geography
dimensions are available:

```bash
dsgrid registry dimensions list -f "dimension_type == geography"
```

Note the `dimension_id` and `version` of your target dimension (e.g.,
`us-states-l48` version `1.0.0`).

## Step 2: Verify Mapping Exists

Confirm that a mapping exists between the dataset's current geography
dimension and the target:

```bash
dsgrid registry dimension-mappings list
```

Look for a mapping whose `from_dimension` matches the dataset's geography
and `to_dimension` matches the target. If no mapping exists, create and
register one using
[How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings).

## Step 3: Generate a Query Template

Use the CLI to generate a query template:

```bash
dsgrid query dataset create-query my-state-aggregation my-dataset-id
```

This creates `dataset_query.json5` with the following structure:

```json5
{
  name: "my-state-aggregation",
  dataset_id: "my-dataset-id",
  to_dimension_references: [],
  mapping_plan: null,
  wrap_time_allowed: false,
  time_based_data_adjustment: {
    leap_day_adjustment: "none",
    daylight_saving_adjustment: "none",
  },
  result: {
    table_format: {
      format_type: "stacked",
    },
    output_format: "parquet",
    sort_columns: [],
  },
}
```

## Step 4: Populate Target Dimensions

Edit `dataset_query.json5` to add entries to `to_dimension_references`. Each
entry identifies a target dimension by its `type`, `dimension_id`, and
`version`:

```json5
{
  name: "my-state-aggregation",
  dataset_id: "my-dataset-id",
  to_dimension_references: [
    {
      type: "geography",
      dimension_id: "us-states-l48",
      version: "1.0.0",
    },
  ],
  // ... rest of config unchanged
}
```

You can map multiple dimensions in a single query by adding more entries. For
example, to also map model years:

```javascript
  to_dimension_references: [
    {
      type: "geography",
      dimension_id: "us-states-l48",
      version: "1.0.0",
    },
    {
      type: "model_year",
      dimension_id: "standard-model-years",
      version: "1.0.0",
    },
  ],
```

:::{note}
You do not need to specify mapping IDs. dsgrid finds registered mappings
between your dataset's dimensions and the target dimensions automatically.
:::

## Step 5: Run the Query

```bash
dsgrid query dataset run dataset_query.json5 -o query_output/
```

dsgrid will:

1. Load the dataset from the registry
2. Resolve mappings between the dataset's dimensions and each target dimension
3. Apply the mappings to transform the data
4. Write results to `query_output/my-state-aggregation/`

## Step 6: Inspect the Output

The output directory contains the mapped data:

```bash
query_output/
└── my-state-aggregation/
    └── table.parquet
```

You can inspect the results with Python:

```python
import pandas as pd

df = pd.read_parquet("query_output/my-state-aggregation/table.parquet")
print(df.head())
print(f"Unique states: {df['geography'].nunique()}")
```

Or with PySpark if the output is large:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("query_output/my-state-aggregation/table.parquet")
df.show()
df.select("geography").distinct().show()
```

## Using a Mapping Plan for Large Datasets

For large datasets, Spark may struggle to apply all mappings in a single pass.
A mapping plan lets you control the order of operations and persist
intermediate results.

Create a mapping plan file (`mapping_plan.json5`):

```json5
{
  dataset_id: "my-dataset-id",
  mappings: [
    {
      name: "model_year",
      persist: true,
    },
    {
      name: "county",
      persist: true,
    },
  ],
}
```

Add the plan to your query config:

```json5
{
  name: "my-state-aggregation",
  dataset_id: "my-dataset-id",
  to_dimension_references: [ /* ... */ ],
  mapping_plan: {
    dataset_id: "my-dataset-id",
    mappings: [
      {name: "model_year", persist: true},
      {name: "county", persist: true},
    ],
  },
  // ...
}
```

If the query fails partway through, dsgrid saves a checkpoint file. Resume from
the checkpoint:

```bash
dsgrid query dataset run dataset_query.json5 -o query_output/ \
    --checkpoint-file __dsgrid_scratch__/tmp_checkpoint.json
```

See the [Map a Dataset tutorial](map_dataset) for more details on mapping plans
and checkpoint workflows.

## Next Steps

- Learn [Dataset Query Concepts](../dataset_mapping/dataset_query_concepts) for
  the full model reference
- Explore [Dimension Mapping Types](../dataset_mapping/dimension_mapping_types)
  to understand how different mapping types transform data
- See [Map a Dataset to a Project](map_dataset) for project-based mapping
- Review [Apache Spark performance tuning](../apache_spark/overview.md#troubleshooting-configuration-problems)
  for large datasets
