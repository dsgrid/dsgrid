# Query a Dataset

In this tutorial you will learn how to use `dsgrid query dataset` to map a
registered dataset to alternate dimensions. The example maps county-level
energy data with detailed subsectors to state-level and a coarser subsector
grouping in a single query.

This tutorial assumes that:

- You have a registered dataset in a dsgrid registry.
- You have populated your `~/.dsgrid.json5` with the registry location.
- The required dimension mappings are registered (or you will register them).

If you need to create and register a dimension mapping, follow
[How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings) first.

## Starting and Target Dimensions

The dataset to be aggregated uses a **county** geography dimension whose records look like this:

```{csv-table} County dimension records (counties.csv)
:header-rows: 1

id,name,state,time_zone
06073,San Diego County,CA,America/Los_Angeles
06075,San Francisco County,CA,America/Los_Angeles
48141,El Paso County,TX,America/Chicago
48201,Harris County,TX,America/Chicago
36001,Albany County,NY,America/New_York
36119,Westchester County,NY,America/New_York
08031,Denver County,CO,America/Denver
08059,Jefferson County,CO,America/Denver
```

The target **state** dimension has these records:

```{csv-table} State dimension records (states.csv)
:header-rows: 1

id,name,is_conus,census_division,census_region
CA,California,True,pacific,west
CO,Colorado,True,mountain,west
TX,Texas,True,west_south_central,south
NY,New York,True,mid_atlantic,northeast
```

A **many-to-one aggregation** mapping connects the two. Each county maps to
exactly one state with a fraction of `1` (the full value is assigned to the
target):

```{csv-table} County-to-state mapping (lookup_county_to_state.csv)
:header-rows: 1

from_id,from_fraction,to_id
06073,1,CA
06075,1,CA
08031,1,CO
08059,1,CO
36001,1,NY
36119,1,NY
48141,1,TX
48201,1,TX
```

The dataset also has a **subsector** dimension with detailed building types:

```{csv-table} Subsector dimension records (subsectors.csv — first rows)
:header-rows: 1

id,name
FullServiceRestaurant,Full Service Restaurant
Hospital,Hospital
LargeHotel,Large Hotel
LargeOffice,Large Office
SmallOffice,Small Office
Warehouse,Warehouse
```

A second mapping aggregates all commercial building types into a single
`commercial_subsectors` category:

```{csv-table} Subsector mapping (lookup_subsectors_to_commercial_subsectors.csv — first rows)
:header-rows: 1

from_id,to_id
FullServiceRestaurant,commercial_subsectors
Hospital,commercial_subsectors
LargeHotel,commercial_subsectors
LargeOffice,commercial_subsectors
SmallOffice,commercial_subsectors
Warehouse,commercial_subsectors
```

## Step 1: Identify Source and Target Dimensions

Start by inspecting the dataset's current dimensions:

```bash
dsgrid registry datasets list
dsgrid registry datasets dump <dataset-id>
```

The `list` command shows all registered datasets. The `dump` command writes a
dataset's full config (including its dimension references) to a local file so
you can inspect it.

Identify which dimensions you want to remap. For this example, we want to:

1. Aggregate **geography** from counties to states.
2. Aggregate **subsector** from individual building types to a coarser grouping.

Check what target dimensions and mappings are available:

```bash
dsgrid registry dimensions list -f "dimension_type == geography"
dsgrid registry dimensions list -f "dimension_type == subsector"
```

Note the `dimension_id` (a UUID) and `version` of each target dimension.

## Step 2: Verify Mappings Exist

Confirm that mappings exist between the dataset's current dimensions and each
target:

```bash
dsgrid registry dimension-mappings list
```

Look for mappings whose `from_dimension` matches the dataset's geography /
subsector and whose `to_dimension` matches the targets. If a mapping does not
exist, create and register one using
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
`version`. The example below maps **two** dimensions — geography and
subsector — in a single query:

```json5
{
  name: "my-state-aggregation",
  dataset_id: "my-dataset-id",
  to_dimension_references: [
    {
      type: "geography",
      dimension_id: "<target-geography-uuid>",
      version: "1.0.0",
    },
    {
      type: "subsector",
      dimension_id: "<target-subsector-uuid>",
      version: "1.0.0",
    },
  ],
  result: {
    table_format: {
      format_type: "stacked",
    },
    output_format: "parquet",
    sort_columns: ["geography", "subsector", "timestamp"],
  },
}
```

:::{note}
You do not need to specify mapping IDs. dsgrid finds registered mappings
between your dataset's dimensions and the target dimensions automatically.
:::

### What happens to dimensions not listed in `to_dimension_references`?

Dimensions that are **not** listed in `to_dimension_references` are kept at
their original resolution. They pass through the query unchanged — they are
**not** aggregated or collapsed.

For example, if the dataset has columns `geography`, `sector`, `subsector`,
`model_year`, and `end_use`, and you only map `geography` and `subsector`,
the output will still contain `sector`, `model_year`, and `end_use` at their
original granularity.

## Step 5: Run the Query

```bash
dsgrid query dataset run dataset_query.json5 -o query_output/
```

dsgrid will:

1. Load the dataset from the registry.
2. Resolve mappings between the dataset's dimensions and each target dimension.
3. Apply the mappings to transform the data.
4. Write results to `query_output/my-state-aggregation/`.

## Step 6: Inspect the Output

The output directory contains the mapped data:

```text
query_output/
└── my-state-aggregation/
    └── table.parquet
```

You can inspect the results with Python:

```python
import pandas as pd

df = pd.read_parquet("query_output/my-state-aggregation/table.parquet")
print(df.head())
print(f"Unique geographies: {df['geography'].nunique()}")
print(f"Unique subsectors: {df['subsector'].nunique()}")
```

Or with PySpark if the output is large:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("query_output/my-state-aggregation/table.parquet")
df.show()
df.select("geography").distinct().show()
df.select("subsector").distinct().show()
```

Because we mapped counties → states, the `geography` column now contains state
abbreviations (`CA`, `CO`, `NY`, `TX`, …) instead of FIPS codes. Similarly,
`subsector` now contains the coarser grouping rather than individual building
types. All other dimension columns remain at their original resolution.

## Using a Mapping Plan for Large Datasets

When `mapping_plan` is omitted (the default), dsgrid automatically determines
the mapping order. For most queries this is sufficient.

For large datasets, Spark may struggle to apply all mappings in a single pass.
A mapping plan lets you control the order in which dimension mappings are
applied and persist intermediate results between steps. Each entry in
`mappings` identifies a dimension by its registered **dimension name** (the
`name` field from the dimension config, not the dimension type). The order of
the list determines the order in which dsgrid applies the mappings.

```json5
{
  name: "my-state-aggregation",
  dataset_id: "my-dataset-id",
  to_dimension_references: [ /* ... */ ],
  mapping_plan: {
    dataset_id: "my-dataset-id",
    mappings: [
      {name: "Commercial Subsectors", persist: true},
      {name: "US States L48", persist: true},
    ],
  },
  // ...
}
```

Use `dsgrid registry dimensions list` to find the exact dimension names.

Setting `persist: true` causes dsgrid to write the intermediate table to disk
after that mapping step completes, which keeps the Spark query plan from growing
too large.

If a mapping operation has `persist: true` and the query fails on a later step,
you can resume from the last persisted checkpoint instead of starting over.
dsgrid writes checkpoint files to the scratch directory
(`__dsgrid_scratch__/` by default) with auto-generated names (e.g.,
`tmp_abc123.json`). Pass the most recent checkpoint to resume:

```bash
dsgrid query dataset run dataset_query.json5 -o query_output/ \
    -c __dsgrid_scratch__/<checkpoint-file>.json
```

:::{note}
Checkpoints require a mapping plan that persists intermediate tables.
:::

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
