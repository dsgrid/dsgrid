# Dataset Query Concepts

A **dataset query** remaps a registered dataset's dimensions without involving
a project. This is useful when you want to map a dataset to alternate
dimensions (e.g., aggregate county-level data to state-level) as a standalone
operation.

If you need to combine multiple datasets, apply result-level aggregations,
filters, or reports, use a **project query** instead. See
[Project Query Concepts](../project_queries/project_query_concepts).

## How It Works

A dataset query has three phases:

1. **Resolve mappings** — dsgrid looks up registered dimension mappings between
   the dataset's current dimensions and the target dimensions you specify. You
   do not supply mapping IDs directly — dsgrid finds them automatically using a
   graph traversal of the dimension mapping registry.

2. **Apply mappings** — dsgrid applies each mapping to transform the data.

3. **Write results** — The mapped data is written to the output directory in
   the specified format (Parquet or CSV).

## The DatasetQueryModel

A dataset query is defined by a JSON5 file with these fields:

```json5
{
  // Required
  name: "my-aggregated-dataset",
  dataset_id: "my-registered-dataset-id",
  to_dimension_references: [
    {
      type: "geography",
      dimension_id: "<target-dimension-uuid>",
      version: "1.0.0",
    },
  ],

  // Optional
  mapping_plan: null,  // leave null; see note below
  wrap_time_allowed: false,
  time_based_data_adjustment: {},

  // Result params (limited subset)
  result: {
    table_format: {format_type: "stacked"},
    output_format: "parquet",
    sort_columns: [],
    single_output_file: false,
  },
}
```

### `to_dimension_references`

This is the key field. Each entry identifies a target dimension in the registry
by its `type`, `dimension_id`, and `version`. dsgrid will find the registered
mapping between the dataset's current dimension and the specified target.

- At least one reference is required.
- You cannot specify two references of the same dimension type.
- If the target dimension is identical to the dataset's current dimension (same
  ID and version), dsgrid skips it — no mapping needed.

### `mapping_plan`

User-defined mapping plans (and the checkpoint files that depend on them) are
not yet supported for dataset queries. Leave `mapping_plan` as `null`; dsgrid
builds a default plan from the resolved mappings. Mapping plans are supported in
project queries; see the [Map a Dataset tutorial](../tutorials/map_dataset).

### `wrap_time_allowed`

When `true`, allows the dataset's time to be wrapped to the target time
dimension if the two differ.

### Supported Result Parameters

Dataset queries support a **limited subset** of the result parameters available
in project queries:

```{list-table}
:header-rows: 1

* - Parameter
  - Supported
  - Notes
* - `table_format`
  - Yes
  - Stacked or pivoted
* - `output_format`
  - Yes
  - `"parquet"` or `"csv"`
* - `sort_columns`
  - Yes
  - Sort result by these columns. On the Spark backend, the output file is
    only sorted if `single_output_file` is also `true`.
* - `single_output_file`
  - Yes
  - Write one file instead of a directory of part files. Only matters on
    Spark; DuckDB always writes a single file.
* - `aggregations`
  - **No**
  - Aggregation happens through mapping types
* - `dimension_filters`
  - **No**
  - Filter before querying or post-process results
* - `reports`
  - **No**
  - Use project queries for reports
* - `replace_ids_with_names`
  - **No**
  - Post-process results if needed
* - `time_zone`
  - **No**
  - Not available for dataset queries
```

:::{important}
Aggregation in a dataset query is achieved through the **dimension mapping
type** (e.g., `many_to_one_aggregation`), not through a result-level
aggregation parameter. This is the key difference from project queries, which
support explicit `aggregations` in the `result` block.
:::

## Mapping Resolution

When you specify a `to_dimension_references` entry, dsgrid resolves the
required mapping automatically:

1. It looks up the dataset's current dimension for that dimension type.
2. It searches the dimension mapping registry graph for the shortest chain of
   registered mappings from the dataset's dimension to the target dimension.
   This is usually a single mapping, but dsgrid can chain several (e.g.,
   county → state → census region). It uses the latest version of each mapping.
3. If a path exists, the mappings are applied in order. If not, the query fails.

This means the dimension mappings must already be registered before you run the
query. See [How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings).
Time is the exception: time conversion does not use registered mappings, so a
time reference only needs to identify the target time dimension.

## Comparison with Project Queries

```{list-table}
:header-rows: 1

* - Feature
  - Dataset Query
  - Project Query
* - Command
  - `dsgrid query dataset`
  - `dsgrid query project`
* - Requires project
  - No
  - Yes
* - Multiple datasets
  - No (single dataset)
  - Yes (combine with expressions)
* - Mapping target
  - Any registered dimension
  - Project base dimensions
* - Aggregation method
  - Through mapping types
  - Through `result.aggregations` or mapping types
* - Filters
  - Not supported
  - Supported (pre and post)
* - Reports
  - Not supported
  - Supported
* - Caching
  - Output only
  - Intermediate + output caching
```

## CLI Commands

### Generate a query template

```bash
dsgrid query dataset create-query <name> <dataset-id>
```

This writes a `dataset_query.json5` file with empty `to_dimension_references`
that you populate with the target dimensions.

### Run a query

```bash
dsgrid query dataset run dataset_query.json5 -o output/
```

Options:

- `-o`, `--output` — Output directory (default: `query_output`)
- `--overwrite` — Overwrite existing results

## Next Steps

- Follow the [Query a Dataset tutorial](../tutorials/query_dataset)
- Learn [Dimension Mapping Concepts](dimension_mapping_concepts) for config structure
- See the [DatasetQueryModel reference](../../software_reference/data_models/dataset_query_model) for
  the full data model
