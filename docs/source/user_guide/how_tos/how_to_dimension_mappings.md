# How to Create Dimension Mappings

This guide walks through the process of creating and registering dimension
mappings. The same process applies whether you are preparing mappings for
dataset submittal, a project query, or a standalone dataset query.

## Prerequisites

- [Install dsgrid](../../getting_started/installation) on your system
- Access a dsgrid registry
- Familiarity with [Dimension Mapping Concepts](../dataset_mapping/dimension_mapping_concepts)

## Steps

### 1. Identify Source and Target Dimensions

Determine which of your dataset's dimensions need to be mapped and what the
target dimensions should be.

**For dataset submittal**: The project config defines the target (base)
dimensions. Use the [registry browser](browse_registry) or project config
repository to inspect them.

**For dataset queries**: You choose the target dimensions yourself — for
example, mapping county-level data to state-level.

Compare each dimension type between source and target:

```bash
# List dimensions in the registry
dsgrid registry dimensions list

# Filter by type
dsgrid registry dimensions list -f "dimension_type == geography"
```

### 2. Check If Target Dimensions Exist

Search the registry for existing dimensions that match your target:

```bash
dsgrid registry dimensions list -f "dimension_type == geography"
```

If the target dimension already exists, note its `dimension_id` and `version`
for use in the mapping config's `to_dimension` field.

### 3. Register Target Dimensions (If Needed)

If the target dimension does not exist, create a dimension config and records
file, then register it:

```json5
// state_dimension.json5
{
  dimensions: [
    {
      "class": "State",
      type: "geography",
      name: "US States L48",
      file: "dimensions/states.csv",
      description: "US states in the lower 48.",
    },
  ],
}
```

```bash
dsgrid registry dimensions register state_dimension.json5 \
    -l "Register US states dimension"
```

### 4. Choose a Mapping Type

Select the appropriate `mapping_type` based on the relationship between source
and target records. See [Dimension Mapping Types](../dataset_mapping/dimension_mapping_types)
for the complete reference, including a decision tree.

Common choices:

- **`many_to_one_aggregation`** — Reducing granularity (e.g., counties → states)
- **`one_to_one`** — Renaming or subsetting records
- **`many_to_many_explicit_multipliers`** — Interpolation with custom weights

### 5. Create a CSV Records File

Create a CSV file with the mapping records. All mapping files have `from_id`
and `to_id` columns. Some mapping types also require a `from_fraction` column.

:::{note}
Every `from_id` and `to_id` value must be a valid record in the corresponding
registered dimension, but you do not need to list every record from those
dimensions. Source records omitted from `from_id` are excluded from the mapping.
Target records omitted from `to_id` are not populated by the mapping.
:::

**Example: County to state aggregation** (`many_to_one_aggregation`)

```text
from_id,to_id
01001,AL
01003,AL
01005,AL
06001,CA
06003,CA
```

**Example: Interpolation** (`many_to_many_explicit_multipliers`)

```text
from_id,to_id,from_fraction
2018,2017,0
2018,2018,1
2018,2019,0.5
2020,2019,0.5
2020,2020,1
```

:::{tip}
To drop source records that have no target equivalent, set `to_id` to an empty
value (leave the field blank after the comma).
:::

### 6. Write the Mapping Config

Create a JSON5 config file referencing the CSV records. Each `from_dimension`
and `to_dimension` identifies a registered dimension by `type`, `dimension_id`,
and `version`. Use `dsgrid registry dimensions list` to find the IDs.

:::{note}
`dimension_id` is a UUID assigned automatically when a dimension is registered
(e.g., `c5e76cb6-4537-4f17-9db9-1e7eeda55eb9`). It is not user-specified.
:::

```json5
{
  mappings: [
    {
      description: "County to state aggregation",
      file: "dimension_mappings/county_to_state.csv",
      mapping_type: "many_to_one_aggregation",
      from_dimension: {
        type: "geography",
        dimension_id: "<from-dimension-uuid>",
        version: "1.0.0",
      },
      to_dimension: {
        type: "geography",
        dimension_id: "<to-dimension-uuid>",
        version: "1.0.0",
      },
    },
  ],
}
```

### 7. Register the Mapping

```bash
dsgrid registry dimension-mappings register dimension_mappings.json5 \
    -l "Register county-to-state mapping"
```

:::{note}
If you are submitting a dataset to a project, mappings can be registered
automatically as part of the `register-and-submit-dataset` command. See the
[Create and Submit a Dataset](../tutorials/create_and_submit_dataset) tutorial.
:::

### 8. Verify

Confirm the mapping was registered:

```bash
# List registered mappings
dsgrid registry dimension-mappings list

# Show details
dsgrid registry dimension-mappings show <mapping-id>

# Export to inspect locally
dsgrid registry dimension-mappings dump <mapping-id> -d output_dir/
```

## Next Steps

- [Dimension Mapping Concepts](../dataset_mapping/dimension_mapping_concepts) — Config structure reference
- [Dimension Mapping Types](../dataset_mapping/dimension_mapping_types) — Type selection guide
- [Query a Dataset](../tutorials/query_dataset) — Use mappings in a dataset query
- [Map a Dataset to a Project](../tutorials/map_dataset) — Use mappings in a project query
