# Dimension Mapping Concepts

A dimension mapping is a data structure that describes how to transform data
from one representation for a given
[DimensionType](../../software_reference/data_models/enums.md#dimensiontype) to
another. Each mapping handles exactly one dimension type (e.g., geography or
model year); to remap multiple dimension types you define a separate mapping for
each. By defining such mappings, datasets can be remapped to a different
resolution for use by another team or tool or can be integrated into a unified
dsgrid project.

## Where Mappings Are Used

Dimension mappings appear in three dsgrid workflows:

1. **Dataset queries** — The `dsgrid query dataset` commands map a registered
   dataset to arbitrary target dimensions without involving a project. See
   [Dataset Query Concepts](dataset_query_concepts).
2. **Dataset submittal** — When submitting a dataset to a project, mappings
   align dataset dimensions to the project's base dimensions. See
   [Submission Checks](../dataset_submittal/submission_checks).
3. **Project queries** — The `dsgrid query project map-dataset` and
   `dsgrid query project run` commands apply mappings to transform data during
   query execution. See [Project Query Concepts](../project_queries/project_query_concepts).

The mapping structure is identical regardless of which workflow consumes it.

## How Mappings Work

A mapping can simply map one value to another or perform aggregations and
disaggregations. It can optionally apply multipliers. Specific behavior is
determined by the `mapping_type` field.

Common mapping types include:
- **Many-to-one aggregation** — Multiple source values map to one target (e.g., counties → states)
- **One-to-many disaggregation** — One source value splits to multiple targets (e.g., states → counties)
- **Many-to-many with multipliers** — Complex transformations with explicit fractions

See [Dimension Mapping Types](dimension_mapping_types) for a complete guide to
all mapping types, their `from_fraction` requirements, and when to use each one.

## Examples

The [dsgrid-project-StandardScenarios repository](https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/dsgrid_project) contains datasets that you can use as examples.

:::{note}
The examples below are taken from project and dataset config files, which use a
pre-registration format that references dimensions by `name` or
`dimension_type` instead of `dimension_id`. When registering mappings
standalone with `dsgrid registry dimension-mappings register`, you must use
`dimension_id` (a UUID) as shown in [Mapping Config Structure](#mapping-config-structure) below.
:::

### Many-To-One Aggregation

The [project config](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/project.json5) defines a mapping of counties to states that can be used to aggregate energy use values. This is a base-to-supplemental dimension mapping. The file defines the mapping like this:

```json5
{
  description: 'Maps US Counties 2020 L48 to State',
  file: 'dimension_mappings/base_to_supplemental/lookup_county_to_state.csv',
  mapping_type: 'many_to_one_aggregation',
  from_dimension: {
    name: 'US Counties 2020 L48',
    type: 'geography',
  },
  to_dimension: {
    name: 'US States L48',
    type: 'geography',
  },
}
```

The first few lines of the records file looks like this:

```text
from_id,to_id
01001,AL
01003,AL
01005,AL
01007,AL
01009,AL
```

### Many-To-Many Explicit Multipliers

The [TEMPO dataset config](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/tempo/dimension_mappings.json5) defines an interpolation-based mapping for model years because the dataset only contains even years. When mapping to the project, dsgrid will use these records to interpolate the values for odd years.

The mapping config looks like this:

```json5
{
  description: "2010-2050 from interpolating for every other year and 0 for 2010-2017",
  dimension_type: "model_year",
  file: "dimension_mappings/model_year_to_model_year.csv",
  mapping_type: "many_to_many_explicit_multipliers",
}
```

A sample of the records file looks like this:

```text
from_id,to_id,from_fraction
2018,2017,0
2018,2018,1
2018,2019,0.5
2020,2019,0.5
2020,2020,1
```

## Mapping Config Structure

Dimension mappings are defined in a JSON5 config file containing a `mappings`
list. Each entry specifies the mapping type, source and target dimensions, and a
CSV file of records.

### Config file format

The top-level structure is a `mappings` list:

```json5
{
  mappings: [
    {
      description: "Maps US Counties 2020 L48 to State",
      file: "dimension_mappings/lookup_county_to_state.csv",
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
    // additional mappings ...
  ],
}
```

:::{note}
`dimension_id` is a UUID assigned by dsgrid when a dimension is registered.
Use `dsgrid registry dimensions list` to find IDs.
:::

Each mapping entry has these fields:

- **`mapping_type`** — The operation dsgrid applies during the mapping (e.g.,
  `many_to_one_aggregation`). See [Dimension Mapping Types](dimension_mapping_types)
  for all options.
- **`from_dimension`** / **`to_dimension`** — Identify the source and target
  dimensions by `type`, `dimension_id`, and `version`. Both must share
  the same
  [DimensionType](../../software_reference/data_models/enums.md#dimensiontype).
- **`file`** — Path to a CSV file containing the mapping records.
- **`description`** — Human-readable description of the mapping.

See the [Dimension Mapping Data Model](../../software_reference/data_models/dimension_mapping_model)
for the complete field specification, including optional tolerance fields.

### CSV records format

Mapping record files are CSV with these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `from_id` | Always | Source dimension record ID |
| `to_id` | Always | Target dimension record ID (empty string or omitted to drop a record) |
| `from_fraction` | Depends on type | Multiplier applied to the value; defaults to `1.0` when omitted |

Whether `from_fraction` is required and how its sums are validated depends on
the `mapping_type`. See [Dimension Mapping Types](dimension_mapping_types) for
the rules.

## Registering Mappings

Dimension mappings must be registered in the dsgrid registry before they can be
used in queries. There are two approaches:

**Standalone registration** — Register mappings independently so they can be
reused across projects and datasets:

```bash
dsgrid registry dimension-mappings register dimension_mappings.json5 \
    -l "Register county-to-state mapping"
```

**Bundled with project or dataset** — Mappings can also be registered
automatically as part of project registration or dataset submittal. See the
[Create and Submit a Dataset](../tutorials/create_and_submit_dataset) tutorial
for an example.

To inspect registered mappings:

```bash
# List all registered dimension mappings
dsgrid registry dimension-mappings list

# Show details of a specific mapping
dsgrid registry dimension-mappings show <mapping-id>

# Export a mapping config to a directory
dsgrid registry dimension-mappings dump <mapping-id> -d output_dir/
```

See [CLI Fundamentals](../../software_reference/cli_fundamentals) for general
CLI usage patterns.

## Next Steps

- See [Dimension Mapping Types](dimension_mapping_types) for the complete type reference
- Follow the [How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings) guide
- Read the [Dataset Query Tutorial](../tutorials/query_dataset) for a query-based mapping walkthrough
- See the [Dimension Mapping Data Model](../../software_reference/data_models/dimension_mapping_model) for complete specifications
