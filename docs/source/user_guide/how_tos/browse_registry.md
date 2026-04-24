# How to Browse the Registry

The dsgrid registry stores all registered projects, datasets, dimensions, and
dimension mappings. This guide shows how to explore the registry using the CLI.

## List All Registry Contents

To get an overview of everything in the registry:

```bash
dsgrid registry list
```

To list a specific resource type:

```bash
dsgrid registry projects list
dsgrid registry datasets list
dsgrid registry dimensions list
dsgrid registry dimension-mappings list
```

To browse a different registry, specify the database URL directly:

```bash
dsgrid -u sqlite:////projects/dsgrid/standard-scenarios.db registry list
```


## Filter List Output

All `list` commands accept `-f` / `--filter` flags. Filters are
case-insensitive and use the format `"Column operation value"`. Multiple
filters can be combined (all must match).

Supported operations: `==`, `!=`, `contains`, `not contains`

```bash
# Dimensions of a specific type
dsgrid registry dimensions list -f "Type == geography"

# Dimensions whose name mentions "county"
dsgrid registry dimensions list -f "Query Name contains county"

# Datasets by a specific submitter
dsgrid registry datasets list -f "Submitter == jdoe"

# Projects whose ID contains "efs"
dsgrid registry projects list -f "ID contains efs"

# Combine filters (both must match)
dsgrid registry datasets list -f "ID contains com" -f "Submitter == jdoe"
```

### Filterable columns by resource

```{list-table}
:header-rows: 1

* - Resource
  - Columns
* - Projects
  - ID, Version, Status, Datasets, Date, Submitter, Description
* - Datasets
  - ID, Version, Date, Submitter, Description
* - Dimensions
  - Type, Query Name, ID, Version, Date, Submitter, Description
* - Dimension Mappings
  - Type [From, To], ID, From ID, To ID, Version, Date, Submitter, Description
```


## Find a Project's Dimensions

To see what base, subset, and supplemental dimensions a project defines:

```bash
dsgrid registry projects list-dimension-names <project-id>
```

This prints dimension names organized by dimension type (geography, sector,
metric, etc.) and category (base, subset, supplemental).

To show only base dimensions (most useful for data submitters):

```bash
dsgrid registry projects list-dimension-names <project-id> \
    --exclude-subset --exclude-supplemental
```

Other filter flags: `--exclude-base`, `--exclude-subset`,
`--exclude-supplemental`.


## Inspect a Dimension's Records

Once you know a dimension's ID (from the `ID` column in a `list` command),
view its records:

```bash
dsgrid registry dimensions show <dimension-id>
```

This prints the dimension's metadata (type, name, description) followed by its
full records table. The default version is `1.0.0`; specify a different version
with `-v`:

```bash
dsgrid registry dimensions show <dimension-id> -v 2.0.0
```

```{tip}
`list-dimension-names` outputs dimension names, not IDs. To find the ID for a
specific dimension, filter the dimensions list by name:
`dsgrid registry dimensions list -f "Query Name contains <name>"`
```


## Inspect a Dimension Mapping

To see the records of a registered dimension mapping:

```bash
dsgrid registry dimension-mappings show <mapping-id>
```

This prints the mapping's from/to dimension metadata followed by the full
records table (`from_id`, `to_id`, and `from_fraction` if applicable).


## Export a Config for Local Inspection

The `dump` command exports a config file (and associated records for dimensions
and mappings) to a local directory:

```bash
# Export a project config (includes required_dimensions for each dataset)
dsgrid registry projects dump <project-id> -d output_dir/

# Export a dataset config
dsgrid registry datasets dump <dataset-id> -d output_dir/

# Export a dimension config and its records
dsgrid registry dimensions dump <dimension-id> -d output_dir/

# Export a dimension mapping config and its records
dsgrid registry dimension-mappings dump <mapping-id> -d output_dir/
```

All `dump` commands accept `-v` for a specific version (defaults to latest) and
`--force` to overwrite existing files.


## Common Workflows

### Data submitter: Understand project requirements

```bash
# 1. Find the project
dsgrid registry projects list

# 2. See its base dimension names
dsgrid registry projects list-dimension-names <project-id> \
    --exclude-subset --exclude-supplemental

# 3. Find the dimension ID for a specific name from step 2
dsgrid registry dimensions list -f "Query Name contains <name-from-step-2>"

# 4. Inspect the records of that dimension
dsgrid registry dimensions show <dimension-id>

# 5. Export the project config to see required_dimensions for your dataset
dsgrid registry projects dump <project-id> -d project_config/
```

### Data submitter: Find existing dimension mappings

```bash
# List all registered geography mappings
dsgrid registry dimension-mappings list -f "Type [From, To] contains geography"

# Inspect a specific mapping's records
dsgrid registry dimension-mappings show <mapping-id>

# Export it locally to use as a template
dsgrid registry dimension-mappings dump <mapping-id> -d mapping_output/
```

### Data submitter: Compare dataset dimensions to project dimensions

```bash
# Show your registered dataset's dimensions
dsgrid registry dimensions list -f "Query Name contains <your-dimension-name>"

# Show the project's base dimensions of the same type
dsgrid registry dimensions list -f "Type == geography"

# Inspect both to compare records
dsgrid registry dimensions show <your-dimension-id>
dsgrid registry dimensions show <project-dimension-id>
```

### Dataset mapper: Find target dimensions for a dataset query

```bash
# 1. Find your registered dataset
dsgrid registry datasets list

# 2. Export its config to see its current dimensions
dsgrid registry datasets dump <dataset-id> -d dataset_config/

# 3. Find a target dimension to map to (e.g., a state-level geography)
dsgrid registry dimensions list -f "Type == geography"

# 4. Inspect the target dimension's records
dsgrid registry dimensions show <target-dimension-id>

# 5. Check if a mapping already exists for this dimension type
#    (broad search — look for your dimension's ID in the From ID or To ID columns)
dsgrid registry dimension-mappings list -f "Type [From, To] contains geography"
```

### Project coordinator: Review existing dimensions when designing a project

```bash
# 1. See what dimensions are already registered
dsgrid registry dimensions list -f "Type == geography"
dsgrid registry dimensions list -f "Type == sector"

# 2. Inspect a candidate dimension's records
dsgrid registry dimensions show <dimension-id>

# 3. See how an existing project is structured (as a reference)
dsgrid registry projects list
dsgrid registry projects list-dimension-names <existing-project-id>
dsgrid registry projects dump <existing-project-id> -d reference_project/
```

### Project coordinator: Inspect a submitted dataset

```bash
# 1. Export the project config to see dataset-to-project mapping references
dsgrid registry projects dump <project-id> -d project_config/
# Look in dimension_mappings.dataset_to_project.<dataset-id> for mapping IDs

# 2. Inspect the mappings used for a specific dataset's submission
dsgrid registry dimension-mappings show <mapping-id-from-project-config>

# 3. Export a dataset config to review its dimensions and layout
dsgrid registry datasets dump <dataset-id> -d dataset_config/
```


## Next Steps

- [How to Create Dimension Mappings](how_to_dimension_mappings) — after identifying which dimensions differ
- [Submission Process](../dataset_submittal/submission_process) — submitting to a project
- [Dataset Query Concepts](../dataset_mapping/dataset_query_concepts) — running standalone dataset queries
- [CLI Reference](../../software_reference/cli_reference) — full command reference
