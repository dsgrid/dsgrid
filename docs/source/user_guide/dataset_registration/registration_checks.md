# Registration Checks

When you register a dataset with `dsgrid registry datasets register`, dsgrid
runs a series of validation checks on your configuration and data. This page
describes each check, the order in which they run, and what to do when one
fails.

## Check Sequence

Registration proceeds in this order:

1. **Configuration validation** — Pydantic model validators fire when your
   configuration file is loaded.
2. **Duplicate registration** — dsgrid verifies the dataset is not already in
   the registry.
3. **Time consistency** — timestamps in the data are checked against the time
   dimension definition.
4. **Write to registry** — the dataset is written (in unpivoted format) to the
   registry store. This happens *before* the remaining checks so that they can
   operate on the canonical unpivoted form. If a later check fails, the written
   data is removed.
5. **Required dimensions** — dsgrid confirms every required dimension type is
   present (unless the project's `DatasetDimensionRequirements` opts out via
   `require_all_dimension_types`).
6. **Schema checks** — column-level validation of the data tables (value
   columns, data types, NULLs, and table-format-specific consistency).
7. **Dimension association completeness** — the cross-join of all non-time
   dimension records must be present in the data (minus any explicitly declared
   missing associations).


## Configuration Validation

These checks run automatically when your JSON/JSON5 configuration file is
parsed.

`dataset_id` format
: Must be a lowercase identifier containing only letters, digits, hyphens, and
  underscores. Leading digits and leading hyphens are not allowed.

Unique dimension filenames
: Every dimension file listed in the configuration must have a unique filename.

Unique dimension names
: Every dimension's `name` field must be unique within the dataset.

Time dimension is not trivial
: The time dimension type may not be "trivial" (the `NoOp` time type).

Layout field mutual exclusivity
: You may set `data_layout` (for initial registration) *or*
  `registry_data_layout` (internal), but not both.

Pivoted-format fields
: If `data_layout` is set to a pivoted format, `pivoted_dimension_type` must
  also be set.

Two Table format fields
: If you are using the Two Table format, the `load_data_lookup` field must
  point to the lookup file.


## Time Consistency

These checks run on the data in its original format (before unpivoting),
because checking timestamps on an unpivoted table with many value columns would
multiply the work.

Timestamp range
: The time range in the data must match the range declared in the time
  dimension's `ranges` field.

Uniform time arrays
: Every combination of non-time dimensions must have the same set of
  timestamps. dsgrid checks both the count and the content of each group's time
  array to ensure uniformity.

Model-year consistency (annual + historical)
: For datasets with annual time resolution and a
  `data_classification` of `historical`, every row's timestamp year must
  equal its `model_year` value.

Chronify-based checks
: When the time dimension supports the chronify library, dsgrid delegates
  validation to chronify, which performs its own range and completeness checks.

### Skipping time checks

Set the environment variable
`__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__` to any value, or set
`check_time_consistency: false` in the project's
`DatasetDimensionRequirements` for this dataset.


## Schema Checks

Schema checks verify the structure of the Parquet data tables. The exact checks
depend on whether you are using the One Table or Two Table format.

### One Table

Value column present
: The `value_column` declared in the configuration must exist in the load data
  table.

No unexpected columns
: The load data table may only contain dimension columns and the value column.
  Any extra columns cause an error.

Dimension columns are strings
: All dimension columns must have `StringType`.

No NULL dimension values
: Dimension columns may not contain NULL values.

### Two Table

All One Table checks apply to the joined table (load data joined with the
lookup table on the `id` column). In addition:

Lookup `id` column
: The lookup table must contain a column named `id`.

Lookup dimension columns are strings
: All dimension columns in the lookup table must have `StringType`.

No NULL values in lookup
: The lookup table may not contain NULL dimension values.

ID set consistency
: The set of `id` values in the load data table must exactly match the set of
  `id` values in the lookup table. dsgrid logs the specific differences when
  they do not match.

Missing dimensions warning
: If any expected dimension columns are absent from the lookup table, dsgrid
  logs a warning (but does not fail).

### Skipping schema checks

Set the environment variable `__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__` to
any value. This skips both the schema checks and the dimension association
check described next.


## Dimension Association Completeness

This check verifies that the data contains every required combination of
non-time dimension records.

### How it works

1. **Per-column check** — For each dimension type, dsgrid compares the distinct
   values in the data against the declared dimension records. This catches
   simple cases (a missing geography ID, for example) and produces clear error
   messages.

2. **Full cross-join check** — dsgrid computes the expected cross-join of all
   non-time dimension records, subtracts any rows listed in the dataset's
   missing-dimension-associations tables, and compares the result to the
   distinct dimension combinations actually present in the data.

### When it fails

If the data is missing required dimension combinations, dsgrid:

- Writes the missing rows to a Parquet file named
  `{dataset_id}__missing_dimension_record_combinations.parquet` in the current
  working directory.
- Runs the Rust-based `find_minimal_patterns` analysis on the missing rows to
  identify the smallest sets of dimension values that explain the gaps. The top
  10 patterns are logged.
- Raises `DSGInvalidDataset` with a pointer to the log file for details.

```{tip}
The patterns output is the fastest way to diagnose the problem. A pattern like
`county = 06037 (500 missing rows)` tells you that every combination involving
county 06037 is absent — likely the county ID is wrong or the county was
omitted from your data.
```

### Declaring expected missing associations

If your dataset intentionally omits certain dimension combinations (for
example, a technology that does not apply in certain states), you can declare
them as *missing dimension associations* so that dsgrid subtracts them before
checking. See {doc}`../how_tos/how_to_missing_associations` for details.

### Skipping this check

Set `check_dimension_associations: false` in the project's
`DatasetDimensionRequirements` for this dataset.
Alternatively, set `__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__`, which skips
both this check and the schema checks above.


## Environment Variable Reference

These environment variables are **only allowed in offline mode**. dsgrid will
refuse to start an online (cloud) registration if any of them are set.

```{list-table}
:header-rows: 1

* - Variable
  - Effect
* - `__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__`
  - Skips schema checks and dimension association completeness.
* - `__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__`
  - Skips time consistency checks.
* - `__DSGRID_SKIP_CHECK_NULL_DIMENSION__`
  - Skips the NULL-value check on dimension columns after mapping
    application.
```

```{warning}
These variables exist as escape hatches for situations where a check is failing
due to a known issue (such as intermittent Spark GC timeouts on very large
datasets). Skipping checks means invalid data can enter the registry.
```
