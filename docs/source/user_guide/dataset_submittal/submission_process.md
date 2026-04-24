# Submission Process

Submitting a dataset to a project connects a registered dataset to a specific
project in the dsgrid registry. During submission, dsgrid registers any new
dimension mappings, identifies which project base dimension each dataset
dimension maps to, and validates that the mapped dataset covers all dimension
combinations the project requires. On success, the dataset's status within the
project changes from `UNREGISTERED` to `REGISTERED`.

This page covers the commands, mapping file formats, and end-to-end workflow. For
the validation checks that run during submission, see
[Submission Checks](submission_checks).

## Prerequisites

- The dataset is registered in the dsgrid registry (via
  `dsgrid registry datasets register`).
- The project is registered and the dataset is listed in the project's
  `datasets` configuration (with status `UNREGISTERED`).
- Dimension mappings have been created for every dimension type where the
  dataset's dimension differs from the project's base dimension. See
  [How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings).

If a dataset dimension is identical to the corresponding project base
dimension (same `dimension_id` and `version`), no mapping is needed for
that dimension type — dsgrid recognizes the match automatically.


## Commands

dsgrid provides two commands for submitting a dataset to a project. Both
perform the same validation; they differ in whether the dataset is already
registered.

### `submit-dataset` (dataset already registered)

Use this when the dataset has already been registered separately with
`dsgrid registry datasets register`.

```bash
dsgrid registry projects submit-dataset \
    -p <project-id> \
    -d <dataset-id> \
    -m dimension_mappings.json5 \
    -l "Submit dataset to project."
```

### `register-and-submit-dataset` (combined operation)

Use this to register and submit in a single transaction. If any step fails,
the entire operation rolls back — the dataset is not left partially registered.

```bash
dsgrid registry projects register-and-submit-dataset \
    -c dataset.json5 \
    -p <project-id> \
    -m dimension_mappings.json5 \
    -l "Register and submit dataset to project."
```

The `-D` / `--data-base-dir` option lets you specify a base directory for data
file paths if they are not relative to the config file location:

```bash
dsgrid registry projects register-and-submit-dataset \
    -c dataset.json5 \
    -p <project-id> \
    -m dimension_mappings.json5 \
    -D /path/to/data \
    -l "Register and submit dataset to project."
```

### Choosing between the two commands

```{list-table}
:header-rows: 1

* - Aspect
  - `submit-dataset`
  - `register-and-submit-dataset`
* - Dataset registration
  - Must be done beforehand
  - Happens automatically
* - Transaction scope
  - Submission only
  - Registration + submission
* - Rollback on failure
  - Only submission rolls back
  - Both registration and submission roll back
* - When to use
  - Iterating on submission after dataset is stable
  - First-time submission, or when you want an atomic operation
```

```{tip}
If you are iterating on dimension mappings and the dataset itself is already
correct, use `submit-dataset` to avoid re-registering the dataset on each
attempt.
```


## Providing Dimension Mappings

Dimension mappings tell dsgrid how to transform dataset dimensions into
project base dimensions. There are three ways to provide mappings at
submission time, controlled by the `-m` and `-r` options.

### Option 1: `-m` with new mappings (recommended for `register-and-submit-dataset`)

When using `register-and-submit-dataset`, the `-m` / `--dimension-mapping-file`
option accepts a JSON5 file that references CSV mapping record files. dsgrid
registers these mappings automatically during submission.

The file must match the
[DatasetBaseToProjectMappingTableListModel](../../software_reference/data_models/dimension_mapping_model)
schema:

```json5
{
  mappings: [
    {
      description: "County to project county mapping",
      dimension_type: "geography",
      file: "dimension_mappings/county_to_county.csv",
      mapping_type: "many_to_one_aggregation",
      // Required only if the project has multiple base dimensions
      // of this type:
      // project_base_dimension_name: "US Counties 2020 L48",
    },
    {
      description: "Model year interpolation",
      dimension_type: "model_year",
      file: "dimension_mappings/model_year_to_model_year.csv",
      mapping_type: "many_to_many_explicit_multipliers",
    },
  ],
}
```

Each entry specifies:

- **`dimension_type`** — The dimension type being mapped (e.g., `geography`,
  `model_year`).
- **`file`** — Path to a CSV file with `from_id`, `to_id`, and optionally
  `from_fraction` columns. Paths are relative to the mapping config file.
- **`mapping_type`** — The mapping operation. See
  [Dimension Mapping Types](../dataset_mapping/dimension_mapping_types).
- **`project_base_dimension_name`** — Required only when the project has
  multiple base dimensions of the same type.

dsgrid identifies the source dimension (from the dataset) and target dimension
(from the project) automatically based on the `dimension_type`.

### Option 2: `-m` with full mapping definitions (for `submit-dataset`)

When using `submit-dataset`, the `-m` option accepts a JSON5 file with fully
specified mappings that include explicit `from_dimension` and `to_dimension`
references. dsgrid registers these mappings during submission.

The file must match the
[DimensionMappingsConfigModel](../../software_reference/data_models/dimension_mapping_model)
schema:

```json5
{
  mappings: [
    {
      description: "County to project county mapping",
      file: "dimension_mappings/county_to_county.csv",
      mapping_type: "many_to_one_aggregation",
      from_dimension: {
        type: "geography",
        dimension_id: "<dataset-geography-uuid>",
        version: "1.0.0",
      },
      to_dimension: {
        type: "geography",
        dimension_id: "<project-geography-uuid>",
        version: "1.0.0",
      },
    },
  ],
}
```

Use `dsgrid registry dimensions list` to find the `dimension_id` values for
your dataset and project dimensions.

### Option 3: `-r` with pre-registered mapping references

If the mappings have already been registered (e.g., via
`dsgrid registry dimension-mappings register`), use `-r` /
`--dimension-mapping-references-file` to reference them by ID:

```json5
{
  references: [
    {
      from_dimension_type: "geography",
      to_dimension_type: "geography",
      mapping_id: "<mapping-uuid>",
      version: "1.0.0",
    },
  ],
}
```

```{note}
The `-m` and `-r` options are mutually exclusive. Use one or the other.
```


### When no mappings are needed

If every dataset dimension is identical to the corresponding project base
dimension (same registered dimension), you can omit both `-m` and `-r`.
dsgrid will detect that no transformation is needed.


## Auto-Generated Reverse Supplemental Mappings

The `-a` / `--autogen-reverse-supplemental-mappings` option handles a special
case: when a dataset's dimension is actually one of the project's
*supplemental* dimensions (not a base dimension) and the project already has a
base-to-supplemental mapping registered for that dimension type.

For each dimension type listed with `-a`, dsgrid checks whether the dataset's
dimension matches a project supplemental dimension. If so and no explicit
mapping is provided, dsgrid automatically creates and registers a reverse
mapping (supplemental → base) so the dataset can be mapped to the project's
base dimension.

```bash
dsgrid registry projects submit-dataset \
    -p my-project -d my-dataset \
    -a subsector -a metric \
    -l "Submit with auto-generated reverse mappings"
```

This is most useful when a dataset's subsector or metric dimension uses
categories that match a project's supplemental (aggregated) view of those
dimensions.


## Understanding Project Requirements

Before submitting, it is important to understand what dimension records the
project expects from your dataset. This is controlled by the
`required_dimensions` field in the project config's `datasets` entry for your
dataset.

### Where to find requirements

Look at the project config file (typically `project.json5` in the project's
config repository). Find your dataset in the `datasets` list and examine its
`required_dimensions` field. You can also use the CLI:

```bash
# Dump the project config to inspect it locally
dsgrid registry projects dump <project-id> -d output_dir/
```

### How requirements work

The `required_dimensions` field has two sections:

**`single_dimensional`** — Specifies required records for individual dimension
types. For example, if the project has residential, commercial, and
transportation sectors but your dataset only covers transportation:

```javascript
required_dimensions: {
  single_dimensional: {
    sector: {
      base: {record_ids: ["transportation"]},
    },
  },
}
```

**`multi_dimensional`** — Specifies required records as combinations across
dimension types. Use this when only specific cross-dimensional combinations are
required (rather than the full cross-join):

```javascript
required_dimensions: {
  multi_dimensional: [
    {
      sector: {base: {record_ids: ["commercial"]}},
      subsector: {base: {record_ids: ["office", "retail"]}},
    },
  ],
}
```

### Key conventions

- **`record_ids: ["__all__"]`** — The dataset must provide all records from
  the project's base dimension of that type. This is the default when no
  requirement is specified.
- **`base_missing`** — Lists records the dataset does *not* need to provide.
  dsgrid computes the required set as the base dimension records minus the
  `base_missing` records.
- **`subset`** — References a project subset dimension to define requirements.

See the [InputDatasetModel](../../software_reference/data_models/project_model.md#inputdatasetmodel)
and [RequiredDimensionsModel](../../software_reference/data_models/project_model.md#requireddimensionsmodel)
reference for the full specification.


## What Happens During Submission

Submission proceeds through these stages:

1. **Status check** — dsgrid confirms the project is registered and the
   dataset's status is `UNREGISTERED`.
2. **Register mappings** — If a mapping file is provided (`-m`), dsgrid
   registers the new dimension mappings.
3. **Validate mapping references** — If a references file is provided (`-r`),
   dsgrid confirms each referenced mapping exists in the registry.
4. **Auto-generate reverse mappings** — If `-a` is specified, dsgrid creates
   reverse supplemental mappings as needed.
5. **Identify base dimensions** — dsgrid determines which project base
   dimension each dataset dimension maps to. See
   [Base Dimension Identification](submission_checks.md#base-dimension-identification).
6. **Completeness check** — dsgrid verifies the mapped dataset covers all
   required dimension combinations. See
   [Dataset-to-Project Mapping Completeness](submission_checks.md#dataset-to-project-mapping-completeness).
7. **Update project config** — On success, dsgrid records the mapping
   references and base dimension names in the project config, sets the
   dataset's status to `REGISTERED`, and (if all datasets are now submitted)
   updates the project status to `COMPLETE`.

If any step fails, the transaction rolls back and the dataset remains
`UNREGISTERED`.


## Iterating on Failures

Submission failures are usually caused by mapping errors or missing dimension
combinations. The typical iteration cycle is:

1. **Run the submit command.**
2. **Read the error output.** Start with per-column errors (e.g., "missing
   values for column=county"), which appear first and are easiest to diagnose.
3. **If the cross-join check fails**, examine the generated Parquet file
   (`{dataset_id}__missing_dimension_record_combinations.parquet`) and the
   logged minimal patterns to identify which combinations are absent.
4. **Fix the mapping CSV** or update the dataset, then re-run.

See the [Troubleshooting](submission_checks.md#troubleshooting) section of
Submission Checks for common error messages and solutions.

```{tip}
When iterating, use `submit-dataset` rather than `register-and-submit-dataset`
to avoid re-registering the dataset each time. You only need to re-register
the dataset if the data or dimensions change.
```


## Next Steps

- [Submission Checks](submission_checks) — Detailed description of each validation
- [How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings) — Step-by-step mapping guide
- [Dimension Mapping Concepts](../dataset_mapping/dimension_mapping_concepts) — Config structure reference
- [Dimension Mapping Types](../dataset_mapping/dimension_mapping_types) — Type selection guide
- [Create and Submit a Dataset](../tutorials/create_and_submit_dataset) — End-to-end tutorial
- [Map a Dataset](../tutorials/map_dataset) — Post-submission mapping to project dimensions
