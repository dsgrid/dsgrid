# Submission Checks

When you submit a dataset to a project with `dsgrid registry projects
submit-dataset`, dsgrid runs a series of validation checks to confirm that the
dataset's dimensions can be mapped onto the project's dimensions. This page
describes each check, the order in which they run, and what to do when one
fails.

## Check Sequence

Submission proceeds in this order:

1. **Project is registered** — the project must already exist in the registry.
2. **Dataset is unregistered in the project** — the dataset's status within the
   project must be `UNREGISTERED`. A dataset that has already been submitted
   cannot be submitted again.
3. **Mapping references exist** — every dimension mapping referenced in the
   submission (whether from a file or from explicit references) must already be
   registered in the dimension mapping registry.
4. **Base dimension identification** — dsgrid determines which project base
   dimension each dataset dimension maps to. When a project has multiple base
   dimensions for the same dimension type, additional disambiguation is
   required (see below).
5. **Dataset-to-project mapping completeness** — the mapped dataset must cover
   every dimension combination that the project expects.


## Project and Dataset Status

Project must be registered
: The project must exist in the registry. If it does not, dsgrid raises
  `DSGValueNotRegistered`.

Dataset must be unregistered
: The dataset's status within the project must be `UNREGISTERED`. If the
  dataset has already been submitted (status `REGISTERED`), dsgrid raises
  `DSGDuplicateValueRegistered`.

Dataset must be listed in the project
: The project configuration must include the dataset in its `datasets` list.
  If the dataset is not found, dsgrid raises a `ValueError`.


## Mapping References

When you supply dimension mappings (via `--dimension-mapping-file` or
`--dimension-mapping-references-file`), dsgrid validates them before
proceeding.

Mappings must be registered
: Every `mapping_id` / `version` pair referenced in the submission must
  already exist in the dimension mapping registry. If not, dsgrid raises
  `DSGValueNotRegistered`.

Multiple base dimensions require `project_base_dimension_name`
: If the project defines more than one base dimension for a given dimension
  type, the mapping file must include a `project_base_dimension_name` field to
  specify which base dimension to target. Otherwise dsgrid raises
  `DSGInvalidDimensionMapping`.


## Base Dimension Identification

After mappings are registered, dsgrid determines which project base dimension
each dataset dimension maps to. This affects how the data is stored and queried
within the project.

- If a mapping explicitly targets a project base dimension, that mapping is
  used.
- If no mapping is provided for a dimension type and the project has exactly
  one base dimension of that type, dsgrid uses it automatically.
- If no mapping is provided and the project has **multiple** base dimensions of
  the same type, dsgrid attempts to identify the correct one by checking
  whether the dataset's dimension records are a subset of each project base
  dimension's records. If the records match exactly one base dimension, dsgrid
  uses it. If the records match multiple base dimensions, dsgrid raises
  `DSGInvalidDataset` asking you to specify a mapping.
- Time dimensions always resolve to the single project time base dimension.


## Dataset-to-Project Mapping Completeness

This is the most substantial submission check. It verifies that, after applying
all dimension mappings, the dataset covers every dimension combination that the
project requires.

### How it works

1. **Build the project's expected dimension table** — dsgrid computes the
   cross-join of the project's base dimension records (excluding time) for the
   dimension types relevant to this dataset. Any missing-dimension-association
   tables declared for the dataset are subtracted from this expected set (after
   applying the same mappings).

2. **Build the mapped dataset dimension table** — dsgrid takes the dataset's
   actual dimension combinations (excluding time), applies the registered
   dimension mappings, and computes the distinct result.

3. **Per-column check** — For each dimension type, dsgrid compares the distinct
   values between the project table and the mapped dataset table. This catches
   cases where an entire dimension value is missing after mapping (for example,
   a county ID that the mapping does not produce) and gives a clear error
   message listing the missing values.

4. **Full cross-join check** — dsgrid performs an `except_all` between the
   project table and the mapped dataset table to find any dimension
   combinations present in the project but absent from the mapped dataset.

### When it fails

**Per-column mismatch** — If distinct values differ for any column, dsgrid logs
the missing values for each column and raises `DSGInvalidDataset`:

> "The mapped dataset has different distinct values than the project for
> column=*county*: diff={*06037*, *06038*}"

**Full cross-join mismatch** — If specific dimension *combinations* are
missing, dsgrid follows the same procedure as the registration dimension
association check:

- Writes the missing rows to
  `{dataset_id}__missing_dimension_record_combinations.parquet`.
- Runs the Rust-based `find_minimal_patterns` analysis and logs the top 10
  patterns.
- Raises `DSGInvalidDataset` with a pointer to the log file.

```{tip}
When diagnosing a submission failure, start with the per-column errors — they
appear first in the log and are easier to act on. If the per-column check
passes but the cross-join check fails, examine the Parquet file and the
logged patterns to identify which specific combinations are missing.
```

### Missing associations and mappings

Missing-dimension-association tables that were declared at registration time
are automatically accounted for during the submission check. dsgrid applies
the same dimension mappings to the missing-association tables before
subtracting them from the expected project table. You do not need to declare
them again.

### Skipping this check

Set the environment variable
`__DSGRID_SKIP_CHECK_DATASET_TO_PROJECT_MAPPING__` to any value.


## Environment Variable Reference

These environment variables are **only allowed in offline mode**. dsgrid will
refuse to start an online (cloud) submission if any of them are set.

```{list-table}
:header-rows: 1

* - Variable
  - Effect
* - `__DSGRID_SKIP_CHECK_DATASET_TO_PROJECT_MAPPING__`
  - Skips the dataset-to-project mapping completeness check.
```

```{warning}
Skipping the mapping check means a dataset could be submitted to a project
even though its mapped dimensions do not fully cover the project's
requirements. This can cause query-time errors or incomplete results.
```


## Troubleshooting

### Common error messages

"mapping_id=*xyz* is not registered"
: You referenced a dimension mapping that does not exist in the registry.
  Register the mapping first, or correct the mapping ID / version in your
  submission file.

"Dataset *X* cannot be submitted to project *Y* with status=REGISTERED"
: The dataset has already been submitted. If you need to re-submit, you must
  first remove the dataset from the project and change its status back to
  `UNREGISTERED`.

"The mapped dataset has different distinct values than the project"
: One or more dimension values expected by the project are not produced by the
  mapped dataset. Check the logged column-level differences to identify the
  specific values. Common causes: the dimension mapping is missing rows, or the
  dataset is missing records for certain dimension values.

"Dataset *X* is missing required dimension records"
: After mapping, the dataset does not contain all required dimension
  *combinations*. Open the generated Parquet file to see exactly which
  combinations are absent, and examine the logged patterns for clues.

"multiple project base dimensions for dimension type"
: The project has more than one base dimension of the same type and dsgrid
  cannot determine which one to use. Add a `project_base_dimension_name`
  field to the mapping entry for that dimension type.

"Bug: *dim_type* has multiple base dimensions … dsgrid could not discern which base dimension to use"
: Similar to the above, but dsgrid attempted automatic identification and
  failed because the dataset records were not a subset of any single project
  base dimension. Provide an explicit mapping.
