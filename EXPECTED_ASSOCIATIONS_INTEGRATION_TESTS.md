# Adding `expected_associations` to Integration Tests

This guide describes how to add test coverage for `expected_associations`
through the existing integration test infrastructure.

## Architecture overview

The test data pipeline has three layers:

1. **Test data files** — in the `dsgrid-test-data` submodule under `datasets/`
2. **Test dataset configs** — in `dsgrid-test-data` under
   `test_efs/dsgrid_project/datasets/modeled/`
3. **Test helper that wires them together** —
   `dsgrid/tests/make_us_data_registry.py`

The `test_efs_comstock` dataset is the primary vehicle. Its config already
declares `missing_associations` and its data directory already contains a
`missing_associations/` subdirectory with per-column-pair CSV files.

## Step-by-step

### 1. Create expected associations data files

In the `dsgrid-test-data` submodule, add an `expected_associations/` directory
alongside the existing `missing_associations/` directory:

```
dsgrid-test-data/datasets/test_efs_comstock/
├── load_data.csv
├── load_data_lookup.json
├── missing_associations/
│   ├── geography__subsector.csv
│   └── sector__subsector.csv
└── expected_associations/          # ← new
    └── sector__subsector.csv       # ← new
```

The file format is the same as `missing_associations` — a CSV (or Parquet)
whose columns are dimension-type names and whose rows are dimension-record IDs.
The file represents the dimension combinations that *should* be present.

A good starting point: take the full cross-join of the test data's dimension
records and **remove** the rows covered by `missing_associations`. You can
generate this from the existing test data:

```python
# Run from the repo root with the Spark env vars set
from dsgrid.utils.spark import create_dataframe_from_dicts
from dsgrid.spark.functions import get_spark_session, except_all

# Build the full cross-join of sector × subsector from the test dimension records
# Then subtract the rows from missing_associations/sector__subsector.csv
# Write the result to expected_associations/sector__subsector.csv
```

Alternatively, to test the **partial-column cross-join expansion** feature of
`_make_expected_dimension_association_table_from_user`, create a file with
*fewer* columns than the full set of non-time dimensions. The schema handler
will automatically cross-join the specified columns with the full records of
the omitted dimensions.

### 2. Declare `expected_associations` in the dataset config

Edit one of the config files in `dsgrid-test-data`:

**`test_efs/dsgrid_project/datasets/modeled/comstock/dataset.json5`** and
**`dataset_with_dimension_ids.json5`**:

```javascript
data_layout: {
  // ... existing fields ...
  expected_associations: [
    "../../../../../datasets/test_efs_comstock/expected_associations",
  ],
  missing_associations: [
    "../../../../../datasets/test_efs_comstock/missing_associations",
  ],
},
```

List `expected_associations` before `missing_associations` per our ordering
convention.

### 3. The `make_us_data_registry.py` helper already handles it

`update_dataset_config_paths()` in `dsgrid/tests/make_us_data_registry.py`
already has a block that resolves `expected_associations` paths from the
dataset data directory by stem. No changes needed there.

### 4. Update the `register_dataset` fixture in `test_datasets.py`

The fixture at `tests/test_datasets.py` ~line 108 rewrites paths after copying
test data. It currently handles `missing_associations` but not
`expected_associations`. Add the equivalent block:

```python
if "expected_associations" in ts and ts["expected_associations"]:
    ts["expected_associations"] = [str(dataset_path / "expected_associations")]
```

### 5. Update the CLI tests in `test_registry_management.py`

`test_register_dataset_with_data_base_dir` (line ~815) and
`test_register_and_submit_dataset_cli` (line ~897) set
`data["data_layout"]["missing_associations"]` to relative paths resolved via
`-A`. Add the equivalent for `expected_associations`:

```python
data["data_layout"]["expected_associations"] = ["expected_associations"]
```

### 6. Consider a negative test

In `test_datasets.py`, `test_invalid_load_data_lookup_missing_records` deletes
one of the `missing_associations` CSV files to verify that registration fails
when undeclared gaps exist. An analogous test for `expected_associations`
would:

- Provide `expected_associations` that **exclude** some combinations actually
  present in the data (extra rows), or that **include** combinations not
  present (missing rows).
- Assert that `DSGInvalidDataset` is raised with an appropriate message.

### 7. Commit submodule changes

Since the data files live in `dsgrid-test-data`, you'll need to commit and
push there first, then update the submodule pointer in the main repo:

```bash
cd dsgrid-test-data
git add datasets/test_efs_comstock/expected_associations/
git add test_efs/dsgrid_project/datasets/modeled/comstock/dataset.json5
git add test_efs/dsgrid_project/datasets/modeled/comstock/dataset_with_dimension_ids.json5
git commit -m "Add expected_associations test data for test_efs_comstock"

cd ..
git add dsgrid-test-data
```

## Key code paths exercised

Adding the above will exercise these production code paths during the normal
test suite:

| Module | Function | What it does |
|--------|----------|--------------|
| `dataset_config.py` | `load_from_user_path` | Resolves `expected_associations` paths |
| `dataset_registry_manager.py` | `_read_expected_associations_tables_from_user_path` | Reads CSV/Parquet files |
| `dataset_registry_manager.py` | `_read_associations_file` | Type-casts and loads |
| `dataset_registry_manager.py` | `_write_to_registry` | Persists to store, checks NULL-row conflict |
| `dataset_schema_handler_base.py` | `_make_expected_dimension_association_table_from_user` | Cross-join expansion |
| `dataset_schema_handler_base.py` | `_check_dimension_associations` | Uses expected as baseline instead of full cross-join |
| `data_store_interface.py` | `write_expected_associations_tables` / `read_expected_associations_tables` | Store I/O |

## Choosing test data carefully

The test data must be consistent: the combinations in
`expected_associations` minus `missing_associations` must exactly match the
non-NULL dimension combinations present in the data files. If they don't,
registration will fail — which is what you want for a negative test but not
for the happy path.

The simplest approach for the happy path is to use the full cross-join minus
the existing missing associations. This is equivalent to not having
`expected_associations` at all in terms of the final required set, but it
exercises the code path where expected associations are loaded, expanded,
and used instead of the computed cross-join.
