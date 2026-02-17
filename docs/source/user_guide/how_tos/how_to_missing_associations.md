# How to Handle Missing Dimension Associations

Datasets may have missing dimension combinations (associations) — for example, a building model might not have data for certain geography-subsector combinations because those building types don't exist in those regions.

dsgrid validates that datasets provide data for all expected dimension combinations. When a dataset legitimately lacks data for certain combinations, you must explicitly declare these **missing associations**.

## Prerequisites

- A dataset config file ready for registration (see [Data File Formats](../dataset_registration/data_file_formats))
- Familiarity with [Dimension Concepts](../dataset_registration/dimension_concepts)

## Declaring Missing Associations

Specify missing associations in the `data_layout` section of your dataset config using the `missing_associations` field. This field accepts a list of paths to files or directories:

```javascript
data_layout: {
  table_format: "one_table",
  value_format: { format_type: "stacked" },
  data_file: { path: "load_data.parquet" },
  missing_associations: [
    "missing_associations.parquet",
    "additional_missing",
  ],
}
```

Each entry in the list can be:

1. **A single file** (CSV or Parquet) containing missing combinations
2. **A directory** containing multiple files, each for different dimension combinations

Paths can be absolute or relative. Relative paths are resolved relative to the dataset configuration file by default. Alternatively, a different base directory can be specified using the `--missing-associations-base-dir` (`-M`) CLI option.

## File Format

Missing association files can be in CSV or Parquet format. They should contain columns for dimension types (all types except time). Each row represents a combination of dimension records that legitimately has no data.

A file can contain any subset of the non-time dimension columns. During validation, dsgrid filters out rows from the expected associations that match the missing associations listed in the file.

Example `missing_associations.parquet` with all non-time dimensions:

```text
geography,sector,subsector,metric,model_year,weather_year
01001,com,large_hotel,heating,2020,2018
01001,com,warehouse,cooling,2020,2018
01003,com,large_hotel,heating,2020,2018
```

Example `missing_associations.csv` with only two dimensions:

```text
geography,subsector
01001,large_hotel
01001,warehouse
01003,large_hotel
```

In this case, all metrics, model years, and weather years are expected to be missing for these combinations of (geography, subsector).

### Directory Format

When using a directory, create separate files for different dimension combinations.

Example directory structure:

```
missing_associations/
├── geography__subsector.csv
├── geography__metric.csv
└── subsector__metric.parquet
```

Each file contains the relevant dimension columns:

```text
# geography__subsector.csv
geography,subsector
01001,large_hotel
01001,warehouse
```

## Iterative Workflow for Identifying Missing Associations

If you don't know which dimension combinations are missing in your dataset, dsgrid provides an iterative workflow to help you identify them.

### 1. Run registration without missing associations

Attempt to register your dataset without specifying `missing_associations`. If there are missing combinations, registration will fail:

```bash
dsgrid registry datasets register dataset.json5 -l "Register my dataset"
```

### 2. Review generated outputs

When registration fails due to missing associations, dsgrid:

- Writes a Parquet file named `<dataset_id>__missing_dimension_record_combinations.parquet` to the current directory. This file contains all missing dimension combinations. It can contain huge numbers of rows.
- Runs pattern analysis (via `find_minimal_patterns`) to identify the simplest column combinations that characterize the gaps. These patterns are logged to help you understand *why* data is missing. For example:

```
Pattern 1: geography | subsector = 01001 | large_hotel (150 missing rows)
Pattern 2: subsector = warehouse (3000 missing rows)
```

This tells you that all combinations involving county 01001 and large_hotel are missing, and all combinations involving warehouse are missing.

- Records these minimal patterns in a `./missing_associations/` directory, in dimension-specific combination files such as `geography__subsector.csv` and `sector__subsector.csv`.

### 3. Choose which output to use and revise as appropriate

You have several options for declaring missing associations:

- **Use the all-inclusive Parquet file**: Reference the generated `<dataset_id>__missing_dimension_record_combinations.parquet` file directly. This contains every missing combination but may be very large.

- **Use the per-dimension CSV files**: Reference the `./missing_associations/` directory containing the minimal pattern files. This is more compact and easier to review.

- **Create your own files**: Create custom CSV or Parquet files based on your understanding of the data. This gives you full control over what is declared as missing.

No matter which option you select, you may want to:

- Fix data errors revealed by the missing data analysis
- Remove rows corresponding to data errors that you fix
- Pick and choose or reorganize the information
- Combine multiple sources if needed

### 4. Re-run registration with missing associations

Add the `missing_associations` field to your `data_layout` pointing to the files or directories:

```javascript
data_layout: {
  table_format: "one_table",
  value_format: { format_type: "stacked" },
  data_file: { path: "load_data.parquet" },
  // Option 1: Use the all-inclusive Parquet file
  missing_associations: ["./my_dataset__missing_dimension_record_combinations.parquet"],

  // Option 2: Use the per-dimension directory
  // missing_associations: ["./missing_associations"],

  // Option 3: Combine multiple sources
  // missing_associations: [
  //   "./missing_associations",
  //   "./additional_missing.parquet",
  // ],
}
```

Run registration again. If successful, the missing associations will be stored in the registry alongside your dataset.

## Using Custom Base Directories

When registering a dataset, you can specify a custom base directory for resolving missing associations paths using `--missing-associations-base-dir` (or `-M`):

```bash
dsgrid registry datasets register dataset.json5 \
  -l "Register my dataset" \
  -M /path/to/missing/files
```

When this option is provided, any relative paths in the `missing_associations` list will be resolved relative to the specified directory instead of the dataset configuration file's directory.

You can combine this with `--data-base-dir` (or `-D`) for data files:

```bash
dsgrid registry datasets register dataset.json5 \
  -l "Register my dataset" \
  -D /path/to/data/files \
  -M /path/to/missing/files
```

These options are also available for the `register-and-submit-dataset` command:

```bash
dsgrid registry projects register-and-submit-dataset \
  -c dataset.json5 \
  -p my-project-id \
  -l "Register and submit dataset" \
  -D /path/to/data/files \
  -M /path/to/missing/files
```

## Validation Behavior

During dataset registration, dsgrid checks that:

1. All dimension combinations in the data files are valid (records match dimension definitions).
2. All expected combinations either have data or are declared as missing.

If dsgrid finds unexpected missing combinations, it will report an error and write the missing combinations to files as described above.
