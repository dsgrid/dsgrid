# Define Dimension Associations

In this tutorial you will learn how to define **expected associations** and **missing associations** for a dsgrid dataset, using a commercial building energy dataset (ComStock) as a worked example.

By the end you will understand:

- How to decide whether to use expected associations, missing associations, or both.
- How to create the association files.
- How to reference them in a dataset config.

:::{note}
This tutorial walks through the reasoning behind each step. For a concise reference, see [How to Handle Dimension Associations](../how_tos/how_to_dimension_associations).
:::

## Prerequisites

- A dataset config file ready for registration (see [Dataset Concepts](../dataset_registration/dataset_concepts) and [Data File Formats](../dataset_registration/data_file_formats))
- Familiarity with [Dimension Concepts](../dataset_registration/dimension_concepts)

## Background: The Example Dataset

The example dataset (drawn from https://github.com/dsgrid/dsgrid-test-data) models a subset of commercial building energy loads for a subset of US counties. Its non-time dimensions are:

| Dimension   | Records |
|-------------|---------|
| geography   | 8 counties (e.g., Denver County CO, Harris County TX) |
| sector      | 2 sectors: `com` (Commercial), `res` (Residential) |
| subsector   | 14 commercial building types (e.g., `com__LargeHotel`, `com__Warehouse`) |
| metric      | 2 end uses: `com_cooling`, `com_fans` (pivoted as columns) |
| model_year  | 1 value: `2012` (trivial) |
| weather_year| 1 value: `2012` (trivial) |
| scenario    | 1 value: `reference` (trivial) |

dsgrid validates associations across all non-time dimensions that appear as rows in the data. **Pivoted** dimensions (here, metric — whose record IDs are data-table column names) and **trivial** dimensions (single-valued dimensions not stored in the data) are excluded. For this example, that leaves three dimensions for association validation: geography, sector, and subsector.

If every combination of these three dimensions were present, the full cross-join would produce 8 × 2 × 14 = 224 combinations. But not every combination makes sense — and the actual data are sparser than that. Our job is to tell dsgrid exactly which combinations are valid and which are legitimately absent.

## Step 1: Identify Which Combinations Are Valid

Before writing any association or config files, think about the relationships between dimensions in the data. Ask:

> Which dimensions are not fully independent? Which combinations of records do not exist in the data?

For ComStock, two relationships stand out:

1. **Sector–subsector relationship.** Of the 14 building types, 13 are commercial (`com`) and one, `com__MidriseApartment`, belongs to the `res` (residential) sector. The 13 commercial (`com`) subsectors have no data under `res`, and `com__MidriseApartment` has no data under `com`. This means 13 + 1 = 14 sector–subsector combinations are invalid out of the 28 in the full cross-join.

2. **Geography–subsector relationship.** Not every building type exists in every county. For example, San Francisco County (`06075`) has no quick-service restaurants in this test dataset, and Denver County (`08031`) is missing both quick-service restaurants and small hotels.

These two relationships are different in character — the first is structural (sector–subsector pairing is fixed) and the second is data-driven (which buildings happen to exist in a county). This affects how we declare them.

## Step 2: Choose Expected vs. Missing Associations

dsgrid gives you two complementary tools:

- **Expected associations** — declare which combinations *should* have data. Everything not listed is assumed missing.
- **Missing associations** — declare which combinations from the expected set *don't* have data. Everything not listed is assumed present.

Guidelines for choosing:

| Situation | Best tool |
|-----------|-----------|
| Most combinations are invalid (dataset is inherently sparse) | Expected associations |
| Most combinations are valid, with a few gaps | Missing associations |
| A structural relationship eliminates many combinations, plus a few edge-case gaps | Expected associations for the structure, then missing associations for the edge cases |

For ComStock, the sector–subsector relationship eliminates half the cross-join, so **expected associations** are the right choice for that relationship. The geography–subsector gaps are a handful of edge cases on top of that, so **missing associations** handle those.

## Step 3: Create the Expected Associations File

We need a file that lists every valid (sector, subsector) combination. Since these are the only two dimensions involved in this relationship, the file contains just those two columns. dsgrid will cross-join these combinations with the full set of records for every other validated dimension (here, geography).

Create a directory called `expected_associations/` alongside your data files and add a CSV:

**`expected_associations/sector__subsector.csv`**

```text
sector,subsector
com,com__LargeHotel
com,com__StripMall
com,com__QuickServiceRestaurant
com,com__MediumOffice
com,com__StandaloneRetail
com,com__FullServiceRestaurant
com,com__LargeOffice
com,com__SmallHotel
com,com__Warehouse
com,com__Hospital
com,com__SmallOffice
com,com__PrimarySchool
com,com__Outpatient
res,com__MidriseApartment
```

Notice the last row: `res,com__MidriseApartment`. Midrise apartments are modeled in ComStock (a commercial building stock tool) but reported under the residential sector. This is the kind of domain-specific detail that expected associations capture clearly.

:::{tip}
Name the file after the dimensions it contains (e.g., `sector__subsector.csv`). This is not required by dsgrid but makes the purpose self-documenting.
:::

:::{tip}
Datasets are not limited to a single expected associations file. If there are multiple structural relationships, feel free to express those in separate files. See the [how-to](../how_tos/how_to_dimension_associations) for how to reference multiple files in the dataset config.
:::

### What this achieves

Without expected associations, dsgrid would expect all 2 × 14 = 28 sector–subsector combinations. With this file, dsgrid expects only the 14 valid combinations, reducing the total from 8 × 28 = 224 to 8 × 14 = 112.

## Step 4: Attempt Registration with Only Expected Associations

At this point your dataset config references `expected_associations` but not `missing_associations`. Run registration:

```bash
dsgrid registry datasets register dataset.json5 -l "Register ComStock dataset"
```

Registration will fail because the data is missing some expected combinations. dsgrid writes two sets of output files:

1. **A single Parquet file** (`<dataset_id>__missing_dimension_record_combinations.parquet`) containing every missing combination. This is always generated, but can be very large.
2. **A directory of CSV files** (`./missing_associations/`) containing minimal patterns that characterize the gaps — for example, `geography__subsector.csv`. This structural format is produced by pattern analysis code written in Rust and is typically easier to review.

## Step 5: Review and Reference the Missing Associations

Look at the generated `missing_associations/geography__subsector.csv`:

```text
geography,subsector
06075,com__QuickServiceRestaurant
08031,com__QuickServiceRestaurant
08031,com__SmallHotel
36001,com__QuickServiceRestaurant
36119,com__SmallHotel
36119,com__Hospital
```

Each row says: "this building type legitimately has no data in this county." For example, the modeled building stock contains no quick-service restaurants in San Francisco County (`06075`), Denver County (`08031`), or Albany County (`36001`).

Confirm that the gaps are legitimate and not data bugs. If anything looks wrong, fix the underlying data and retry from Step 4.

Once you are satisfied the missing combinations are correct, move the generated files to a permanent location (or leave them in place) and add a `missing_associations` entry to your dataset config pointing to them:

```javascript
data_layout: {
  table_format: "two_table",
  value_format: "pivoted",
  pivoted_dimension_type: "metric",
  data_file: {
    path: "load_data.csv",
  },
  lookup_data_file: {
    path: "load_data_lookup.json",
  },
  expected_associations: [
    "expected_associations",
  ],
  missing_associations: [
    "missing_associations",
  ],
}
```

Both fields accept a list of paths. Each path can be a file or a directory. When a directory is given, dsgrid reads all CSV and Parquet files in it.

The missing associations file contains only the dimensions involved in the relationship. dsgrid applies these as filters across all other validated dimensions — since sector is the only other validated dimension and each subsector maps to exactly one sector, these 6 rows each remove exactly one combination from the expected set.

## Step 6: Re-run Registration

Run registration again:

```bash
dsgrid registry datasets register dataset.json5 -l "Register ComStock dataset"
```

dsgrid validates that the data contains exactly the expected combinations minus the declared missing combinations. If validation passes, both association declarations are stored in the registry alongside the dataset.

## How the Math Works

Here is a summary of how dsgrid computes the valid set of dimension combinations for this example. Only non-time, non-trivial, non-pivoted dimensions participate — in this case geography, sector, and subsector.

1. **Start from expected associations:** The `sector__subsector.csv` file defines 14 valid sector–subsector combinations. Because expected associations are provided, dsgrid uses these directly instead of building the full 2 × 14 = 28 cross-join of sector and subsector.

2. **Cross-join with remaining dimensions:** Geography is not covered by the expected associations file, so dsgrid cross-joins the 14 combinations with all 8 geographies: 14 × 8 = 112 expected combinations.

3. **Subtract missing associations:** The `geography__subsector.csv` file lists 6 geography–subsector combinations. Each removes exactly one row from the expected set, so 6 combinations are subtracted.

4. **Final expected count:** 112 − 6 = **106 valid combinations.** The data must contain exactly these 106 non-time dimension combinations.

## Key Takeaways

- **Think about dimension relationships first.** Before writing association files, identify which dimensions are not fully independent. This determines whether to use expected associations, missing associations, or both.
- **Use column subsets.** Association files don't need every dimension column. Include only the constrained dimensions — dsgrid cross-joins with the full set of records for omitted dimensions.
- **Expected associations define structure; missing associations handle edge cases.** Use expected associations when large portions of the cross-join are invalid. Layer missing associations on top for the remaining gaps.
- **The iterative workflow is your friend.** If you don't know what's missing, attempt registration and let dsgrid tell you. Review the output, fix any data bugs, and declare the legitimate gaps.
