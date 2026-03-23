# Output Formats

Query results can be customized through parameters in `QueryResultParamsModel`, which controls table structure, file format, data organization, and post-processing. This guide explains how each parameter influences the output.

## Overview of QueryResultParamsModel

The `QueryResultParamsModel` class provides fine-grained control over how query results are formatted and stored. Parameters are passed when executing a query and determine:

- **Table structure** — how rows and columns are organized (stacked vs. pivoted)
- **File format** — CSV or Parquet output
- **Column naming** — dimension names vs. dimension types
- **Data filtering and sorting** — which rows appear and in what order
- **Time zone conversion** — localization to specific time zones
- **Aggregation behavior** — how dimensions are rolled up
- **Additional reports** — pre-defined analysis outputs

---

## Table Format: Stacked vs. Pivoted

The `table_format` parameter controls the overall structure of your result table.

### Stacked Format (Default)

```python
table_format: StackedTableFormatModel()
```

**Structure**: Long, narrow table with all dimensions as columns (plus one `value` column).

| geography | subsector | time | value |
|-----------|-----------|------|-------|
| CA | Industrial | 2020-01-01 | 1200.5 |
| CA | Industrial | 2020-01-02 | 1195.3 |
| CA | Residential | 2020-01-01 | 450.2 |
| TX | Industrial | 2020-01-01 | 2100.8 |

**Use when**: Working with time series analysis, filtering by multiple dimensions, or performing statistical calculations. This format is most practical for most analyses.

### Pivoted Format

```python
table_format: PivotedTableFormatModel(
    dimension_to_pivot: "subsector"  # column to spread across as column headers
)
```

**Structure**: Wider table where one dimension's records become column headers, with values spread across columns.

| geography | time | Industrial | Residential |
|-----------|------|-----------|------------|
| CA | 2020-01-01 | 1200.5 | 450.2 |
| CA | 2020-01-02 | 1195.3 | 448.7 |
| TX | 2020-01-01 | 2100.8 | 620.5 |

**Use when**: Comparing values across a dimension (e.g., subsector-to-subsector comparison), creating wide comparison matrices, or preparing data for spreadsheet analysis.

---

## Column Naming: Dimension Names vs. Types

The `column_type` parameter controls whether columns use actual dimension names or generic type labels.

### Dimension Names (Default)

```python
column_type: ColumnType.DIMENSION_NAMES
```

Columns use actual dimension names from your project: `geography`, `subsector`, `time`, `value`.

**Use when**: Reading results directly or performing ad-hoc analysis where dimension names provide context.

### Dimension Types

```python
column_type: ColumnType.DIMENSION_TYPES
```

Columns use generic type names: `geography_type`, `subsector_type`, `time_type`, `value`.

**Use when**: Registering query results as a derived dataset (required). This naming convention ensures compatibility with the dsgrid data registration system.

---

## File Format

The `output_format` parameter controls the output file format.

```python
output_format: "parquet"  # or "csv"
```

| Format | Advantages | Disadvantages | Use When |
|--------|-----------|---------------|----------|
| **parquet** (default) | Efficient compression, preserves data types, scalable to large files | Requires specialized tools to read | Results are large or will be processed further |
| **csv** | Universal, human-readable, opens in spreadsheet applications | Large file sizes, loses data type information | Results are small or for sharing with others |

---

## Replace IDs with Names

The `replace_ids_with_names` parameter controls whether dimension record IDs or names appear in results.

### Default Behavior (False)

```python
replace_ids_with_names: False
```

Results contain dimension record IDs:

| geography_id | subsector_id | value |
|-------------|------------|-------|
| 06001 | 110 | 1200.5 |
| 06002 | 110 | 950.3 |

### With Names Replacement (True)

```python
replace_ids_with_names: True
```

Results contain human-readable names:

| geography_name | subsector_name | value |
|--------|------------|-------|
| Alameda County | Oil and gas extraction | 1200.5 |
| Alpine County | Oil and gas extraction | 950.3 |

**Note**: This is purely cosmetic and doesn't affect data values. Choose based on whether you need human-readable output or IDs for downstream processing.

---

## Aggregation Control

The `aggregations` parameter and `aggregate_each_dataset` flag control how dimensions are rolled up in your results.

### Aggregating Dimensions

Use `aggregations` to roll up (group) dimensions:

```python
aggregations: [
    AggregationModel(
        dimension: "subsector",
        aggregation_function: "sum"
    ),
    AggregationModel(
        dimension: "time",
        aggregation_function: "mean"
    )
]
```

This removes `subsector` and `time` as columns and instead aggregates values across all subsectors (summing) and all times (averaging).

**Result**: Output has fewer columns (`geography` only, plus `value`), with rolled-up values.

### Aggregating Individual Datasets vs. Combined Result

The `aggregate_each_dataset` parameter determines when aggregation happens:

```python
aggregate_each_dataset: False  # Default: aggregate after combining datasets
aggregate_each_dataset: True   # Aggregate each dataset separately first
```

**When combining datasets with different dimensionality** (union followed by aggregation), set `aggregate_each_dataset: True` to avoid duplicate rows.

---

## Sorting Results

The `sort_columns` parameter controls the row order in your output:

```python
sort_columns: ["geography", "time"]
```

Results are sorted first by `geography`, then by `time`. This makes output easier to scan and analyze.

**Note**: Column order in the output follows dimension order, while sorting affects row order.

---

## Time Zone Conversion

The `time_zone` parameter converts timestamps to a specific time zone:

```python
time_zone: "America/New_York"      # Convert to specific IANA timezone
time_zone: "geography"              # Convert to each geography's time zone
time_zone: None                     # Keep original (no conversion)
```

When time zone conversion is applied:
- Timestamps are converted to the specified zone
- The time column becomes timezone-naive (no UTC offset embedded)
- A separate `time_zone` column records the timezone for each row

**Example**: If your data is in UTC and you set `time_zone: "America/Los_Angeles"`, timestamps shift 7-8 hours earlier (depending on DST).

---

## Filtering Results

The `dimension_filters` parameter applies post-processing filters to the query result:

```python
dimension_filters: [
    DimensionFilters(
        dimension: "geography",
        records: ["06001", "06003", "06005"]  # Only these counties
    ),
    DimensionFilters(
        dimension: "subsector",
        records: ["110", "120"]  # Only these subsectors
    )
]
```

Filters are applied **after** the query executes, removing rows that don't match. This is different from query-level filters (which reduce computation) but useful for extracting subsets from results.

---

## Reports

The `reports` parameter generates pre-defined analysis reports on the query result:

```python
reports: [
    ReportInputModel(report_name: "peak_load"),
    ReportInputModel(report_name: "seasonal_pattern")
]
```

Each report generates additional output files with specific analyses (e.g., peak load analysis, seasonal breakdowns). Available reports depend on your dsgrid installation.

---

## Complete Example

Here's a complete `QueryResultParamsModel` configuration:

```python
from dsgrid.query.models import (
    QueryResultParamsModel,
    AggregationModel,
    DimensionFilters,
    ColumnType,
    StackedTableFormatModel,
)

result_params = QueryResultParamsModel(
    # Table structure
    table_format=StackedTableFormatModel(),
    column_type=ColumnType.DIMENSION_NAMES,

    # Output format
    output_format="parquet",

    # Data presentation
    replace_ids_with_names=True,
    sort_columns=["geography", "time"],

    # Filtering and aggregation
    aggregations=[
        AggregationModel(
            dimension="subsector",
            aggregation_function="sum"
        )
    ],
    aggregate_each_dataset=False,
    dimension_filters=[
        DimensionFilters(
            dimension="geography",
            records=["06001", "06003"]
        )
    ],

    # Time zone conversion
    time_zone="America/Los_Angeles",

    # Additional analysis
    reports=[
        ReportInputModel(report_name="peak_load")
    ]
)
```

This configuration produces a Parquet file with:
- Stacked rows (long format)
- Subsectors aggregated (summed)
- Only California counties 06001 and 06003
- Timestamps converted to America/Los_Angeles
- A peak load report generated
- Columns sorted by geography then time
- Human-readable dimension names
