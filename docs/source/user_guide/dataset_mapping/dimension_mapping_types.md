# Dimension Mapping Types

dsgrid supports 15 dimension mapping types that control how data values are
transformed when mapping between dimensions. Each type has specific rules about
the `from_fraction` column and how fractions are validated.

This page is the conceptual guide. For formal enum definitions and archetype
constraint flags, see:
- [DimensionMappingType](../../software_reference/data_models/enums.md#dimensionmappingtype)
- [DimensionMappingArchetype](../../software_reference/data_models/enums.md#dimensionmappingarchetype)

## Overview

Mapping types are grouped below by their validation behavior and typical use
cases. The groupings correspond to the
[archetype](../../software_reference/data_models/enums.md#dimensionmappingarchetype)
that dsgrid assigns automatically based on the `mapping_type`.

### Quick Reference

```{list-table}
:header-rows: 1

* - `mapping_type`
  - `from_fraction` required?
  - Fraction sum rule
  - Typical use
* - `one_to_one`
  - No (defaults to 1.0)
  - sum by `from_id` = 1
  - Renaming, down-selection
* - `many_to_one_aggregation`
  - No (defaults to 1.0)
  - sum by `from_id` = 1
  - Counties → states
* - `many_to_one_reassignment`
  - No (defaults to 1.0)
  - sum by `from_id` = 1
  - Reclassifying categories
* - `duplication`
  - No (defaults to 1.0)
  - None
  - Copying a value to multiple targets
* - `one_to_many_disaggregation`
  - Yes
  - sum by `from_id` = 1
  - Splitting state → counties by fraction
* - `many_to_many_aggregation`
  - Yes
  - sum by `from_id` = 1
  - Complex rollups with fractions
* - `many_to_many_disaggregation`
  - Yes
  - sum by `from_id` = 1
  - Complex splits with fractions
* - `many_to_one_assignment`
  - Yes
  - sum by `to_id` = 1
  - Weighted assignment to target
* - `one_to_many_assignment`
  - Yes
  - sum by `to_id` = 1
  - Distributing to targets (sum of weights per target = 1)
* - `many_to_many_assignment`
  - Yes
  - sum by `to_id` = 1
  - Complex weighted assignment to targets
* - `one_to_one_explicit_multipliers`
  - Yes
  - None
  - Scaling factor per record
* - `one_to_many_explicit_multipliers`
  - Yes
  - None
  - Scaling + splitting
* - `many_to_one_explicit_multipliers`
  - Yes
  - None
  - Scaling + aggregation
* - `many_to_many_explicit_multipliers`
  - Yes
  - None
  - Interpolation with explicit weights
```

## Standard Mappings

These types have an optional `from_fraction` column (defaults to 1.0) and
validate that `sum(from_fraction) = 1` when grouped by `from_id`. They are the
most common mapping types.

### `one_to_one`

Each source record maps to exactly one target record and vice versa. Use this
for renaming IDs, selecting a subset of records, or mapping between equivalent
dimensions with different naming conventions.

- No duplicate `from_id` values allowed
- No duplicate `to_id` values allowed
- Set `to_id` to empty to drop a source record

```text
from_id,to_id
old_metric_name,new_metric_name
deprecated_id,current_id
unwanted_record,
```

### `many_to_one_aggregation`

Multiple source records map to the same target record. Values are summed
(aggregated) during the mapping. This is the **most commonly used** mapping
type.

- No duplicate `from_id` values allowed (each source maps to exactly one target)
- Duplicate `to_id` values allowed (multiple sources can map to the same target)

**Example: County to state aggregation**

```text
from_id,to_id
01001,AL
01003,AL
01005,AL
06001,CA
06003,CA
```

### `many_to_one_reassignment`

Identical structure to `many_to_one_aggregation`. The distinction is semantic:
use `many_to_one_reassignment` when you are reclassifying records into
different categories rather than performing a hierarchical aggregation.

## Duplication

### `duplication`

One source record is copied to multiple target records. The same value is
duplicated for each target. No fraction sum validation is performed.

- Duplicate `from_id` values allowed
- No duplicate `to_id` values allowed

```text
from_id,to_id
national_total,state_A
national_total,state_B
national_total,state_C
```

## Disaggregation and Complex Fraction Mappings

These types **require** a `from_fraction` column and validate that
`sum(from_fraction) = 1` when grouped by `from_id`. Values are multiplied by
`from_fraction` during the mapping.

### `one_to_many_disaggregation`

One source record splits into multiple target records. The `from_fraction`
column specifies what share of the source value goes to each target.

- Duplicate `from_id` values allowed (one source maps to many targets)
- No duplicate `to_id` values allowed

**Example: State-level data split to counties by population share**

```text
from_id,to_id,from_fraction
CA,06001,0.054
CA,06003,0.001
CA,06005,0.008
```

### `many_to_many_aggregation`

Multiple sources map to multiple targets with fractions. Fractions must sum to
1 per `from_id`. Use when sources contribute fractionally to multiple targets
while also being aggregated.

### `many_to_many_disaggregation`

Similar to `many_to_many_aggregation` but used when the primary intent is
disaggregation — splitting sources across targets.

## Assignment Mappings

These types **require** a `from_fraction` column and validate that
`sum(from_fraction) = 1` when grouped by **`to_id`** (not `from_id`). This
ensures each target receives a complete allocation.

### `many_to_one_assignment`

Multiple sources are assigned to a single target with weights that sum to 1 per
target.

### `one_to_many_assignment`

One source distributes to multiple targets. The fractions sum to 1 per target
(each target's total allocation from all sources equals 1).

### `many_to_many_assignment`

Complex weighted assignment where fractions sum to 1 per target. Use when
multiple sources contribute to multiple targets and you need to ensure each
target is fully allocated.

## Explicit Multiplier Mappings

These types **require** a `from_fraction` column but perform **no fraction sum
validation**. The `from_fraction` values are treated as arbitrary scaling
factors or interpolation weights.

### `one_to_one_explicit_multipliers`

One-to-one mapping with an explicit scaling factor per record.

### `one_to_many_explicit_multipliers`

One source maps to multiple targets with explicit (unconstrained) multipliers.

### `many_to_one_explicit_multipliers`

Multiple sources map to one target with explicit multipliers.

### `many_to_many_explicit_multipliers`

Multiple sources map to multiple targets with explicit multipliers. This is
commonly used for **temporal interpolation** when a dataset contains data for a
subset of time periods and must be mapped to a full set.

**Example: Model year interpolation for even-year-only data**

The dataset has data for even years (2018, 2020, ...). The target dimension
requires every year. Odd years are interpolated as 50/50 blends of the
neighboring even years, and years before the data range get a multiplier of 0.

```text
from_id,to_id,from_fraction
2018,2017,0
2018,2018,1
2018,2019,0.5
2020,2019,0.5
2020,2020,1
2020,2021,0.5
2022,2021,0.5
2022,2022,1
```

## Choosing a Mapping Type

Use this decision tree:

1. **Are source and target dimensions the same granularity?**
   - Yes, renaming only → `one_to_one`
   - Yes, with a scaling factor → `one_to_one_explicit_multipliers`

2. **Are you reducing granularity (aggregating)?**
   - Simple hierarchical rollup → `many_to_one_aggregation`
   - Reclassifying into different categories → `many_to_one_reassignment`
   - With fractional contributions → `many_to_many_aggregation`

3. **Are you increasing granularity (disaggregating)?**
   - Splitting by known fractions → `one_to_many_disaggregation`
   - Complex splits → `many_to_many_disaggregation`

4. **Do you need to duplicate values?**
   - Copy to multiple targets → `duplication`

5. **Do you need unconstrained multipliers?**
   - Interpolation weights, growth factors → use the appropriate `*_explicit_multipliers` variant

6. **Do fractions need to sum to 1 per target?**
   - Weighted allocation to targets → use the appropriate `*_assignment` variant

## Next Steps

- Learn the [config structure and CSV format](dimension_mapping_concepts) for authoring mappings
- Follow the [How to Create Dimension Mappings](../how_tos/how_to_dimension_mappings) step-by-step guide
- See the [DimensionMappingType enum](../../software_reference/data_models/enums.md#dimensionmappingtype) for the formal definition
