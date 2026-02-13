# How to Create Project Supplemental Dimensions

Supplemental dimensions provide alternative aggregations or views of the base dimensions, enabling flexible analysis without requiring datasets to be remapped.

## Overview

Supplemental dimensions allow users to:

- Query data at different levels of aggregation
- Group dimension records in domain-specific ways
- Provide multiple views of the same base data
- Support various analysis scenarios without data duplication

## Common Use Cases

- **Geographic aggregations**: Counties → States → Regions
- **Sector groupings**: Detailed subsectors → Collapsed categories
- **Metric combinations**: Individual end uses → Fuel types → Total energy
- **Time aggregations**: Hourly → Daily → Monthly → Annual

## Steps

### 1. Identify Analysis Requirements

Determine what aggregations users will need:

- Common query patterns
- Reporting requirements
- Analysis use cases
- Stakeholder needs

### 2. Design Supplemental Dimension Structure

For each supplemental dimension, define:

- The base dimension it aggregates from
- The target records and their relationships
- Mapping logic (one-to-one, many-to-one, or many-to-many)

### 3. Create Supplemental Dimension Configuration

Add supplemental dimensions to the project config:

```javascript
dimensions: {
  base_dimensions: [
    // ... base dimensions
  ],
  supplemental_dimensions: [
    {
      "class": "State",
      type: "geography",
      name: "States",
      file: "dimensions/supplemental/states.csv",
      description: "US states aggregated from counties.",
    },
    {
      "class": "SubsectorBySectorCollapsed",
      type: "subsector",
      name: "Subsectors by Sector Collapsed",
      file: "dimensions/supplemental/subsectors_collapsed.csv",
      description: "Simplified subsector groupings by sector.",
    },
  ],
}
```

### 4. Create Base-to-Supplemental Mappings

Define how base dimensions map to supplemental dimensions:

```javascript
dimension_mappings: {
  base_to_supplemental: [
    {
      description: "County to State aggregation",
      file: "dimension_mappings/county_to_state.csv",
      mapping_type: "many_to_one_aggregation",
      from_dimension: {
        type: "geography",
        name: "ACS County 2020",
      },
      to_dimension: {
        type: "geography",
        name: "States",
      },
    },
  ],
}
```

### 5. Create Mapping Record Files

Create CSV files that define the mappings:

```text
from_id,to_id
01001,AL
01003,AL
01005,AL
...
06001,CA
06003,CA
```

For many-to-many mappings with fractions:

```text
from_id,to_id,from_fraction
detailed_metric_1,aggregated_metric_A,0.6
detailed_metric_1,aggregated_metric_B,0.4
detailed_metric_2,aggregated_metric_A,1.0
```

## Mapping Types

### Many-to-One Aggregation

Multiple base records map to a single supplemental record (e.g., counties to states):

- **Use when**: Natural hierarchical aggregation exists
- **Example**: Geographic rollups, sector consolidation

### One-to-One

Direct correspondence between base and supplemental (e.g., renaming):

- **Use when**: Same granularity, different labeling
- **Example**: Alternative naming conventions

### Many-to-Many

Complex relationships with fractional allocations:

- **Use when**: Records split or combine in complex ways
- **Example**: Subsector disaggregations, fuel type groupings

## Best Practices

- **Minimize supplemental dimensions**: Only create what's truly needed
- **Ensure consistency**: Mappings should be logically sound
- **Document rationale**: Explain why each supplemental dimension exists
- **Test thoroughly**: Verify aggregations produce expected results
- **Version with base**: Keep supplemental dimensions aligned with base changes

## Example: Subsector Aggregation

Base dimension has detailed building types:

```text
id,name,sector
single_family,Single Family Home,res
multi_family_small,Small Multi-Family,res
multi_family_large,Large Multi-Family,res
office_small,Small Office,com
office_large,Large Office,com
```

Supplemental dimension provides simplified view:

```text
id,name,sector
residential_buildings,Residential Buildings,res
commercial_buildings,Commercial Buildings,com
```

Mapping file:

```text
from_id,to_id
single_family,residential_buildings
multi_family_small,residential_buildings
multi_family_large,residential_buildings
office_small,commercial_buildings
office_large,commercial_buildings
```

## Next Steps

- Learn about [base dimensions](how_to_base_dimensions)
- Understand [dimension mapping concepts](../dataset_mapping/concepts)
- Follow the [create project tutorial](../tutorials/create_project)
