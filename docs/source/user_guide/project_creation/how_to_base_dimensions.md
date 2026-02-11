# How to Create Project Base Dimensions

Base dimensions define the fundamental dimensional structure of a dsgrid project. They represent the common dimensional framework that all datasets in the project must map to.

## Overview

Base dimensions typically include:

- **Geography**: Spatial units (e.g., counties, states, regions)
- **Sector**: Economic sectors (e.g., residential, commercial, industrial, transportation)
- **Subsector**: More detailed sector breakdowns (e.g., building types)
- **Metric**: Measured quantities (e.g., energy consumption, end uses)
- **Time**: Temporal resolution (e.g., hourly, annual)
- **Model Year**: Years for which data is modeled or projected
- **Weather Year**: Years representing weather patterns
- **Scenario**: Different modeling scenarios or cases

## Steps

### 1. Analyze Dataset Dimensions

Review the dimensions from all datasets that will be included in the project:

- Identify common dimensional structures across datasets
- Determine the finest granularity needed for each dimension type
- Consider future datasets that may be added

### 2. Define Base Dimension Records

For each dimension type, create a comprehensive set of base records:

- Use the most detailed granularity required by any dataset
- Ensure complete coverage of all dimension values
- Follow standardized naming conventions
- Include metadata (names, descriptions, additional attributes)

### 3. Create Dimension Configuration Files

Define base dimensions in the project config:

```javascript
dimensions: {
  base_dimensions: [
    {
      "class": "County",
      type: "geography",
      name: "ACS County 2020",
      file: "dimensions/base/counties.csv",
      description: "American Community Survey US counties, 2020.",
    },
    {
      "class": "Sector",
      type: "sector",
      name: "Sectors",
      file: "dimensions/base/sectors.csv",
      description: "Primary economic sectors.",
    },
    // ... more base dimensions
  ],
}
```

### 4. Create Record Files

Create CSV files with dimension records:

```text
id,name,description
res,Residential,"Residential sector"
com,Commercial,"Commercial sector"
ind,Industrial,"Industrial sector"
trans,Transportation,"Transportation sector"
```

### 5. Validate Base Dimensions

Ensure that:

- All datasets can map to the base dimensions
- Records are mutually exclusive and collectively exhaustive
- Dimension hierarchies are well-defined

## Best Practices

- **Start comprehensive**: Include all records needed by any dataset
- **Use consistent IDs**: Maintain stable, meaningful identifiers
- **Document thoroughly**: Provide clear descriptions for all records
- **Version carefully**: Track changes to dimension definitions
- **Coordinate with data providers**: Ensure datasets can map to your base dimensions

## Next Steps

- Learn about [supplemental dimensions](how_to_supplemental_dimensions)
- Understand [project concepts](concepts) in detail
- Follow the [create project tutorial](../tutorials/create_project)
