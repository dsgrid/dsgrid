# How to Filter a Query

dsgrid offers several ways to filter the result of a query. It is important to understand some dsgrid behaviors to get an optimal result. Please refer to [query concepts](../project_queries/query_concepts) for details.

The examples below show how to define the filters in JSON5 or Python, along with the equivalent SQL expression that dsgrid applies through its Ibis-based query backend.

All examples except `DimensionFilterBetweenColumnOperatorModel` assume that the dataframe being filtered is the dimension record table. `DimensionFilterBetweenColumnOperatorModel` assumes that the table is the load data dataframe with time-series information.

:::{note}
Whenever multiple filters are provided in an array, dsgrid performs an `and` across all filters.
:::

## Filter Types

### 1. Expression Filter

Filter the table where a dimension column matches an expression. This example filters the geography dimension by selecting only data where the county matches the ID `06037` (Los Angeles, CA). This is equivalent to `column == "06037"`. You can use any SQL expression.

:::{note}
All values for dimensions in the filters must be strings.
:::

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    operator: "==",
    value: "06037",
    filter_type: "expression",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    DimensionFilterExpressionModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        operator="==",
        value="06037",
        negate=False,
    ),
]
```
:::

:::{tab-item} SQL
```sql
WHERE geography = '06037'
```
:::

::::

### 2. Raw Expression Filter

Similar to the first but use a raw expression. `DimensionFilterExpressionModel` creates a string by inserting the input parameters and adding required quotes. `DimensionFilterExpressionRawModel` uses your exact value. This allows you to make a more complex, custom expression.

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    value: "== '06037'",
    filter_type: "expression_raw",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    DimensionFilterExpressionRawModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        value="== '06037'",
        negate=False,
    ),
]
```
:::

:::{tab-item} SQL
```sql
WHERE geography = '06037'
```
:::

::::

### 3. Column Operator Filter

Filter a table where the specified column matches the specified value(s) according to the given SQL operator. This is useful for cases where you want to match partial strings or use a list of possible values.

**Example: Filter by multiple values**

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    column: "id",
    operator: "isin",
    value: ["06037", "06073"],
    filter_type: "column_operator",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    DimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column="id",
        operator="isin",
        value=["06037", "06073"],
        negate=False,
    ),
]
```
:::

:::{tab-item} SQL
```sql
WHERE geography IN ('06037', '06073')
```
:::

::::

**Example: Pattern matching with LIKE**

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    column: "name",
    operator: "like",
    value: "%County",
    filter_type: "column_operator",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    DimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.GEOGRAPHY,
        dimension_name="county",
        column="name",
        operator="like",
        value="%County",
        negate=False,
    ),
]
```
:::

:::{tab-item} SQL
```sql
WHERE name LIKE '%County'
```
:::

::::

### 4. Supplemental Dimension Filter

Filter on supplemental dimension records. This example filters the metric supplemental dimension by fuel type.

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "metric",
    dimension_name: "end_uses_by_fuel_type",
    column: "fuel_id",
    operator: "isin",
    value: ["electricity", "natural_gas"],
    filter_type: "supplemental_column_operator",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    SupplementalDimensionFilterColumnOperatorModel(
        dimension_type=DimensionType.METRIC,
        dimension_name="end_uses_by_fuel_type",
        column="fuel_id",
        operator="isin",
        value=["electricity", "natural_gas"],
        negate=False,
    ),
]
```
:::

::::

### 5. Time-Based Filter

Filter data between two bounds of a time column, inclusive. This is useful for selecting data within specific time ranges.

::::{tab-set}

:::{tab-item} JSON5
```javascript
dimension_filters: [
  {
    dimension_type: "time",
    dimension_name: "time_est",
    column: "timestamp",
    lower_bound: "2012-01-01 00:00:00",
    upper_bound: "2012-01-31 23:59:59",
    filter_type: "between_column_operator",
    negate: false,
  },
]
```
:::

:::{tab-item} Python
```python
dimension_filters=[
    DimensionFilterBetweenColumnOperatorModel(
        dimension_type=DimensionType.TIME,
        dimension_name="time_est",
        column="timestamp",
        lower_bound="2012-01-01 00:00:00",
        upper_bound="2012-01-31 23:59:59",
        negate=False,
    ),
]
```
:::

:::{tab-item} SQL
```sql
WHERE timestamp BETWEEN '2012-01-01 00:00:00' AND '2012-01-31 23:59:59'
```
:::

::::

## Common Operators

Filters with `filter_type: "column_operator"` (and `"supplemental_column_operator"`) accept
exactly these operators:

| Operator | Description | Example Value |
|----------|-------------|---------------|
| `contains` | Substring match | `"County"` |
| `startswith` | Prefix match | `"06"` |
| `endswith` | Suffix match | `"037"` |
| `like` | SQL pattern match | `"%County"` |
| `rlike` | Regex match | `"^06.*"` |
| `isin` | In list | `["06037", "06073"]` |
| `isNull` | Value is null | (value is ignored) |
| `isNotNull` | Value is not null | (value is ignored) |
| `between` | Between two values, inclusive | `["2020", "2050"]` |

Filters with `filter_type: "expression"` interpolate their operator into SQL, so comparison
operators such as `==`, `!=`, `>`, `>=`, `<`, and `<=` are written there instead.

## Negating Filters

Set `negate: true` to invert any filter. For example, to exclude specific counties:

```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    column: "id",
    operator: "isin",
    value: ["02013", "02016"],  // Alaska counties
    filter_type: "column_operator",
    negate: true,  // Exclude these counties
  },
]
```

## Combining Multiple Filters

When multiple filters are provided in an array, dsgrid applies them with an AND operation. To filter for California counties in years 2030-2050:

```javascript
dimension_filters: [
  {
    dimension_type: "geography",
    dimension_name: "county",
    column: "id",
    operator: "like",
    value: "06%",  // CA counties start with 06
    filter_type: "column_operator",
    negate: false,
  },
  {
    dimension_type: "model_year",
    dimension_name: "model_year",
    column: "id",
    operator: "between",
    value: ["2030", "2050"],
    filter_type: "column_operator",
    negate: false,
  },
]
```

## Best Practices

1. **Filter early**: Apply filters at the dataset level when possible to reduce data processing
2. **Use appropriate filter types**: Choose the most specific filter type for your use case
3. **Leverage supplemental dimensions**: Use supplemental dimension filters for complex aggregations
4. **Test incrementally**: Start with simple filters and add complexity
5. **Check dimension names**: Ensure dimension names match those defined in the project

## Next Steps

- Learn about [query concepts](../project_queries/query_concepts) for understanding query processing
- Follow the [query project tutorial](../tutorials/query_project)
- Explore the [CLI reference](../../software_reference/cli_reference) for command-line query options
