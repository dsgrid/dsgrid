# Dataset Query

## DatasetQueryModel

*dsgrid.query.models.DatasetQueryModel*

Defines how to transform a dataset

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` | *(required)* | Name of query |
| `version` | `str` | `"0.1.0"` | Version of the query structure. Changes to the major or minor version invalidate cached tables. |
| `result` | [QueryResultParamsModel](dataset_query_model.md#queryresultparamsmodel) | `replace_ids_with_names=False aggregations=[] aggregate_each_dataset=False reports=[] column_type=<ColumnType.DIMENSION_NAMES: 'dimension_names'> table_format=StackedTableFormatModel(format_type=<ValueFormat.STACKED: 'stacked'>) output_format='parquet' sort_columns=[] dimension_filters=[] time_zone=None` | Controls the output results |
| `dataset_id` | `str` | *(required)* | Dataset ID for query |
| `to_dimension_references` | list[[DimensionReferenceModel](dimension_model.md#dimensionreferencemodel)] | *(required)* | Map the dataset to these dimensions. Mappings must exist in the registry. There cannot be duplicate mappings. |
| `mapping_plan` | [DatasetMappingPlan](dataset_query_model.md#datasetmappingplan) \| None | `None` | Defines the order in which to map the dimensions of the dataset. |
| `time_based_data_adjustment` | [TimeBasedDataAdjustmentModel](project_model.md#timebaseddataadjustmentmodel) | `leap_day_adjustment=<LeapDayAdjustmentType.NONE: 'none'> daylight_saving_adjustment=DaylightSavingAdjustmentModel(spring_forward_hour=<DaylightSavingSpringForwardType.NONE: 'none'>, fall_back_hour=<DaylightSavingFallBackType.NONE: 'none'>)` | Defines how the rest of the dataframe is adjusted with respect to time. E.g., when drop associated data when dropping a leap day timestamp. |
| `wrap_time_allowed` | `bool` | `False` | Whether to allow dataset time to be wrapped to the destination time dimension, if different. |

</div>


---

## AggregationModel

*dsgrid.query.models.AggregationModel*

Aggregate on one or more dimensions.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `aggregation_function` | `Any` | `None` | Must be a function name in pyspark.sql.functions |
| `dimensions` | [DimensionNamesModel](dataset_query_model.md#dimensionnamesmodel) | *(required)* | Dimensions on which to aggregate |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `check_aggregation_function` | `check_aggregation_function` | No description |
| `check_for_metric` | `check_for_metric` | No description |

</div>


---

## ColumnModel

*dsgrid.query.models.ColumnModel*

Defines one column in a SQL aggregation statement.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dimension_name` | `str` | *(required)* |  |
| `function` | `Any` | `None` | Function or name of function in pyspark.sql.functions. |
| `alias` | `str` \| None | `None` | Name of the resulting column. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `handle_function` | `handle_function` | No description |
| `handle_alias` | `handle_alias` | No description |

</div>


---

## DatasetMappingPlan

*dsgrid.query.dataset_mapping_plan.DatasetMappingPlan*

Defines how to map a dataset to a list of dimensions.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dataset_id` | `str` | *(required)* | ID of the dataset to be mapped. |
| `mappings` | list[[MapOperation](dataset_query_model.md#mapoperation)] | `[]` | Defines how to map each dimension of the dataset. |
| `apply_fraction_op` | [MapOperation](dataset_query_model.md#mapoperation) | `name='apply_fraction_op' handle_data_skew=False persist=False mapping_reference=None` | Defines handling of the query that applies the from_fraction value after mapping all dimensions. |
| `apply_scaling_factor_op` | [MapOperation](dataset_query_model.md#mapoperation) | `name='apply_scaling_factor_op' handle_data_skew=False persist=False mapping_reference=None` | Defines handling of the query that applies the scaling factor, if one exists. This happens after apply_fraction_op. |
| `convert_units_op` | [MapOperation](dataset_query_model.md#mapoperation) | `name='convert_units_op' handle_data_skew=False persist=False mapping_reference=None` | Defines handling of the query that converts units. This happens after apply_fraction_op and before mapping time. It is strongly recommended to not persist this table because the code currently always persists before mapping time. |
| `map_time_op` | [MapOperation](dataset_query_model.md#mapoperation) | `name='map_time' handle_data_skew=False persist=False mapping_reference=None` | Defines handling of the query that maps the time dimension. This happens after convert_units_op. Unlike the other dimension mappings, this does not use the generic mapping code. It relies on specific handling in chronify by time type. |
| `keep_intermediate_files` | `bool` | `False` | If True, keep the intermediate tables created during the mapping process. This is useful for debugging and benchmarking, but will consume more disk space. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `check_names` | `*(model)*` | No description |

</div>


---

## DimensionNamesModel

*dsgrid.query.models.DimensionNamesModel*

Defines the list of dimensions to which the value columns should be aggregated.
If a value is empty, that dimension will be aggregated and dropped from the table.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `geography` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `metric` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `model_year` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `scenario` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `sector` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `subsector` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `time` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |
| `weather_year` | list[`str` \| [ColumnModel](dataset_query_model.md#columnmodel)] | *(required)* |  |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `fix_columns` | `*(model)*` | No description |

</div>


---

## MapOperation

*dsgrid.query.dataset_mapping_plan.MapOperation*

Defines one mapping operation for a dataset.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` | *(required)* | Identifier for the mapping operation. This must be a unique name. |
| `handle_data_skew` | `bool` \| None | `None` | Use a salting technique to handle data skew in this mapping operation. Skew can happen when some partitions have significantly more data than others, resulting in unbalanced task execution times. If this value is None, dsgrid will make its own determination of whether this should be done based on the characteristics of the mapping operation. Setting it to True or False will override that behavior and inform dsgrid of what to do. This will automatically trigger a persist to the filesystem (implicitly setting persist to True). |
| `persist` | `bool` | `False` | Persist the intermediate dataset to the filesystem after mapping this dimension. This can be useful to prevent the query from becoming too large. It can also be useful for benchmarking and debugging purposes. |
| `mapping_reference` | [DimensionMappingReferenceModel](dimension_mapping_model.md#dimensionmappingreferencemodel) \| None | `None` | Reference to the model used to map the dimension. Set at runtime by dsgrid. |

</div>


---

## PivotedTableFormatModel

*dsgrid.dataset.models.PivotedTableFormatModel*

Defines a pivoted table format where one dimension's records are columns.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `format_type` | `Literal` | `"pivoted"` |  |
| `pivoted_dimension_type` | [DimensionType](enums.md#dimensiontype) | *(required)* | The dimension type whose records are columns that contain data values. |

</div>


---

## QueryResultParamsModel

*dsgrid.query.models.QueryResultParamsModel*

Controls post-processing and storage of CompositeDatasets

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `replace_ids_with_names` | `bool` | `False` | Replace dimension record IDs with their names in result tables. Project queries only; not supported in dataset queries. |
| `aggregations` | list[[AggregationModel](dataset_query_model.md#aggregationmodel)] | `[]` | Defines how to aggregate dimensions. Project queries only; dataset queries achieve aggregation through mappings to to_dimensions. |
| `aggregate_each_dataset` | `bool` | `False` | If True, aggregate each dataset before applying the expression to create one overall dataset. This parameter must be set to True for queries that will be adding or subtracting datasets with different dimensionality. Defaults to False, which corresponds to the default behavior of performing one aggregation on the overall dataset. WARNING: For a standard query that performs a union of datasets, setting this value to True could produce rows with duplicate dimension combinations, especially if one or more dimensions are also dropped. Project queries only; not supported in dataset queries. |
| `reports` | list[[ReportInputModel](dataset_query_model.md#reportinputmodel)] | `[]` | Run these pre-defined reports on the result. Project queries only; not supported in dataset queries. |
| `column_type` | [ColumnType](enums.md#columntype) | `"dimension_names"` | Whether to make the result table columns dimension types. Default behavior is to use dimension names. In order to register a result table as a derived dataset, this must be set to dimension_types. Project queries only; not supported in dataset queries. |
| `table_format` | [PivotedTableFormatModel](dataset_query_model.md#pivotedtableformatmodel) \| [StackedTableFormatModel](dataset_query_model.md#stackedtableformatmodel) | `format_type=<ValueFormat.STACKED: 'stacked'>` |  |
| `output_format` | `str` | `"parquet"` | Output file format: csv or parquet |
| `sort_columns` | list[`str`] | `[]` | Sort the results by these dimension names. |
| `dimension_filters` | list[`Annotated`] | `[]` | Filters to apply to the result. Must contain columns in the result. Project queries only; not supported in dataset queries. |
| `time_zone` | `str` \| `Literal` \| None | `None` | Convert the results to this time zone. If 'geography', use the time zone of the geography dimension. The resulting time column will be time zone-naive with time zone recorded in a separate column. Project queries only; not supported in dataset queries. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `check_format` | `check_format` | No description |
| `check_pivot_dimension_type` | `*(model)*` | No description |
| `check_column_type` | `*(model)*` | No description |

</div>


---

## ReportInputModel

*dsgrid.query.models.ReportInputModel*

Base data model for all dsgrid data models

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `report_type` | [ReportType](enums.md#reporttype) | *(required)* |  |
| `inputs` | `Any` | `None` |  |

</div>


---

## StackedTableFormatModel

*dsgrid.dataset.models.StackedTableFormatModel*

Defines a stacked (unpivoted) table format with a single value column.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `format_type` | `Literal` | `"stacked"` |  |

</div>
