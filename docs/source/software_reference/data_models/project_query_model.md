# Project Query

## ProjectQueryModel

*dsgrid.query.models.ProjectQueryModel*

Represents a user query on a Project.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `name` | `str` | *(required)* | Name of query |
| `version` | `str` | `"0.1.0"` | Version of the query structure. Changes to the major or minor version invalidate cached tables. |
| `result` | [QueryResultParamsModel](dataset_query_model.md#queryresultparamsmodel) | `replace_ids_with_names=False aggregations=[] aggregate_each_dataset=False reports=[] column_type=<ColumnType.DIMENSION_NAMES: 'dimension_names'> table_format=StackedTableFormatModel(format_type=<ValueFormat.STACKED: 'stacked'>) output_format='parquet' sort_columns=[] dimension_filters=[] time_zone=None` | Controls the output results |
| `project` | [ProjectQueryParamsModel](project_query_model.md#projectqueryparamsmodel) | *(required)* | Defines the datasets to use and how to transform them. |

</div>


---

## DatasetModel

*dsgrid.query.models.DatasetModel*

Specifies the datasets to use in a project query.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dataset_id` | `str` | *(required)* | Identifier for the resulting dataset |
| `source_datasets` | list[`Annotated`] | *(required)* | Datasets from which to read. Each must be of type DatasetBaseModel. |
| `expression` | `str` \| None | `None` | Expression to combine datasets. Default is to take a union of all datasets. |
| `params` | [ProjectQueryDatasetParamsModel](project_query_model.md#projectquerydatasetparamsmodel) | `dimension_filters=[]` | Parameters affecting datasets. Used for caching intermediate tables. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `handle_expression` | `handle_expression` | No description |

</div>


---

## ProjectQueryDatasetParamsModel

*dsgrid.query.models.ProjectQueryDatasetParamsModel*

Parameters in a project query that only apply to datasets

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dimension_filters` | list[`Annotated`] | `[]` | Filters to apply to all datasets |

</div>


---

## ProjectQueryParamsModel

*dsgrid.query.models.ProjectQueryParamsModel*

Defines how to transform a project into a CompositeDataset

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `project_id` | `str` | *(required)* | Project ID for query |
| `dataset` | [DatasetModel](project_query_model.md#datasetmodel) | *(required)* | Definition of the dataset to create. |
| `excluded_dataset_ids` | list[`str`] | `[]` | Datasets to exclude from query |
| `include_dsgrid_dataset_components` | `bool` | `False` |  |
| `version` | `str` \| None | `None` | Version of project or dataset on which the query is based. Should not be set by the user |
| `mapping_plans` | list[[DatasetMappingPlan](dataset_query_model.md#datasetmappingplan)] | `[]` | Defines the order in which to map the dimensions of datasets. |
| `spark_conf_per_dataset` | list[[SparkConfByDataset](project_query_model.md#sparkconfbydataset)] | `[]` | Apply these Spark configuration settings while a dataset is being processed. |

</div>

### Validators

<div class="model-validators-table">

| Name | Applies To | Description |
|------|------------|-------------|
| `check_duplicate_dataset_ids` | `check_duplicate_dataset_ids` | No description |
| `check_unsupported_fields` | `*(model)*` | No description |
| `check_invalid_dataset_ids` | `*(model)*` | No description |

</div>


---

## SparkConfByDataset

*dsgrid.query.models.SparkConfByDataset*

Defines a custom Spark configuration to use while running a query on a dataset.

### Fields

<div class="model-fields-table">

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `dataset_id` | `str` | *(required)* |  |
| `conf` | dict[`str`, `Any`] | *(required)* |  |

</div>
