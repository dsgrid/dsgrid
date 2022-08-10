import abc
from pathlib import Path
from typing import Any, List, Optional, Union

import pyspark.sql.functions as F
from pydantic import Field, root_validator, validator

from dsgrid.data_models import DSGBaseModel

# from dsgrid.dataset.dimension_filters import (
#     DimensionFilterExpressionModel,
#     DimensionFilterExpressionRawModel,
#     DimensionFilterColumnOperatorModel,
# )
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.files import compute_hash, load_data


class FilteredDatasetModel(DSGBaseModel):

    dataset_id: str = Field(title="dataset_id", description="Dataset ID")
    filters: List[Any] = Field(
        # Union[
        #     DimensionFilterExpressionModel,
        #     DimensionFilterExpressionRawModel,
        #     DimensionFilterColumnOperatorModel,
        # ]
        # ] = Field(
        title="filters",
        description="Dimension filters to apply to the dataset'",
    )


class MetricReductionModel(DSGBaseModel):
    """Reduces the metric values for each set of unique dimension combinations, excluding
    dimension_query_name."""

    dimension_query_name: str = Field(
        title="dimension_query_name", description="Dimension query name"
    )
    operation: str = Field(
        title="operation",
        description="Operation to perform on the dimension.",
    )

    @validator("operation")
    def check_operation(cls, operation):
        allowed = ["avg", "min", "mean", "max", "sum"]
        if operation not in allowed:
            raise ValueError(f"operation={operation} is not supported. Allowed={allowed}")
        return operation


class AggregationModel(DSGBaseModel):

    group_by_columns: List[str] = Field(
        title="group_by_columns", description="Columns on which to group"
    )
    aggregation_function: Any = Field(
        title="aggregation_function",
        description="Must be a function name in pyspark.sql.functions",
    )
    name: Optional[str]

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["aggregation_function"] = data["aggregation_function"].__name__
        return data

    @validator("group_by_columns")
    def check_group_by_columns(cls, group_by_columns):
        if not group_by_columns:
            raise ValueError("group_by_columns cannot be empty")
        return group_by_columns

    @validator("aggregation_function")
    def check_aggregation_function(cls, aggregation_function):
        if isinstance(aggregation_function, str):
            aggregation_function = getattr(F, aggregation_function, None)
            if aggregation_function is None:
                raise ValueError(f"{aggregation_function} is not defined in pyspark.sql.functions")
        return aggregation_function


class ChainedAggregationModel(DSGBaseModel):
    aggregations: List[AggregationModel]

    @validator("aggregations")
    def check_aggregations(cls, aggregations):
        length = len(aggregations)
        if len(aggregations) < 2:
            raise ValueError(f"length of ChainedAggregationModel must be at least 2: {length}")
        if aggregations[-1].name is None:
            raise ValueError("ChainedAggregationModel requires its last model to define a name.")
        return aggregations


# TODO: Deserializing JSON into QueryModel probably won't work for all cases, notably the unions.


class ProjectQueryModel(DSGBaseModel):
    """Defines how to transform a project into a derived dataset"""

    project_id: str = Field(title="project_id", description="Project ID for query")
    dataset_ids: List[str] = Field(
        title="dataset_ids", description="Dataset IDs from which to read"
    )
    excluded_dataset_ids: List[str] = Field(
        title="excluded_dataset_ids", description="Datasets to exclude from query", default=[]
    )
    include_dsgrid_dataset_components: bool = Field(
        title="include_dsgrid_dataset_components", description=""
    )
    metric_reductions: List[MetricReductionModel] = Field(
        title="metric_reductions",
        description="Specifies how metric values should be reduced.",
        default=[],
    )
    drop_dimensions: List[DimensionType] = Field(
        title="drop_dimensions",
        description="Drop columns for these dimensions.",
        default=[],
    )
    dimension_filters: List[Any] = Field(
        # List[
        # Union[
        #     DimensionFilterExpressionModel,
        #     DimensionFilterExpressionRawModel,
        #     DimensionFilterColumnOperatorModel,
        # ]
        # ] = Field(
        title="dimension_filters",
        description="Filters to apply to all datasets",
        default=[],
    )
    # TODO: When do we filter dimensions based on project vs dataset?
    # filtered_datasets: FilteredDatasetModel = Field(
    #     title="filtered_datasets", description="", default=[]
    # )
    version: Optional[str] = Field(
        title="version",
        description="Version of project or dataset on which the query is based. "
        "Should not be set by the user",
    )

    @root_validator(pre=True)
    def check_unsupported_fields(cls, values):
        if values.get("include_dsgrid_dataset_components", True):
            raise ValueError("Setting include_dsgrid_dataset_components=true is not supported yet")
        if values.get("drop_dimensions", []):
            raise ValueError("drop_dimensions is not supported yet")
        if values.get("excluded_dataset_ids", []):
            raise ValueError("excluded_dataset_ids is not supported yet")
        return values

    @validator("excluded_dataset_ids")
    def check_dataset_ids(cls, excluded_dataset_ids, values):
        dataset_ids = values.get("dataset_ids")
        if dataset_ids is None:
            return excluded_dataset_ids

        if excluded_dataset_ids and dataset_ids:
            raise ValueError("excluded_dataset_ids and dataset_ids cannot both be set")

        return excluded_dataset_ids


class QueryBaseModel(DSGBaseModel, abc.ABC):
    """Base class for all queries"""

    name: str = Field(title="name", description="Name of query")

    @classmethod
    def from_file(cls, filename: Path):
        """Deserialize the query model from a file."""
        return cls(**load_data(filename))

    def serialize(self):
        """Return a JSON representation of the model along with a hash that uniquely identifies it."""
        text = self.json(indent=2)
        return compute_hash(text.encode()), text

    def serialize_cached_content(self):
        """Return a JSON representation of the model that can be used for caching purposes along
        with a hash that uniquely identifies it.
        """
        text = self.json(exclude={"name"}, indent=2)
        return compute_hash(text.encode()), text


def check_aggregations(aggregations):
    for agg in aggregations:
        if isinstance(agg, AggregationModel) and agg.name is None:
            raise ValueError("AggregationModel must define a name")
    return aggregations


class QueryResultModel(DSGBaseModel):
    """Defines fie queries that produce result tables"""

    supplemental_columns: List[str] = Field(
        title="supplemental_columns",
        description="Add these supplemental dimension query names as columns in result tables. "
        "Applies to all aggregations.",
        default=[],
    )
    replace_ids_with_names: bool = Field(
        title="replace_ids_with_names",
        description="Replace dimension record IDs with their names in result tables.",
        default=False,
    )
    metric_reductions: List[MetricReductionModel] = Field(
        title="metric_reductions",
        description="Specifies how metric values should be reduced.",
        default=[],
    )
    aggregations: List[Union[AggregationModel, ChainedAggregationModel]] = Field(
        title="aggregations", description="Informs how to groupBy and aggregate data.", default=[]
    )
    output_format: str = Field(
        title="output_format", description="Output file format: csv or parquet", default="parquet"
    )
    # TODO: implement
    sort_dimensions: List = Field(
        title="sort_dimensions",
        description="Sort the results by these dimensions.",
        default=[],
    )
    # TODO: implement
    time_zone: Optional[str] = Field(
        title="time_zone",
        description="Convert the results to this time zone.",
        default=None,
    )

    @root_validator(pre=True)
    def check_unsupported_fields(cls, values):
        if values.get("sort_dimensions", []):
            raise ValueError("Setting sort_dimensions is not supported yet")
        return values

    @validator("aggregations")
    def check_aggregations(cls, aggregations):
        return check_aggregations(aggregations)

    @validator("output_format")
    def check_format(cls, fmt):
        allowed = {"csv", "parquet"}
        if fmt not in allowed:
            raise ValueError(f"output_format={fmt} is not supported. Allowed={allowed}")
        return fmt


class ProjectQueryResultModel(QueryBaseModel):
    """Represents a user query on a Project."""

    project: ProjectQueryModel = Field(
        title="project", description="Defines the datasets to use and how to transform them."
    )
    result: QueryResultModel = Field(
        title="result",
        description="Controls the output results",
        default=QueryResultModel(),
    )

    @root_validator
    def check_metric_reductions(cls, values):
        if "result" not in values or "project" not in values:
            return values
        if values["result"].metric_reductions and values["project"].metric_reductions:
            raise ValueError(
                "metric_reductions cannot be set within the project constraints as well as in the result"
            )
        return values

    def serialize_cached_content(self):
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        text = self.project.json(indent=2)
        return compute_hash(text.encode()), text


class DerivedDatasetQueryModel(QueryBaseModel):
    """Represents a user query to create a Derived Dataset. This dataset requires a Project
    in order to retrieve dimension records and dimension mapping records.
    """

    dataset_id: str = Field(title="dataset_id", description="Derived Dataset ID for query")
    project: ProjectQueryModel = Field(
        title="project", description="Defines the datasets to use and how to transform them."
    )
    # TODO: not all fields apply here. The differences between a derived dataset and result
    # are still murky.
    result: QueryResultModel = Field(
        title="result",
        description="Controls the output results",
        default=QueryResultModel(),
    )

    def serialize_cached_content(self):
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        text = self.project.json(indent=2)
        return compute_hash(text.encode()), text


# class CreateStandaloneDerivedDatasetQueryModel(QueryBaseModel):
# """Represents a user query to create a Standalone Derived Dataset. This dataset contains
# all dimension records and dimension mapping records. It does not need a Project.
# """
# TODO: create the format (config / .toml) that will define this object


class DerivedDatasetQueryResultModel(QueryBaseModel):
    """Represents a user query on a Derived Dataset."""

    dataset_id: str = Field(title="dataset_id", description="Derived Dataset ID for query")
    result: QueryResultModel = Field(
        title="result", description="Controls the output results", default=QueryResultModel()
    )
