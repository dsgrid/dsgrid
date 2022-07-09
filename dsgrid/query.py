from pathlib import Path
from typing import Any, List, Optional, Union

from pydantic import Field, validator
import pyspark.sql.functions as F

from dsgrid.data_models import DSGBaseModel
from dsgrid.dataset.dimension_filters import (
    DimensionFilterList,
    DimensionFilterValue,
    DimensionFilterCombo,
)
from dsgrid.dimension.base_models import DimensionType


class FilteredDatasetModel(DSGBaseModel):

    dataset_id: str = Field(title="dataset_id", description="Dataset ID")
    filters: List[Union[DimensionFilterList, DimensionFilterValue, DimensionFilterCombo]] = Field(
        title="filters",
        description="Dimension filters to apply to the dataset'",
    )


class AlternateDimensionModel(DSGBaseModel):

    query_name: str = Field(title="query_name", description="Dimension query name")
    keep_individual_fields: bool = Field(
        description="Whether to keep individual fields or combine them"
    )


class AlternateMetricDimensionModel(AlternateDimensionModel):

    operation: Optional[str] = Field(
        description="Operation to perform on the dimension records. Only valid if "
        "keep_individual_fields is false."
    )

    @validator("operation")
    def check_operation(cls, operation, values):
        keep_individual_fields = values.get("keep_individual_fields")
        if keep_individual_fields is None:
            return operation

        if operation is not None:
            if keep_individual_fields:
                raise ValueError("operation can only be set when keep_individual_fields is false")
            allowed = ["avg", "min", "max", "sum"]
            if operation not in allowed:
                raise ValueError(f"operation={operation} is not supported. Allowed={allowed}")
        return operation


class AlternateDimensionsModel(DSGBaseModel):

    data_source: Optional[AlternateDimensionModel]
    geography: Optional[AlternateDimensionModel]
    metric: Optional[AlternateMetricDimensionModel]
    model_year: Optional[AlternateDimensionModel]
    scenario: Optional[AlternateDimensionModel]
    sector: Optional[AlternateDimensionModel]
    subsector: Optional[AlternateDimensionModel]
    time: Optional[AlternateDimensionModel]
    weather_year: Optional[AlternateDimensionModel]


# TODO DT: move to tests
assert sorted(AlternateDimensionsModel.__fields__) == sorted([x.value for x in DimensionType])


class AggregationModel(DSGBaseModel):

    group_by_columns: List[str] = Field(
        title="group_by_columns", description="Columns on which to group"
    )
    aggregate_function: Optional[Any] = Field(
        title="aggregate_function", description="Must be a function name in pyspark.sql.functions"
    )

    @validator("aggregate_function")
    def handle_aggregate_function(cls, aggregate_function):
        if isinstance(aggregate_function, str):
            return getattr(F, aggregate_function)
        return aggregate_function


class QueryModel(DSGBaseModel):
    """Represents a user query."""

    name: str = Field(title="name", description="Name of query")
    output_dir: Path = Field(
        title="output_dir", description="Directory in which to store output tables"
    )
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
    aggregation: Optional[AggregationModel] = Field(
        title="aggregation", description="Informs how to groupBy and aggregate data."
    )
    dimension_filters: List[
        Union[DimensionFilterList, DimensionFilterValue, DimensionFilterCombo]
    ] = Field(
        title="dimension_filters", description="Filters to apply to all datasets", default=[]
    )
    # TODO: When do we filter dimensions based on project vs dataset?
    # filtered_datasets: FilteredDatasetModel = Field(
    #     title="filtered_datasets", description="", default=[]
    # )
    alternate_dimensions: AlternateDimensionsModel = Field(
        title="alternate_dimensions",
        description="Specifies alternate dimensions to use. If not set, use project's base dimension.",
        default=AlternateDimensionsModel(),
    )

    @validator("excluded_dataset_ids")
    def check_dataset_ids(cls, excluded_dataset_ids, values):
        dataset_ids = values.get("dataset_ids")
        if dataset_ids is None:
            return excluded_dataset_ids

        if excluded_dataset_ids and dataset_ids:
            raise ValueError("excluded_dataset_ids and dataset_ids cannot both be set")

        return excluded_dataset_ids


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(self, query_config_model: QueryModel):
        self._model = query_config_model
        # self._dataframes = {}  # dataset_id to DataFrame
        self._alt_dimension_records = {}  # DimensionType to DataFrame

    @property
    def model(self):
        return self._model

    def iter_alt_dimension_records(self):
        return self._alt_dimension_records.items()

    def get_alt_dimension_records(self, dimension_type):
        return self._alt_dimension_records.get(dimension_type)

    def set_alt_dimension_records(self, dimension_type, records):
        self._alt_dimension_records[dimension_type] = records
