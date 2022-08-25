import abc
import enum
from pathlib import Path
from typing import Any, List, Optional, Set

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

    dataset_id: str = Field(description="Dataset ID")
    filters: List[Any] = Field(
        # Union[
        #     DimensionFilterExpressionModel,
        #     DimensionFilterExpressionRawModel,
        #     DimensionFilterColumnOperatorModel,
        # ]
        # ] = Field(
        description="Dimension filters to apply to the dataset'",
    )


class DimensionQueryNamesModel(DSGBaseModel):

    data_source: List[str]
    geography: List[str]
    metric: List[str]
    model_year: List[str]
    scenario: List[str]
    sector: List[str]
    subsector: List[str]
    time: List[str]
    weather_year: List[str]


class AggregationModel(DSGBaseModel):
    """Aggregate on one or more dimensions."""

    aggregation_function: Any = Field(
        description="Must be a function name in pyspark.sql.functions",
    )
    dimensions: DimensionQueryNamesModel = Field(description="Dimensions on which to aggregate")

    @validator("aggregation_function")
    def check_aggregation_function(cls, aggregation_function):
        if isinstance(aggregation_function, str):
            aggregation_function = getattr(F, aggregation_function, None)
            if aggregation_function is None:
                raise ValueError(f"{aggregation_function} is not defined in pyspark.sql.functions")
        elif aggregation_function is None:
            raise ValueError("aggregation_function cannot be None")
        return aggregation_function

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["aggregation_function"] = data["aggregation_function"].__name__
        return data

    def iter_dimensions_to_keep(self):
        """Yield the dimension type and query name for each dimension to keep."""
        for field in DimensionQueryNamesModel.__fields__:
            for val in getattr(self.dimensions, field):
                yield DimensionType(field), val


class ChainedAggregationModel(DSGBaseModel):
    dimensions_to_aggregate: List[AggregationModel]

    @validator("dimensions_to_aggregate")
    def check_aggregations(cls, dimensions_to_aggregate):
        length = len(dimensions_to_aggregate)
        if len(dimensions_to_aggregate) < 2:
            raise ValueError(f"length of ChainedAggregationModel must be at least 2: {length}")
        if dimensions_to_aggregate[-1].name is None:
            raise ValueError("ChainedAggregationModel requires its last model to define a name.")
        return dimensions_to_aggregate


def check_aggregations(dimensions_to_aggregate):
    for agg in dimensions_to_aggregate:
        if isinstance(agg, AggregationModel) and agg.name is None:
            raise ValueError("AggregationModel must define a name")
    return dimensions_to_aggregate


# TODO: Deserializing JSON into QueryModel probably won't work for all cases, notably the unions.


class ReportType(enum.Enum):
    """Pre-defined reports"""

    PEAK_LOAD = "peak_load"


class ReportInputModel(DSGBaseModel):

    report_type: ReportType
    inputs: Any


class TableFormatType(enum.Enum):
    """Table format types"""

    PIVOTED = "pivoted"
    LONG = "long"


class DatasetDimensionsMetadataModel(DSGBaseModel):
    """Defines the dimensions of a dataset serialized to file."""

    data_source: Set[str] = set()
    geography: Set[str] = set()
    metric: Set[str] = set()
    model_year: Set[str] = set()
    scenario: Set[str] = set()
    sector: Set[str] = set()
    subsector: Set[str] = set()
    time: Set[str] = set()
    weather_year: Set[str] = set()


class PivotedDatasetMetadataModel(DSGBaseModel):

    columns: Set[str] = set()
    dimension_type: Optional[DimensionType]


class DatasetMetadataModel(DSGBaseModel):
    """Defines the metadata for a dataset serialized to file."""

    dimensions: DatasetDimensionsMetadataModel = DatasetDimensionsMetadataModel()
    pivoted: PivotedDatasetMetadataModel = PivotedDatasetMetadataModel()
    table_format_type: Optional[TableFormatType]


class ProjectQueryParamsModel(DSGBaseModel):
    """Defines how to transform a project into a CompositeDataset"""

    project_id: str = Field(description="Project ID for query")
    dataset_ids: List[str] = Field(description="Dataset IDs from which to read")
    excluded_dataset_ids: List[str] = Field(
        description="Datasets to exclude from query", default=[]
    )
    include_dsgrid_dataset_components: bool = Field(description="")
    dimension_filters: List[Any] = Field(
        # List[
        # Union[
        #     DimensionFilterExpressionModel,
        #     DimensionFilterExpressionRawModel,
        #     DimensionFilterColumnOperatorModel,
        # ]
        # ] = Field(
        description="Filters to apply to all datasets",
        default=[],
    )
    aggregations: List[AggregationModel] = Field(
        description="Defines how to aggregate dimensions",
        default=[],
    )
    # TODO: When do we filter dimensions based on project vs dataset?
    # filtered_datasets: FilteredDatasetModel = Field(
    #     description="", default=[]
    # )
    # TODO: Should this be a result param instead of project? Or both?
    table_format: TableFormatType = Field(
        description="Controls table format",
        default=TableFormatType.PIVOTED,
    )
    version: Optional[str] = Field(
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
        fmt = TableFormatType.PIVOTED.value
        if values.get("table_format", fmt) not in (fmt, TableFormatType.PIVOTED):
            raise ValueError(f"only table_format={fmt} is currently supported")
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

    name: str = Field(description="Name of query")

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


class QueryResultParamsModel(DSGBaseModel):
    """Controls post-processing and storage of CompositeDatasets"""

    supplemental_columns: List[str] = Field(
        description="Add these supplemental dimension query names as columns in result tables. "
        "Applies to all dimensions_to_aggregate.",
        default=[],
    )
    replace_ids_with_names: bool = Field(
        description="Replace dimension record IDs with their names in result tables.",
        default=False,
    )
    aggregations: List[AggregationModel] = Field(
        description="Defines how to aggregate dimensions",
        default=[],
    )
    reports: List[ReportInputModel] = Field(
        description="Run these pre-defined reports on the result.", default=[]
    )
    output_format: str = Field(description="Output file format: csv or parquet", default="parquet")
    # TODO: implement
    sort_dimensions: List = Field(
        description="Sort the results by these dimensions.",
        default=[],
    )
    # TODO: implement
    time_zone: Optional[str] = Field(
        description="Convert the results to this time zone.",
        default=None,
    )

    @root_validator(pre=True)
    def check_unsupported_fields(cls, values):
        if values.get("sort_dimensions", []):
            raise ValueError("Setting sort_dimensions is not supported yet")
        return values

    @validator("output_format")
    def check_format(cls, fmt):
        allowed = {"csv", "parquet"}
        if fmt not in allowed:
            raise ValueError(f"output_format={fmt} is not supported. Allowed={allowed}")
        return fmt


class ProjectQueryModel(QueryBaseModel):
    """Represents a user query on a Project."""

    project: ProjectQueryParamsModel = Field(
        description="Defines the datasets to use and how to transform them."
    )
    result: QueryResultParamsModel = Field(
        description="Controls the output results",
        default=QueryResultParamsModel(),
    )

    def serialize_cached_content(self):
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        text = self.project.json(indent=2)
        return compute_hash(text.encode()), text


class CreateCompositeDatasetQueryModel(QueryBaseModel):
    """Represents a user query to create a Result Dataset. This dataset requires a Project
    in order to retrieve dimension records and dimension mapping records.
    """

    dataset_id: str = Field(description="Composite Dataset ID for query")
    project: ProjectQueryParamsModel = Field(
        description="Defines the datasets to use and how to transform them."
    )
    result: QueryResultParamsModel = Field(
        description="Controls the output results",
        default=QueryResultParamsModel(),
    )

    def serialize_cached_content(self):
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        text = self.project.json(indent=2)
        return compute_hash(text.encode()), text


class CompositeDatasetQueryModel(QueryBaseModel):
    """Represents a user query on a dataset."""

    dataset_id: str = Field(description="Aggregated Dataset ID for query")
    result: QueryResultParamsModel = Field(
        description="Controls the output results", default=QueryResultParamsModel()
    )
