import abc
import enum
from typing import Any, List, Optional, Set, Union

import pyspark.sql.functions as F
from pydantic import Field, root_validator, validator
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import make_dimension_filter, DimensionFilterBaseModel
from dsgrid.utils.files import compute_hash


class FilteredDatasetModel(DSGBaseModel):

    dataset_id: str = Field(description="Dataset ID")
    filters: List[Any] = Field(
        description="Dimension filters to apply to the dataset'",
    )


class ColumnModel(DSGBaseModel):
    """Defines one column in a SQL aggregation statement."""

    dimension_query_name: str
    function: Optional[Any] = Field(
        description="Function or name of function in pyspark.sql.functions."
    )
    alias: Optional[str] = Field(description="Name of the resulting column.")

    @validator("function")
    def handle_function(cls, function_name):
        if function_name is None:
            return function_name
        if not isinstance(function_name, str):
            return function_name

        func = getattr(F, function_name, None)
        if func is None:
            raise ValueError(f"function={function_name} is not defined in pyspark.sql.functions")
        return func

    @validator("alias")
    def handle_alias(cls, alias, values):
        if alias is not None:
            return alias

        func = values.get("function")
        if func is not None:
            name = values["dimension_query_name"]
            return f"{func.__name__}__{name}"

        return alias

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        if data["function"] is not None:
            data["function"] = data["function"].__name__
        return data

    def get_column_name(self):
        if self.alias is not None:
            return self.alias
        if self.function is None:
            return self.dimension_query_name
        return f"{self.function.__name__}__{self.dimension_query_name})"


class DimensionQueryNamesModel(DSGBaseModel):
    """Defines the list of dimensions to which the value columns should be aggregated.
    If a value is empty, that dimension will be aggregated and dropped from the table.
    """

    data_source: List[Union[str, ColumnModel]]
    geography: List[Union[str, ColumnModel]]
    metric: List[Union[str, ColumnModel]]
    model_year: List[Union[str, ColumnModel]]
    scenario: List[Union[str, ColumnModel]]
    sector: List[Union[str, ColumnModel]]
    subsector: List[Union[str, ColumnModel]]
    time: List[Union[str, ColumnModel]]
    weather_year: List[Union[str, ColumnModel]]

    @root_validator
    def fix_columns(cls, values):
        for dim_type in DimensionType:
            field = dim_type.value
            container = values[field]
            for i, item in enumerate(container):
                if isinstance(item, str):
                    container[i] = ColumnModel(dimension_query_name=item)
        return values


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
        """Yield the dimension type and ColumnModel for each dimension to keep."""
        for field in DimensionQueryNamesModel.__fields__:
            for val in getattr(self.dimensions, field):
                yield DimensionType(field), val


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


class CacheableQueryBaseModel(DSGBaseModel):
    def serialize(self):
        """Return a JSON representation of the model along with a hash that uniquely identifies it."""
        text = self.json(indent=2)
        return compute_hash(text.encode()), text


class SparkConfByDataset(DSGBaseModel):

    dataset_id: str
    conf: dict[str, Any]


class ProjectQueryDatasetParamsModel(CacheableQueryBaseModel):
    """Parameters in a project query that only apply to datasets"""

    dimension_filters: List[Any] = Field(
        # Use Any here because we don't want Pydantic to try to discern the types.
        description="Filters to apply to all datasets",
        default=[],
    )
    per_dataset_aggregations: List[AggregationModel] = Field(
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

    @validator("dimension_filters")
    def handle_dimension_filters(cls, dimension_filters):
        for i, dimension_filter in enumerate(dimension_filters):
            if not isinstance(dimension_filter, DimensionFilterBaseModel):
                dimension_filters[i] = make_dimension_filter(dimension_filter)
        return dimension_filters

    @validator("per_dataset_aggregations")
    def check_aggregations(cls, aggregations):
        for agg in aggregations:
            for dim_type in DimensionType:
                field = dim_type.value
                for item in getattr(agg.dimensions, field):
                    # TODO: we could add support for this, but it does add some ambiguity.
                    if item.function is not None:
                        raise ValueError(
                            f"function={item.function} cannot be set in ProjectQueryParamsModel"
                        )
                    if item.alias is not None:
                        # TODO: need to support aliases here. The code that handles columns during
                        # dataset concatenation doesn't support it.
                        raise ValueError(
                            f"alias={item.alias} cannot be set in ProjectQueryParamsModel"
                        )
        return aggregations


class ProjectQueryParamsModel(CacheableQueryBaseModel):
    """Defines how to transform a project into a CompositeDataset"""

    project_id: str = Field(description="Project ID for query")
    dataset_ids: List[str] = Field(description="Dataset IDs from which to read")
    excluded_dataset_ids: List[str] = Field(
        description="Datasets to exclude from query", default=[]
    )
    # TODO: default needs to change
    include_dsgrid_dataset_components: bool = Field(description="", default=False)
    dataset_params: ProjectQueryDatasetParamsModel = Field(
        description="Parameters affecting datasets. Will be used for caching intermediate tables.",
        default=ProjectQueryDatasetParamsModel(),
    )
    version: Optional[str] = Field(
        description="Version of project or dataset on which the query is based. "
        "Should not be set by the user",
    )
    spark_conf_per_dataset: list[SparkConfByDataset] = Field(
        description="Apply these Spark configuration settings while a dataset is being processed.",
        default=[],
    )

    @root_validator(pre=True)
    def check_unsupported_fields(cls, values):
        if values.get("include_dsgrid_dataset_components", False):
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

    def get_spark_conf(self, dataset_id) -> dict[str, Any]:
        """Return the Spark settings to apply while processing dataset_id."""
        for dataset in self.spark_conf_per_dataset:
            if dataset.dataset_id == dataset_id:
                return dataset.conf
        return {}


QUERY_FORMAT_VERSION = VersionInfo.parse("0.1.0")


class QueryBaseModel(CacheableQueryBaseModel, abc.ABC):
    """Base class for all queries"""

    name: str = Field(description="Name of query")
    # TODO: This field is not being used. Wait until development slows down.
    version: str = Field(
        description="Version of the query structure. Changes to the major or minor version invalidate cached tables.",
        default=str(QUERY_FORMAT_VERSION),  # TODO: str shouldn't be required
    )

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        if data["version"] is not None:
            data["version"] = str(data["version"])
        return data

    def serialize_cached_content(self):
        """Return a JSON representation of the model that can be used for caching purposes along
        with a hash that uniquely identifies it.
        """
        text = self.json(exclude={"name"}, indent=2)
        return compute_hash(text.encode()), text


class QueryResultParamsModel(CacheableQueryBaseModel):
    """Controls post-processing and storage of CompositeDatasets"""

    supplemental_columns: List[Union[str, ColumnModel]] = Field(
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
    sort_columns: List[str] = Field(
        description="Sort the results by these dimension query names.",
        default=[],
    )
    dimension_filters: List[Any] = Field(
        # Use Any here because we don't want Pydantic to try to discern the types.
        description="Filters to apply to the result. Must contain columns in the result.",
        default=[],
    )
    # TODO: implement
    time_zone: Optional[str] = Field(
        description="Convert the results to this time zone.",
        default=None,
    )

    @validator("supplemental_columns")
    def fix_supplemental_columns(cls, supplemental_columns):
        for i, column in enumerate(supplemental_columns):
            if isinstance(column, str):
                supplemental_columns[i] = ColumnModel(dimension_query_name=column)
        return supplemental_columns

    @validator("output_format")
    def check_format(cls, fmt):
        allowed = {"csv", "parquet"}
        if fmt not in allowed:
            raise ValueError(f"output_format={fmt} is not supported. Allowed={allowed}")
        return fmt

    @validator("dimension_filters")
    def handle_dimension_filters(cls, dimension_filters):
        for i, dimension_filter in enumerate(dimension_filters):
            if not isinstance(dimension_filter, DimensionFilterBaseModel):
                dimension_filters[i] = make_dimension_filter(dimension_filter)
        return dimension_filters


class ProjectQueryModel(QueryBaseModel):
    """Represents a user query on a Project."""

    project: ProjectQueryParamsModel = Field(
        description="Defines the datasets to use and how to transform them.",
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
