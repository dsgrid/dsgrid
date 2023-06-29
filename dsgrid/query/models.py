import abc
import enum
from typing import Any, Optional, Union

import pyspark.sql.functions as F
from pydantic import Field, root_validator, validator
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel, DSGEnum
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import make_dimension_filter, DimensionFilterBaseModel
from dsgrid.utils.files import compute_hash


class FilteredDatasetModel(DSGBaseModel):
    """Filters to apply to a dataset"""

    dataset_id: str = Field(description="Dataset ID")
    filters: list[Any] = Field(
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


class ColumnType(DSGEnum):
    """Defines what the columns of a dataset table represent."""

    DIMENSION_TYPES = "dimension_types"
    DIMENSION_QUERY_NAMES = "dimension_query_names"


class DimensionQueryNamesModel(DSGBaseModel):
    """Defines the list of dimensions to which the value columns should be aggregated.
    If a value is empty, that dimension will be aggregated and dropped from the table.
    """

    geography: list[Union[str, ColumnModel]]
    metric: list[Union[str, ColumnModel]]
    model_year: list[Union[str, ColumnModel]]
    scenario: list[Union[str, ColumnModel]]
    sector: list[Union[str, ColumnModel]]
    subsector: list[Union[str, ColumnModel]]
    time: list[Union[str, ColumnModel]]
    weather_year: list[Union[str, ColumnModel]]

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

    def list_dropped_dimensions(self):
        """Return a list of dimension types that will be dropped by the aggregation."""
        return [
            DimensionType(x)
            for x in DimensionQueryNamesModel.__fields__
            if not getattr(self.dimensions, x)
        ]


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


class DimensionMetadataModel(DSGBaseModel):
    """Defines the columns in a table for a dimension."""

    dimension_query_name: str
    column_names: list[str]

    def make_key(self):
        return "__".join([self.dimension_query_name] + self.column_names)


class DatasetDimensionsMetadataModel(DSGBaseModel):
    """Records the dimensions and columns of a dataset as it is transformed by a query."""

    geography: list[DimensionMetadataModel] = []
    metric: list[DimensionMetadataModel] = []
    model_year: list[DimensionMetadataModel] = []
    scenario: list[DimensionMetadataModel] = []
    sector: list[DimensionMetadataModel] = []
    subsector: list[DimensionMetadataModel] = []
    time: list[DimensionMetadataModel] = []
    weather_year: list[DimensionMetadataModel] = []

    def add_metadata(self, dimension_type: DimensionType, metadata: DimensionMetadataModel):
        """Add dimension metadata. Skip duplicates."""
        container = getattr(self, dimension_type.value)
        if metadata.make_key() not in {x.make_key() for x in container}:
            container.append(metadata)

    def get_metadata(self, dimension_type: DimensionType):
        """Return the dimension metadata."""
        return getattr(self, dimension_type.value)

    def replace_metadata(
        self, dimension_type: DimensionType, metadata: list[DimensionMetadataModel]
    ):
        """Replace the dimension metadata."""
        setattr(self, dimension_type.value, metadata)

    def get_column_names(self, dimension_type: DimensionType):
        """Return the column names for the given dimension type."""
        column_names = set()
        for item in getattr(self, dimension_type.value):
            column_names.update(item.column_names)
        return column_names

    def get_dimension_query_names(self, dimension_type: DimensionType):
        """Return the dimension query names for the given dimension type."""
        return {x.dimension_query_name for x in getattr(self, dimension_type.value)}

    def remove_metadata(self, dimension_type: DimensionType, dimension_query_name):
        """Remove the dimension metadata for the given dimension query name."""
        container = getattr(self, dimension_type.value)
        for i, metadata in enumerate(container):
            if metadata.dimension_query_name == dimension_query_name:
                container.pop(i)
                break


class PivotedDatasetMetadataModel(DSGBaseModel):

    columns: set[str] = set()
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
    """Defines a custom Spark configuration to use while running a query on a dataset."""

    dataset_id: str
    conf: dict[str, Any]


class ProjectQueryDatasetParamsModel(CacheableQueryBaseModel):
    """Parameters in a project query that only apply to datasets"""

    dimension_filters: list[Any] = Field(
        # Use Any here because we don't want Pydantic to try to discern the types.
        description="Filters to apply to all datasets",
        default=[],
    )
    # TODO #202: Should this be a result param instead of project? Or both?
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


class DatasetModel(DSGBaseModel):
    """Specifies the datasets to use in a project query."""

    dataset_id: str = Field(description="Identifier for the resulting dataset")
    source_datasets: list[Any] = Field(
        description="Datasets from which to read. Each must be of type DatasetBaseModel."
    )
    expression: str | None = Field(
        description="Expression to combine datasets. Default is to take a union of all datasets.",
        default=None,
    )
    params: ProjectQueryDatasetParamsModel = Field(
        description="Parameters affecting datasets. Used for caching intermediate tables.",
        default=ProjectQueryDatasetParamsModel(),
    )

    @validator("source_datasets", each_item=True)
    def check_component(cls, component):
        if isinstance(component, DatasetBaseModel):
            return component
        if component["dataset_type"] == DatasetType.STANDALONE.value:
            return StandaloneDatasetModel(**component)
        elif component["dataset_type"] == DatasetType.EXPONENTIAL_GROWTH.value:
            return ExponentialGrowthDatasetModel(**component)
        raise ValueError(f"dataset_type={component['dataset_type']} isn't supported")

    @validator("expression")
    def handle_expression(cls, expression, values):
        if expression is None:
            expression = " | ".join((x.dataset_id for x in values["source_datasets"]))
        return expression


class DatasetType(enum.Enum):
    """Defines the type of a dataset in a query."""

    EXPONENTIAL_GROWTH = "exponential_growth"
    STANDALONE = "standalone"
    DERIVED = "derived"


class DatasetBaseModel(DSGBaseModel, abc.ABC):
    @abc.abstractmethod
    def get_dataset_id(self) -> str:
        """Return the primary dataset ID.

        Returns
        -------
        str
        """


class StandaloneDatasetModel(DatasetBaseModel):
    """A dataset with energy use data."""

    dataset_id: str = Field(description="Dataset identifier")
    dataset_type: DatasetType = Field(
        description="Type of dataset specified in a query",
        default=DatasetType.STANDALONE,
    )

    @validator("dataset_type")
    def check_dataset_type(cls, dataset_type):
        if dataset_type != DatasetType.STANDALONE:
            raise ValueError(f"dataset_type must be {DatasetType.STANDALONE}: {dataset_type}")
        return dataset_type

    def get_dataset_id(self) -> str:
        return self.dataset_id


class ExponentialGrowthDatasetModel(DatasetBaseModel):
    """A dataset with growth rates that can be applied to a standalone dataset."""

    dataset_id: str = Field(description="Identifier for the resulting dataset")
    initial_value_dataset_id: str = Field(description="Principal dataset identifier")
    growth_rate_dataset_id: str = Field(
        description="Growth rate dataset identifier to apply to the principal dataset"
    )
    construction_method: str = Field(
        description="Specifier for the code that applies the growth rate to the principal dataset"
    )
    base_year: int = Field(
        description="Base year of the dataset to use in growth rate application. Must be a year "
        "defined in the principal dataset's model year dimension. If None, there must be only "
        "one model year in that dimension and it will be used.",
        default=None,
    )
    dataset_type: DatasetType = Field(
        description="Type of dataset specified in a query",
        default=DatasetType.EXPONENTIAL_GROWTH,
    )

    @validator("dataset_type")
    def check_dataset_type(cls, dataset_type):
        if dataset_type != DatasetType.EXPONENTIAL_GROWTH:
            raise ValueError(
                f"dataset_type must be {DatasetType.EXPONENTIAL_GROWTH}: {dataset_type}"
            )
        return dataset_type

    def get_dataset_id(self) -> str:
        return self.initial_value_dataset_id


class ProjectQueryParamsModel(CacheableQueryBaseModel):
    """Defines how to transform a project into a CompositeDataset"""

    project_id: str = Field(description="Project ID for query")
    dataset: DatasetModel = Field(description="Definition of the dataset to create.")
    excluded_dataset_ids: list[str] = Field(
        description="Datasets to exclude from query", default=[]
    )
    # TODO #203: default needs to change
    include_dsgrid_dataset_components: bool = Field(description="", default=False)
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
    # TODO #204: This field is not being used. Wait until development slows down.
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

    supplemental_columns: list[Union[str, ColumnModel]] = Field(
        description="Add these supplemental dimension query names as columns in result tables. "
        "Applies to all dimensions_to_aggregate.",
        default=[],
    )
    replace_ids_with_names: bool = Field(
        description="Replace dimension record IDs with their names in result tables.",
        default=False,
    )
    aggregations: list[AggregationModel] = Field(
        description="Defines how to aggregate dimensions",
        default=[],
    )
    reports: list[ReportInputModel] = Field(
        description="Run these pre-defined reports on the result.", default=[]
    )
    column_type: ColumnType = Field(
        description="Whether to make the result table columns dimension types. Default behavior "
        "is to use dimension query names. In order to register a result table as a derived "
        f"dataset, this must be set to {ColumnType.DIMENSION_TYPES.value}.",
        default=ColumnType.DIMENSION_QUERY_NAMES,
    )
    output_format: str = Field(description="Output file format: csv or parquet", default="parquet")
    sort_columns: list[str] = Field(
        description="Sort the results by these dimension query names.",
        default=[],
    )
    dimension_filters: list[Any] = Field(
        # Use Any here because we don't want Pydantic to try to discern the types.
        description="Filters to apply to the result. Must contain columns in the result.",
        default=[],
    )
    # TODO #205: implement
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

    @validator("column_type")
    def check_column_type(cls, column_type, values):
        if column_type == ColumnType.DIMENSION_TYPES:
            # Cannot allow duplicate column names.
            if values["supplemental_columns"]:
                raise ValueError(
                    f"column_type={ColumnType.DIMENSION_TYPES} is incompatible with supplemental_columns"
                )
            for agg in values["aggregations"]:
                for dim_type in DimensionType:
                    columns = getattr(agg.dimensions, dim_type.value)
                    if len(columns) > 1:
                        raise ValueError(
                            f"Multiple columns are incompatible with {column_type=}. {columns=}"
                        )
        return column_type


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
