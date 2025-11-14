import abc
import itertools
from enum import StrEnum
from typing import Any, Generator, Union, Literal, Self, TypeAlias

from pydantic import field_validator, model_validator, Field, field_serializer, ValidationInfo
from semver import VersionInfo
from typing_extensions import Annotated

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.project_config import DatasetBaseDimensionNamesModel
from dsgrid.data_models import DSGBaseModel, make_model_config
from dsgrid.dataset.models import (
    TableFormatModel,
    UnpivotedTableFormatModel,
    TableFormatType,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterExpressionModel,
    DimensionFilterExpressionRawModel,
    DimensionFilterColumnOperatorModel,
    DimensionFilterBetweenColumnOperatorModel,
    SubsetDimensionFilterModel,
    SupplementalDimensionFilterColumnOperatorModel,
)
from dsgrid.dimension.time import TimeBasedDataAdjustmentModel
from dsgrid.query.dataset_mapping_plan import (
    DatasetMappingPlan,
)
from dsgrid.spark.types import F
from dsgrid.utils.files import compute_hash
from dsgrid.dimension.time import TimeZone


DimensionFilters: TypeAlias = Annotated[
    Union[
        DimensionFilterExpressionModel,
        DimensionFilterExpressionRawModel,
        DimensionFilterColumnOperatorModel,
        DimensionFilterBetweenColumnOperatorModel,
        SubsetDimensionFilterModel,
        SupplementalDimensionFilterColumnOperatorModel,
    ],
    Field(discriminator="filter_type"),
]


class FilteredDatasetModel(DSGBaseModel):
    """Filters to apply to a dataset"""

    dataset_id: str = Field(description="Dataset ID")
    filters: list[DimensionFilters]


class ColumnModel(DSGBaseModel):
    """Defines one column in a SQL aggregation statement."""

    dimension_name: str
    function: Any = Field(
        default=None, description="Function or name of function in pyspark.sql.functions."
    )
    alias: str | None = Field(default=None, description="Name of the resulting column.")

    @field_validator("function")
    @classmethod
    def handle_function(cls, function_name):
        if function_name is None:
            return function_name
        if not isinstance(function_name, str):
            return function_name

        func = getattr(F, function_name, None)
        if func is None:
            msg = f"function={function_name} is not defined in pyspark.sql.functions"
            raise ValueError(msg)
        return func

    @field_validator("alias")
    @classmethod
    def handle_alias(cls, alias, info: ValidationInfo):
        if alias is not None:
            return alias
        func = info.data.get("function")
        if func is not None:
            name = info.data["dimension_name"]
            return f"{func.__name__}__{name}"

        return alias

    @field_serializer("function")
    def serialize_function(self, function, _):
        if function is not None:
            return function.__name__
        return function

    def get_column_name(self):
        if self.alias is not None:
            return self.alias
        if self.function is None:
            return self.dimension_name
        return f"{self.function.__name__}__{self.dimension_name})"


class ColumnType(StrEnum):
    """Defines what the columns of a dataset table represent."""

    DIMENSION_TYPES = "dimension_types"
    DIMENSION_NAMES = "dimension_names"


class DimensionNamesModel(DSGBaseModel):
    """Defines the list of dimensions to which the value columns should be aggregated.
    If a value is empty, that dimension will be aggregated and dropped from the table.
    """

    model_config = make_model_config(protected_namespaces=())

    geography: list[Union[str, ColumnModel]]
    metric: list[Union[str, ColumnModel]]
    model_year: list[Union[str, ColumnModel]]
    scenario: list[Union[str, ColumnModel]]
    sector: list[Union[str, ColumnModel]]
    subsector: list[Union[str, ColumnModel]]
    time: list[Union[str, ColumnModel]]
    weather_year: list[Union[str, ColumnModel]]

    @model_validator(mode="before")
    def fix_columns(cls, values):
        for dim_type in DimensionType:
            field = dim_type.value
            container = values[field]
            for i, item in enumerate(container):
                if isinstance(item, str):
                    container[i] = ColumnModel(dimension_name=item)
        return values


class AggregationModel(DSGBaseModel):
    """Aggregate on one or more dimensions."""

    aggregation_function: Any = Field(
        default=None,
        description="Must be a function name in pyspark.sql.functions",
    )
    dimensions: DimensionNamesModel = Field(description="Dimensions on which to aggregate")

    @field_validator("aggregation_function")
    @classmethod
    def check_aggregation_function(cls, aggregation_function):
        if isinstance(aggregation_function, str):
            aggregation_function = getattr(F, aggregation_function, None)
            if aggregation_function is None:
                msg = f"{aggregation_function} is not defined in pyspark.sql.functions"
                raise ValueError(msg)
        elif aggregation_function is None:
            msg = "aggregation_function cannot be None"
            raise ValueError(msg)
        return aggregation_function

    @field_validator("dimensions")
    @classmethod
    def check_for_metric(cls, dimensions):
        if not dimensions.metric:
            msg = "An AggregationModel must include the metric dimension."
            raise ValueError(msg)
        return dimensions

    @field_serializer("aggregation_function")
    def serialize_aggregation_function(self, function, _):
        return function.__name__

    def iter_dimensions_to_keep(self) -> Generator[tuple[DimensionType, ColumnModel], None, None]:
        """Yield the dimension type and ColumnModel for each dimension to keep."""
        for field in DimensionNamesModel.model_fields:
            for val in getattr(self.dimensions, field):
                yield DimensionType(field), val

    def list_dropped_dimensions(self) -> list[DimensionType]:
        """Return a list of dimension types that will be dropped by the aggregation."""
        return [
            DimensionType(x)
            for x in DimensionNamesModel.model_fields
            if not getattr(self.dimensions, x)
        ]


class ReportType(StrEnum):
    """Pre-defined reports"""

    PEAK_LOAD = "peak_load"


class ReportInputModel(DSGBaseModel):
    report_type: ReportType
    inputs: Any = None


class DimensionMetadataModel(DSGBaseModel):
    """Defines the columns in a table for a dimension."""

    dimension_name: str
    column_names: list[str] = Field(
        description="Columns associated with this dimension. Could be a dimension name, "
        "the string-ified DimensionType, multiple strings as can happen with time, or dimension "
        "record IDS if the dimension is pivoted."
    )

    def make_key(self):
        return "__".join([self.dimension_name] + self.column_names)


class DatasetDimensionsMetadataModel(DSGBaseModel):
    """Records the dimensions and columns of a dataset as it is transformed by a query."""

    model_config = make_model_config(protected_namespaces=())

    geography: list[DimensionMetadataModel] = []
    metric: list[DimensionMetadataModel] = []
    model_year: list[DimensionMetadataModel] = []
    scenario: list[DimensionMetadataModel] = []
    sector: list[DimensionMetadataModel] = []
    subsector: list[DimensionMetadataModel] = []
    time: list[DimensionMetadataModel] = []
    weather_year: list[DimensionMetadataModel] = []

    def add_metadata(
        self, dimension_type: DimensionType, metadata: DimensionMetadataModel
    ) -> None:
        """Add dimension metadata. Skip duplicates."""
        container = getattr(self, dimension_type.value)
        if metadata.make_key() not in {x.make_key() for x in container}:
            container.append(metadata)

    def get_metadata(self, dimension_type: DimensionType) -> list[DimensionMetadataModel]:
        """Return the dimension metadata."""
        return getattr(self, dimension_type.value)

    def replace_metadata(
        self, dimension_type: DimensionType, metadata: list[DimensionMetadataModel]
    ) -> None:
        """Replace the dimension metadata."""
        setattr(self, dimension_type.value, metadata)

    def get_column_names(self, dimension_type: DimensionType) -> set[str]:
        """Return the column names for the given dimension type."""
        column_names = set()
        for item in getattr(self, dimension_type.value):
            column_names.update(item.column_names)
        return column_names

    def get_dimension_names(self, dimension_type: DimensionType) -> set[str]:
        """Return the dimension names for the given dimension type."""
        return {x.dimension_name for x in getattr(self, dimension_type.value)}

    def remove_metadata(self, dimension_type: DimensionType, dimension_name: str) -> None:
        """Remove the dimension metadata for the given dimension name."""
        container = getattr(self, dimension_type.value)
        for i, metadata in enumerate(container):
            if metadata.dimension_name == dimension_name:
                container.pop(i)
                break


class DatasetMetadataModel(DSGBaseModel):
    """Defines the metadata for a dataset serialized to file."""

    dimensions: DatasetDimensionsMetadataModel = DatasetDimensionsMetadataModel()
    table_format: TableFormatModel
    # This will be set at the query context level but not per-dataset.
    base_dimension_names: DatasetBaseDimensionNamesModel = DatasetBaseDimensionNamesModel()

    def get_table_format_type(self) -> TableFormatType:
        """Return the format type of the table."""
        return TableFormatType(self.table_format.format_type)


class CacheableQueryBaseModel(DSGBaseModel):
    def serialize_with_hash(self, *args, **kwargs) -> tuple[str, str]:
        """Return a JSON representation of the model along with a hash that uniquely identifies it."""
        text = self.model_dump_json(indent=2)
        return compute_hash(text.encode()), text


class SparkConfByDataset(DSGBaseModel):
    """Defines a custom Spark configuration to use while running a query on a dataset."""

    dataset_id: str
    conf: dict[str, Any]


class ProjectQueryDatasetParamsModel(CacheableQueryBaseModel):
    """Parameters in a project query that only apply to datasets"""

    dimension_filters: list[DimensionFilters] = Field(
        description="Filters to apply to all datasets",
        default=[],
    )


class DatasetType(StrEnum):
    """Defines the type of a dataset in a query."""

    PROJECTION = "projection"
    STANDALONE = "standalone"
    DERIVED = "derived"


class DatasetConstructionMethod(StrEnum):
    """Defines the type of construction method for DatasetType.PROJECTION."""

    EXPONENTIAL_GROWTH = "exponential_growth"
    ANNUAL_MULTIPLIER = "annual_multiplier"


class DatasetBaseModel(DSGBaseModel, abc.ABC):
    @abc.abstractmethod
    def get_dataset_id(self) -> str:
        """Return the primary dataset ID.

        Returns
        -------
        str
        """

    @abc.abstractmethod
    def list_source_dataset_ids(self) -> list[str]:
        """Return a list of all source dataset IDs."""


class StandaloneDatasetModel(DatasetBaseModel):
    """A dataset with energy use data."""

    dataset_type: Literal[DatasetType.STANDALONE] = Field(default=DatasetType.STANDALONE)
    dataset_id: str = Field(description="Dataset identifier")

    def get_dataset_id(self) -> str:
        return self.dataset_id

    def list_source_dataset_ids(self) -> list[str]:
        return [self.dataset_id]


class ProjectionDatasetModel(DatasetBaseModel):
    """A dataset with growth rates that can be applied to a standalone dataset."""

    dataset_type: Literal[DatasetType.PROJECTION] = Field(default=DatasetType.PROJECTION)
    dataset_id: str = Field(description="Identifier for the resulting dataset")
    initial_value_dataset_id: str = Field(description="Principal dataset identifier")
    growth_rate_dataset_id: str = Field(
        description="Growth rate dataset identifier to apply to the principal dataset"
    )
    construction_method: DatasetConstructionMethod = Field(
        default=DatasetConstructionMethod.EXPONENTIAL_GROWTH,
        description="Specifier for the code that applies the growth rate to the principal dataset",
    )
    base_year: int | None = Field(
        description="Base year of the dataset to use in growth rate application. Must be a year "
        "defined in the principal dataset's model year dimension. If None, there must be only "
        "one model year in that dimension and it will be used.",
        default=None,
    )

    def get_dataset_id(self) -> str:
        return self.initial_value_dataset_id

    def list_source_dataset_ids(self) -> list[str]:
        return [self.initial_value_dataset_id, self.growth_rate_dataset_id]


AbstractDatasetModel = Annotated[
    Union[StandaloneDatasetModel, ProjectionDatasetModel], Field(discriminator="dataset_type")
]


class DatasetModel(DSGBaseModel):
    """Specifies the datasets to use in a project query."""

    dataset_id: str = Field(description="Identifier for the resulting dataset")
    source_datasets: list[AbstractDatasetModel] = Field(
        description="Datasets from which to read. Each must be of type DatasetBaseModel.",
    )
    expression: str | None = Field(
        description="Expression to combine datasets. Default is to take a union of all datasets.",
        default=None,
    )
    params: ProjectQueryDatasetParamsModel = Field(
        description="Parameters affecting datasets. Used for caching intermediate tables.",
        default=ProjectQueryDatasetParamsModel(),
    )

    @field_validator("expression")
    @classmethod
    def handle_expression(cls, expression, info: ValidationInfo):
        if "source_datasets" not in info.data:
            return expression

        if expression is None:
            expression = " | ".join((x.dataset_id for x in info.data["source_datasets"]))
        return expression


class ProjectQueryParamsModel(CacheableQueryBaseModel):
    """Defines how to transform a project into a CompositeDataset"""

    project_id: str = Field(description="Project ID for query")
    dataset: DatasetModel = Field(description="Definition of the dataset to create.")
    excluded_dataset_ids: list[str] = Field(
        description="Datasets to exclude from query", default=[]
    )
    # TODO #203: default needs to change
    include_dsgrid_dataset_components: bool = Field(description="", default=False)
    version: str | None = Field(
        default=None,
        description="Version of project or dataset on which the query is based. "
        "Should not be set by the user",
    )
    mapping_plans: list[DatasetMappingPlan] = Field(
        default=[],
        description="Defines the order in which to map the dimensions of datasets.",
    )
    spark_conf_per_dataset: list[SparkConfByDataset] = Field(
        description="Apply these Spark configuration settings while a dataset is being processed.",
        default=[],
    )

    @model_validator(mode="before")
    @classmethod
    def check_unsupported_fields(cls, values):
        if values.get("include_dsgrid_dataset_components", False):
            msg = "Setting include_dsgrid_dataset_components=true is not supported yet"
            raise ValueError(msg)
        if values.get("drop_dimensions", []):
            msg = "drop_dimensions is not supported yet"
            raise ValueError(msg)
        if values.get("excluded_dataset_ids", []):
            msg = "excluded_dataset_ids is not supported yet"
            raise ValueError(msg)
        return values

    @model_validator(mode="after")
    def check_invalid_dataset_ids(self) -> Self:
        source_dataset_ids: set[str] = set()
        for src_dataset in self.dataset.source_datasets:
            source_dataset_ids.update(src_dataset.list_source_dataset_ids())
        for item in itertools.chain(self.mapping_plans, self.spark_conf_per_dataset):
            if item.dataset_id not in source_dataset_ids:
                msg = f"Dataset {item.dataset_id} is not a source dataset"
                raise ValueError(msg)
        return self

    @field_validator("mapping_plans", "spark_conf_per_dataset")
    @classmethod
    def check_duplicate_dataset_ids(cls, value: list) -> list:
        dataset_ids: set[str] = set()
        for item in value:
            if item.dataset_id in dataset_ids:
                msg = f"{item.dataset_id} is stored multiple times"
                raise ValueError(msg)
            dataset_ids.add(item.dataset_id)
        return value

    def set_dataset_mapper(self, new_mapper: DatasetMappingPlan) -> None:
        for i, mapper in enumerate(self.mapping_plans):
            if mapper.dataset_id == new_mapper.dataset_id:
                self.mapping_plans[i] = new_mapper
                return
        self.mapping_plans.append(new_mapper)

    def get_dataset_mapping_plan(self, dataset_id: str) -> DatasetMappingPlan | None:
        """Return the mapping plan for this dataset_id or None if the user did not
        specify one.
        """
        for mapper in self.mapping_plans:
            if dataset_id == mapper.dataset_id:
                return mapper
        return None

    def get_spark_conf(self, dataset_id: str) -> dict[str, Any]:
        """Return the Spark settings to apply while processing dataset_id."""
        for dataset in self.spark_conf_per_dataset:
            if dataset.dataset_id == dataset_id:
                return dataset.conf
        return {}


QUERY_FORMAT_VERSION = VersionInfo.parse("0.1.0")


class QueryResultParamsModel(CacheableQueryBaseModel):
    """Controls post-processing and storage of CompositeDatasets"""

    replace_ids_with_names: bool = Field(
        description="Replace dimension record IDs with their names in result tables.",
        default=False,
    )
    aggregations: list[AggregationModel] = Field(
        description="Defines how to aggregate dimensions",
        default=[],
    )
    aggregate_each_dataset: bool = Field(
        description="If True, aggregate each dataset before applying the expression to create one "
        "overall dataset. This parameter must be set to True for queries that will be adding or "
        "subtracting datasets with different dimensionality. Defaults to False, which corresponds to "
        "the default behavior of performing one aggregation on the overall dataset. WARNING: "
        "For a standard query that performs a union of datasets, setting this value to True could "
        "produce rows with duplicate dimension combinations, especially if one or more "
        "dimensions are also dropped.",
        default=False,
    )
    reports: list[ReportInputModel] = Field(
        description="Run these pre-defined reports on the result.", default=[]
    )
    column_type: ColumnType = Field(
        description="Whether to make the result table columns dimension types. Default behavior "
        "is to use dimension names. In order to register a result table as a derived "
        f"dataset, this must be set to {ColumnType.DIMENSION_TYPES.value}.",
        default=ColumnType.DIMENSION_NAMES,
    )
    table_format: TableFormatModel = UnpivotedTableFormatModel()
    output_format: str = Field(description="Output file format: csv or parquet", default="parquet")
    sort_columns: list[str] = Field(
        description="Sort the results by these dimension names.",
        default=[],
    )
    dimension_filters: list[DimensionFilters] = Field(
        description="Filters to apply to the result. Must contain columns in the result.",
        default=[],
    )
    # TODO #205: implement
    time_zone: TimeZone | Literal["geography"] | None = Field(
        description="Convert the results to this time zone. If 'geography', use the time zone "
        "of the geography dimension. The resulting time column will be time zone-naive with "
        "time zone recorded in a separate column.",
        default=None,
    )

    @model_validator(mode="after")
    def check_pivot_dimension_type(self) -> "QueryResultParamsModel":
        if self.table_format.format_type == TableFormatType.PIVOTED:
            pivoted_dim_type = self.table_format.pivoted_dimension_type
            for agg in self.aggregations:
                names = getattr(agg.dimensions, pivoted_dim_type.value)
                num_names = len(names)
                if num_names == 0:
                    msg = (
                        f"The pivoted dimension ({pivoted_dim_type}) "
                        "must be specified in all aggregations."
                    )
                    raise ValueError(msg)
                elif len(names) > 1:
                    msg = (
                        f"The pivoted dimension ({pivoted_dim_type}) "
                        "cannot have more than one dimension name: {names}"
                    )
                    raise ValueError(msg)
        return self

    @field_validator("output_format")
    @classmethod
    def check_format(cls, fmt):
        allowed = {"csv", "parquet"}
        if fmt not in allowed:
            msg = f"output_format={fmt} is not supported. Allowed={allowed}"
            raise ValueError(msg)
        return fmt

    @model_validator(mode="after")
    def check_column_type(self) -> "QueryResultParamsModel":
        if self.column_type == ColumnType.DIMENSION_TYPES:
            for agg in self.aggregations:
                for dim_type in DimensionType:
                    columns = getattr(agg.dimensions, dim_type.value)
                    if len(columns) > 1:
                        msg = f"Multiple columns are incompatible with {self.column_type=}. {columns=}"
                        raise ValueError(msg)
        return self


class QueryBaseModel(CacheableQueryBaseModel, abc.ABC):
    """Base class for all queries"""

    name: str = Field(description="Name of query")
    # TODO #204: This field is not being used. Wait until development slows down.
    version: str = Field(
        description="Version of the query structure. Changes to the major or minor version invalidate cached tables.",
        default=str(QUERY_FORMAT_VERSION),  # TODO: str shouldn't be required
    )
    result: QueryResultParamsModel = Field(
        default=QueryResultParamsModel(),
        description="Controls the output results",
    )

    def serialize_cached_content(self) -> dict[str, Any]:
        """Return a JSON-able representation of the model that can be used for caching purposes."""
        return self.model_dump(mode="json", exclude={"name"})


class ProjectQueryModel(QueryBaseModel):
    """Represents a user query on a Project."""

    project: ProjectQueryParamsModel = Field(
        description="Defines the datasets to use and how to transform them.",
    )

    def serialize_cached_content(self) -> dict[str, Any]:
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        exclude = {
            "spark_conf_per_dataset",  # Doesn't change the query.
            "version",  # We use the project major version as a separate field.
        }
        return self.project.model_dump(mode="json", exclude=exclude)


class DatasetQueryModel(QueryBaseModel):
    """Defines how to transform a dataset"""

    dataset_id: str = Field(description="Dataset ID for query")
    to_dimension_references: list[DimensionReferenceModel] = Field(
        description="Map the dataset to these dimensions. Mappings must exist in the registry. "
        "There cannot be duplicate mappings."
    )
    mapping_plan: DatasetMappingPlan | None = Field(
        default=None,
        description="Defines the order in which to map the dimensions of the dataset.",
    )
    time_based_data_adjustment: TimeBasedDataAdjustmentModel = Field(
        description="Defines how the rest of the dataframe is adjusted with respect to time. "
        "E.g., when drop associated data when dropping a leap day timestamp.",
        default=TimeBasedDataAdjustmentModel(),
    )
    wrap_time_allowed: bool = Field(
        default=False,
        description="Whether to allow dataset time to be wrapped to the destination time "
        "dimension, if different.",
    )
    result: QueryResultParamsModel = Field(
        default=QueryResultParamsModel(),
        description="Controls the output results",
    )


def make_dataset_query(
    name: str,
    dataset_id: str,
    to_dimension_references: list[DimensionReferenceModel],
    plan: DatasetMappingPlan | None = None,
) -> DatasetQueryModel:
    """Create a query to map a dataset to alternate dimensions.

    Parameters
    ----------
    dataset_id: str
    plan: DatasetMappingPlan | None
        Optional plan to control the mapping operation.
    """
    plans: list[DatasetMappingPlan] = []
    if plan is not None:
        plans.append(plan)
    return DatasetQueryModel(
        name=name,
        dataset_id=dataset_id,
        to_dimension_references=to_dimension_references,
        mapping_plan=plan,
    )


def make_query_for_standalone_dataset(
    project_id: str,
    dataset_id: str,
    plan: DatasetMappingPlan | None = None,
    column_type: ColumnType = ColumnType.DIMENSION_NAMES,
) -> ProjectQueryModel:
    """Create a query to map a standalone dataset to a project's dimensions.

    Parameters
    ----------
    project_id: str
    dataset_id: str
    plan: DatasetMappingPlan | None
        Optional plan to control the mapping operation.
    column_type: ColumnType
        The type of columns in the result table. Default is ColumnType.DIMENSION_NAMES.
    """
    plans: list[DatasetMappingPlan] = []
    if plan is not None:
        plans.append(plan)
    return ProjectQueryModel(
        name=dataset_id,
        project=ProjectQueryParamsModel(
            project_id=project_id,
            dataset=DatasetModel(
                dataset_id=dataset_id,
                source_datasets=[StandaloneDatasetModel(dataset_id=dataset_id)],
            ),
            mapping_plans=plans,
        ),
        result=QueryResultParamsModel(
            column_type=column_type,
        ),
    )


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

    def serialize_cached_content(self) -> dict[str, Any]:
        # Exclude all result-oriented fields in orer to faciliate re-using queries.
        return self.project.model_dump(mode="json", exclude="spark_conf_per_dataset")


class CompositeDatasetQueryModel(QueryBaseModel):
    """Represents a user query on a dataset."""

    dataset_id: str = Field(description="Aggregated Dataset ID for query")
    result: QueryResultParamsModel = Field(
        description="Controls the output results", default=QueryResultParamsModel()
    )
