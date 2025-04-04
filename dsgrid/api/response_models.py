from dsgrid.data_models import DSGBaseModel
from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.config.dimensions import DimensionCommonModel, ProjectDimensionModel
from dsgrid.config.project_config import ProjectConfigModel, ProjectDimensionNamesModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import ReportType
from .models import AsyncTaskModel


class ListProjectsResponse(DSGBaseModel):
    """Defines the reponse to the list_projects command."""

    projects: list[ProjectConfigModel]


class GetProjectResponse(DSGBaseModel):
    """Defines the reponse to the get_project command."""

    project: ProjectConfigModel


class ListDatasetsResponse(DSGBaseModel):
    """Defines the reponse to the list_datasets command."""

    datasets: list[DatasetConfigModel]


class GetDatasetResponse(DSGBaseModel):
    """Defines the reponse to the get_dataset command."""

    dataset: DatasetConfigModel


class ListProjectDimensionsResponse(DSGBaseModel):
    """Defines the reponse to the list_project_dimensions command."""

    project_id: str
    dimensions: list[ProjectDimensionModel]


class GetProjectDimensionNamesResponse(DSGBaseModel):
    """Defines the reponse to the get_project_dimension_names command."""

    project_id: str
    dimension_names: ProjectDimensionNamesModel


class GetProjectBaseDimensionNameResponse(DSGBaseModel):
    """Defines the reponse to the get_project_dimension_name command."""

    project_id: str
    dimension_type: DimensionType
    dimension_name: str


class ListProjectSupplementalDimensionNames(DSGBaseModel):
    """Defines the response to the list_project_supplemental_dimension_names command"""

    project_id: str
    dimension_type: DimensionType
    dimension_names: list[str]


class ListDimensionTypesResponse(DSGBaseModel):
    """Defines the response to the list_dimension_types command."""

    types: list[DimensionType]


class ListDimensionsResponse(DSGBaseModel):
    """Defines the response to the list_dimensions command."""

    dimensions: list[DimensionCommonModel]


class GetDimensionResponse(DSGBaseModel):
    """Defines the response to the get_dimension command."""

    dimension: DimensionCommonModel


class ListDimensionRecordsResponse(DSGBaseModel):
    """Defines the response to the list_dimension_records command."""

    records: list[dict]


class ListReportTypesResponse(DSGBaseModel):
    """Defines the response to the list_report_types command."""

    types: list[ReportType]


class ListTableFormatTypesResponse(DSGBaseModel):
    """Defines the response to the list_table_format_types command."""

    types: list[TableFormatType]


class SparkSubmitProjectQueryResponse(DSGBaseModel):
    """Defines the response to the submit_project_query command."""

    async_task_id: int


class ListAsyncTasksResponse(DSGBaseModel):
    """Defines the response to the list_async_tasks command."""

    async_tasks: list[AsyncTaskModel]


class GetAsyncTaskResponse(DSGBaseModel):
    """Defines the response to the list_async_tasks command."""

    async_task: AsyncTaskModel
