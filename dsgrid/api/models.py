import enum
from datetime import datetime

from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.query.models import ProjectQueryModel


class AsyncTaskStatus(enum.Enum):
    """Statuses for async operations"""

    QUEUED = "queued"  # not used yet
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    CANCELED = "canceled"  # not used yet


class AsyncTaskType(enum.Enum):
    """Asynchronous task types"""

    PROJECT_QUERY = "project_query"


class ProjectQueryAsyncResultModel(DSGBaseModel):
    # metadata: DatasetMetadataModel  # TODO: not sure if we need this
    data_file: str
    archive_file: str
    archive_file_size_mb: float


class AsyncTaskModel(DSGBaseModel):
    """Tracks an asynchronous operation."""

    async_task_id: int
    task_type: AsyncTaskType
    status: AsyncTaskStatus
    return_code: int | None = None
    result: ProjectQueryAsyncResultModel | None = None  # eventually, union of all result types
    start_time: datetime
    completion_time: datetime | None = None


class StoreModel(DSGBaseModel):
    next_async_task_id: int = 1
    async_tasks: dict[int, AsyncTaskModel] = {}
    outstanding_async_tasks: set[int] = set()


class SparkSubmitProjectQueryRequest(DSGBaseModel):
    use_spark_submit: bool = Field(
        default=True,
        description="If True, run the query command through spark-submit. If False, run the "
        "command directly in dsgrid.",
    )
    spark_submit_options: dict[str, str] = Field(
        default={},
        description="Options to forward to the spark-submit command (e.g., --master spark://hostname:7077",
    )
    query: ProjectQueryModel
