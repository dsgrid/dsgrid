import os
import sys
from tempfile import NamedTemporaryFile
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query

from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, FileResponse

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.config.dimensions import create_dimension_common_model, create_project_dimension_model
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotStored
from dsgrid.loggers import setup_logging
from dsgrid.query.models import (
    ReportType,
    TableFormatType,
)
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.run_command import run_command
from dsgrid.utils.spark import init_spark
from .api_manager import ApiManager
from .models import (
    AsyncTaskStatus,
    AsyncTaskType,
    ProjectQueryAsyncResultModel,
    SparkSubmitProjectQueryRequest,
)
from .response_models import (
    GetAsyncTaskResponse,
    GetDatasetResponse,
    GetDimensionResponse,
    GetProjectBaseDimensionQueryNameResponse,
    GetProjectDimensionQueryNamesResponse,
    ListProjectDimensionsResponse,
    GetProjectResponse,
    ListAsyncTasksResponse,
    ListDatasetsResponse,
    ListDimensionRecordsResponse,
    ListDimensionTypesResponse,
    ListDimensionsResponse,
    ListProjectSupplementalDimensionQueryNames,
    ListProjectsResponse,
    ListReportTypesResponse,
    ListTableFormatTypesResponse,
    SparkSubmitProjectQueryResponse,
)


logger = setup_logging(__name__, "dsgrid_api.log")
DSGRID_REGISTRY_DATABASE_URL = os.environ.get("DSGRID_REGISTRY_DATABASE_URL")
if DSGRID_REGISTRY_DATABASE_URL is None:
    raise Exception("The environment variable DSGRID_REGISTRY_DATABASE_URL must be set.")
DSGRID_REGISTRY_DATABASE_NAME = os.environ.get("DSGRID_REGISTRY_DATABASE_NAME")
if DSGRID_REGISTRY_DATABASE_NAME is None:
    raise Exception("The environment variable DSGRID_REGISTRY_DATABASE_NAME must be set.")
QUERY_OUTPUT_DIR = os.environ.get("DSGRID_QUERY_OUTPUT_DIR")
if QUERY_OUTPUT_DIR is None:
    raise Exception("The environment variable DSGRID_QUERY_OUTPUT_DIR must be set.")
API_SERVER_STORE_DIR = os.environ.get("DSGRID_API_SERVER_STORE_DIR")
if API_SERVER_STORE_DIR is None:
    raise Exception("The environment variable DSGRID_API_SERVER_STORE_DIR must be set.")

offline_mode = True
no_prompts = True
# There could be collisions on the only-allowed SparkSession between the main process and
# subprocesses that run queries.
# If both processes try to use the Hive metastore, a crash will occur.
spark = init_spark("dsgrid_api", check_env=False)
conn = DatabaseConnection.from_url(
    DSGRID_REGISTRY_DATABASE_URL, database=DSGRID_REGISTRY_DATABASE_NAME
)
manager = RegistryManager.load(
    conn, REMOTE_REGISTRY, offline_mode=offline_mode, no_prompts=no_prompts
)
api_mgr = ApiManager(API_SERVER_STORE_DIR, manager)

# Current limitations:
# This can only run in one process. State is tracked in memory. This could be solved by
# storing state in a database like Redis or MongoDB.
# Deployment strategy is TBD.
app = FastAPI(swagger_ui_parameters={"tryItOutEnabled": True})
app.add_middleware(GZipMiddleware, minimum_size=1024)
origins = [
    "http://localhost",
    "https://localhost",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Welcome to the dsgrid API!"}


# TODO: Filtering?
@app.get("/projects", response_model=ListProjectsResponse)
async def list_projects():
    """List the projects."""
    mgr = manager.project_manager
    return ListProjectsResponse(
        projects=[mgr.get_by_id(x).model for x in mgr.list_ids()],
    )


@app.get("/projects/{project_id}", response_model=GetProjectResponse)
async def get_project(project_id: str):
    """Return the project with project_ID."""
    mgr = manager.project_manager
    return GetProjectResponse(
        project=mgr.get_by_id(project_id).model,
    )


@app.get(
    "/projects/{project_id}/dimensions",
    response_model=ListProjectDimensionsResponse,
)
async def list_project_dimensions(project_id: str):
    """List the project's dimensions."""
    mgr = manager.project_manager
    project = mgr.get_by_id(project_id)
    dimensions = []
    for item in project.get_dimension_query_names_model().dict().values():
        dimension = create_project_dimension_model(project.get_dimension(item["base"]).model, True)
        dimensions.append(dimension)
        for query_name in item["supplemental"]:
            dimension = create_project_dimension_model(
                project.get_dimension(query_name).model, False
            )
            dimensions.append(dimension)

    return ListProjectDimensionsResponse(project_id=project_id, dimensions=dimensions)


@app.get(
    "/projects/{project_id}/dimensions/dimension_query_names",
    response_model=GetProjectDimensionQueryNamesResponse,
)
async def get_project_dimension_query_names(project_id: str):
    """List the base and supplemental dimension query names for the project by type."""
    mgr = manager.project_manager
    project = mgr.get_by_id(project_id)
    return GetProjectDimensionQueryNamesResponse(
        project_id=project_id,
        dimension_query_names=project.get_dimension_query_names_model(),
    )


@app.get(
    "/projects/{project_id}/dimensions/base_dimension_query_name/{dimension_type}",
    response_model=GetProjectBaseDimensionQueryNameResponse,
)
async def get_project_base_dimension_query_name(project_id: str, dimension_type: DimensionType):
    """Get the project's base dimension query name for the given dimension type."""
    mgr = manager.project_manager
    config = mgr.get_by_id(project_id)
    return GetProjectBaseDimensionQueryNameResponse(
        project_id=project_id,
        dimension_type=dimension_type,
        dimension_query_name=config.get_base_dimension(dimension_type).model.dimension_query_name,
    )


@app.get(
    "/projects/{project_id}/dimensions/supplemental_dimension_query_names/{dimension_type}",
    response_model=ListProjectSupplementalDimensionQueryNames,
)
async def list_project_supplemental_dimension_query_names(
    project_id: str, dimension_type: DimensionType
):
    """list the project's supplemental dimension query names for the given dimension type."""
    mgr = manager.project_manager
    config = mgr.get_by_id(project_id)
    return ListProjectSupplementalDimensionQueryNames(
        project_id=project_id,
        dimension_type=dimension_type,
        dimension_query_names=[
            x.model.dimension_query_name
            for x in config.list_supplemental_dimensions(
                dimension_type, sort_by="dimension_query_name"
            )
        ],
    )


@app.get(
    "/projects/{project_id}/dimensions/dimensions_by_query_name/{dimension_query_name}",
    response_model=GetDimensionResponse,
)
async def get_project_dimension(project_id: str, dimension_query_name: str):
    """Get the project's dimension for the given dimension query name."""
    mgr = manager.project_manager
    config = mgr.get_by_id(project_id)
    return GetDimensionResponse(
        dimension=create_dimension_common_model(config.get_dimension(dimension_query_name).model)
    )


# TODO: Add filtering by project_id
@app.get("/datasets", response_model=ListDatasetsResponse)
async def list_datasets():
    """list the datasets."""
    mgr = manager.dataset_manager
    return ListDatasetsResponse(
        datasets=[mgr.get_by_id(x).model for x in mgr.list_ids()],
    )


@app.get("/datasets/{dataset_id}", response_model=GetDatasetResponse)
async def get_dataset(dataset_id: str):
    """Return the dataset with dataset_id."""
    mgr = manager.dataset_manager
    return GetDatasetResponse(dataset=mgr.get_by_id(dataset_id).model)


@app.get("/dimensions/types", response_model=ListDimensionTypesResponse)
async def list_dimension_types():
    """List the dimension types."""
    return ListDimensionTypesResponse(types=_list_enums(DimensionType))


# TODO: Add filtering for dimension IDs
@app.get("/dimensions", response_model=ListDimensionsResponse)
async def list_dimensions(dimension_type: DimensionType | None = None):
    """List the dimensions for the given type."""
    mgr = manager.dimension_manager
    return ListDimensionsResponse(
        dimensions=[
            create_dimension_common_model(mgr.get_by_id(x).model)
            for x in mgr.list_ids(dimension_type=dimension_type)
        ],
    )


@app.get("/dimensions/{dimension_id}", response_model=GetDimensionResponse)
async def get_dimension(dimension_id: str):
    """Get the dimension for the dimension_id."""
    mgr = manager.dimension_manager
    return GetDimensionResponse(
        dimension=create_dimension_common_model(mgr.get_by_id(dimension_id).model)
    )


@app.get("/dimensions/records/{dimension_id}", response_model=ListDimensionRecordsResponse)
async def list_dimension_records(dimension_id: str):
    """List the records for the dimension ID."""
    mgr = manager.dimension_manager
    model = mgr.get_by_id(dimension_id).model
    records = [] if model.dimension_type == DimensionType.TIME else model.records
    return ListDimensionRecordsResponse(records=records)


@app.get("/reports/types", response_model=ListReportTypesResponse)
async def list_report_types():
    """List the report types available for queries."""
    return ListReportTypesResponse(types=_list_enums(ReportType))


@app.get("/table_formats/types", response_model=ListTableFormatTypesResponse)
async def list_table_format_types():
    """List the table format types available for query results."""
    return ListTableFormatTypesResponse(types=_list_enums(TableFormatType))


@app.post("/queries/projects", response_model=SparkSubmitProjectQueryResponse)
async def submit_project_query(
    query: SparkSubmitProjectQueryRequest, background_tasks: BackgroundTasks
):
    """Submit a project query for execution."""
    if not api_mgr.can_start_new_async_task():
        # TODO: queue the task and run it later.
        raise HTTPException(422, "Too many async tasks are already running")
    async_task_id = api_mgr.initialize_async_task(AsyncTaskType.PROJECT_QUERY)
    # TODO: how to handle the output directory on the server?
    # TODO: force should not be True
    # TODO: how do we manage the number of background tasks?
    background_tasks.add_task(_submit_project_query, query, async_task_id)
    return SparkSubmitProjectQueryResponse(async_task_id=async_task_id)


@app.get("/async_tasks/status", response_model=ListAsyncTasksResponse)
def list_async_tasks(
    async_task_ids: list[int] | None = Query(default=None), status: AsyncTaskStatus | None = None
):
    """Return the async tasks. Filter results by async task ID or status."""
    return ListAsyncTasksResponse(
        async_tasks=api_mgr.list_async_tasks(async_task_ids=async_task_ids, status=status)
    )


@app.get("/async_tasks/status/{async_task_id}", response_model=GetAsyncTaskResponse)
def get_async_task_status(async_task_id: int):
    """Return the async task."""
    try:
        result = api_mgr.list_async_tasks(async_task_ids=[async_task_id])
        assert len(result) == 1
        return GetAsyncTaskResponse(async_task=result[0])
    except DSGValueNotStored as e:
        raise HTTPException(404, detail=str(e))


@app.get("/async_tasks/data/{async_task_id}")
def get_async_task_data(async_task_id: int):
    """Return the data for a completed async task."""
    task = api_mgr.get_async_task_status(async_task_id)
    if task.status != AsyncTaskStatus.COMPLETE:
        raise HTTPException(
            422,
            detail=f"Data can only be read for completed tasks: async_task_id={async_task_id} status={task.status}",
        )
    if task.task_type == AsyncTaskType.PROJECT_QUERY:
        if not task.result.data_file:
            raise HTTPException(400, f"{task.result.data_file=} is invalid")
        # TODO: Sending data this way has major limitations. We lose all the benefits of Parquet and
        # compression.
        # We should also check how much data we can read through the Spark driver.
        text = (
            spark.read.parquet(str(task.result.data_file))
            .toPandas()
            .to_json(orient="split", index=False)
        )
    else:
        raise NotImplementedError(f"task type {task.task_type} is not implemented")

    return Response(content=text, media_type="application/json")


@app.get("/async_tasks/archive_file/{async_task_id}", response_class=FileResponse)
def download_async_task_archive_file(async_task_id: int):
    """Download the archive file for a completed async task."""
    task = api_mgr.get_async_task_status(async_task_id)
    if task.status != AsyncTaskStatus.COMPLETE:
        raise HTTPException(
            422,
            detail=f"Data can only be downloaded for completed tasks: async_task_id={async_task_id} status={task.status}",
        )
    return FileResponse(task.result.archive_file)


def _submit_project_query(spark_query: SparkSubmitProjectQueryRequest, async_task_id):
    with NamedTemporaryFile(mode="w", suffix=".json") as fp:
        query = spark_query.query
        fp.write(query.json())
        fp.write("\n")
        fp.flush()
        output_dir = Path(QUERY_OUTPUT_DIR)
        dsgrid_exec = "dsgrid-cli.py"
        base_cmd = (
            f"query project run --offline "
            f"--url={DSGRID_REGISTRY_DATABASE_URL} "
            f"--database-name={DSGRID_REGISTRY_DATABASE_NAME} "
            f"--output={output_dir} --zip-file --force {fp.name}"
        )
        if spark_query.use_spark_submit:
            # Need to find the full path to pass to spark-submit.
            dsgrid_exec = _find_exec(dsgrid_exec)
            spark_cmd = "spark-submit"
            if spark_query.spark_submit_options:
                spark_cmd += " " + " ".join(
                    (f"{k} {v}" for k, v in spark_query.spark_submit_options.items())
                )
            cmd = f"{spark_cmd} {dsgrid_exec} {base_cmd}"
        else:
            cmd = f"{dsgrid_exec} {base_cmd}"
        logger.info(f"Submitting project query command: {cmd}")
        ret = run_command(cmd)
        if ret == 0:
            data_dir = output_dir / query.name / "table.parquet"
            zip_filename = str(output_dir / query.name) + ".zip"
            result = ProjectQueryAsyncResultModel(
                # metadata=load_data(output_dir / query.name / "metadata.json"),
                data_file=str(data_dir),
                archive_file=str(zip_filename),
                archive_file_size_mb=os.stat(zip_filename).st_size / 1_000_000,
            )
        else:
            logger.error("Failed to submit a project query: return_code=%s", ret)
            result = ProjectQueryAsyncResultModel(
                # metadata={},
                data_file="",
                archive_file="",
                archive_file_size_mb=0,
            )

    api_mgr.complete_async_task(async_task_id, ret, result=result)


def _find_exec(name):
    for path in sys.path:
        exec_path = Path(path) / name
        if exec_path.exists():
            return exec_path
    raise Exception(f"Did not find {name}")


def _list_enums(enum_type):
    return sorted([x.value for x in enum_type])
