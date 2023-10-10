import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from dsgrid.dimension.base_models import DimensionType
from dsgrid.api.models import AsyncTaskStatus, SparkSubmitProjectQueryRequest
from dsgrid.api.response_models import (
    GetAsyncTaskResponse,
    GetDatasetResponse,
    GetDimensionResponse,
    GetProjectBaseDimensionQueryNameResponse,
    GetProjectDimensionQueryNamesResponse,
    GetProjectResponse,
    ListAsyncTasksResponse,
    ListDatasetsResponse,
    ListDimensionRecordsResponse,
    ListDimensionTypesResponse,
    ListDimensionsResponse,
    ListProjectSupplementalDimensionQueryNames,
    ListProjectsResponse,
    ListProjectDimensionsResponse,
    ListReportTypesResponse,
    ListTableFormatTypesResponse,
    SparkSubmitProjectQueryResponse,
)
from dsgrid.query.models import (
    ReportType,
    TableFormatType,
)
from dsgrid.utils.files import load_data

logger = logging.getLogger(__name__)
# These env variables need to be set before the app is imported.
os.environ["DSGRID_REGISTRY_DATABASE_URL"] = "http://localhost:8529"
os.environ["DSGRID_REGISTRY_DATABASE_NAME"] = "simple-standard-scenarios"
QUERY_OUTPUT_DIR = Path(tempfile.gettempdir()) / "test_dsgrid_query_output"
os.environ["DSGRID_QUERY_OUTPUT_DIR"] = str(QUERY_OUTPUT_DIR)
API_SERVER_STORE_DIR = Path(tempfile.gettempdir()) / "test_dsgrid_api_server"
os.environ["DSGRID_API_SERVER_STORE_DIR"] = str(API_SERVER_STORE_DIR)
for path in (QUERY_OUTPUT_DIR, API_SERVER_STORE_DIR):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()


from dsgrid.api.app import API_SERVER_STORE_DIR, app  # noqa: E402

client = TestClient(app)
PROJECT_ID = "dsgrid_conus_2022"


def test_api_root():
    assert check_response("/").json()["message"] == "Welcome to the dsgrid API!"


def test_openapi():
    response = check_response("/openapi.json")
    assert "openapi" in response.json()


def test_list_projects():
    response = ListProjectsResponse(**check_response("/projects").json())
    assert len(response.projects) == 1
    assert response.projects[0].project_id == PROJECT_ID


def test_get_project():
    response = GetProjectResponse(**check_response(f"/projects/{PROJECT_ID}").json())
    assert response.project.project_id == PROJECT_ID


def test_list_datasets():
    response = ListDatasetsResponse(**check_response("/datasets").json())
    assert len(response.datasets) == 12
    assert (
        response.datasets[0].dataset_id == "aeo2021_reference_commercial_energy_use_growth_factors"
    )


def test_get_dataset():
    response = GetDatasetResponse(
        **check_response("/datasets/comstock_conus_2022_reference").json()
    )
    assert response.dataset.dataset_id == "comstock_conus_2022_reference"


def test_list_project_dimensions():
    response = ListProjectDimensionsResponse(
        **check_response(f"/projects/{PROJECT_ID}/dimensions").json()
    )
    assert response.project_id == PROJECT_ID
    assert response.dimensions


def test_get_project_dimension_query_names():
    response = check_response(f"/projects/{PROJECT_ID}/dimensions/dimension_query_names")
    GetProjectDimensionQueryNamesResponse(**response.json())


def test_get_project_base_dimension_query_name():
    dim = DimensionType.TIME.value
    response = check_response(f"/projects/{PROJECT_ID}/dimensions/base_dimension_query_name/{dim}")
    result = GetProjectBaseDimensionQueryNameResponse(**response.json())
    assert result.dimension_query_name == "time_est"


def test_list_project_supplemental_dimension_query_names():
    dim = DimensionType.GEOGRAPHY.value
    response = check_response(
        f"/projects/{PROJECT_ID}/dimensions/supplemental_dimension_query_names/{dim}"
    )
    result = ListProjectSupplementalDimensionQueryNames(**response.json())
    assert result.dimension_query_names == [
        "all_geographies",
        "census_division",
        "census_region",
        "conus",
        "reeds_pca",
        "state",
    ]


def test_list_dimension_types():
    response = check_response("/dimensions/types")
    result = ListDimensionTypesResponse(**response.json())
    assert result.types == sorted([x for x in DimensionType])


def test_list_dimensions():
    response = check_response("/dimensions")
    result = ListDimensionsResponse(**response.json())
    assert len(result.dimensions)
    found_dimensions = {x.dimension_type for x in result.dimensions}
    assert len(found_dimensions) == len(DimensionType)


def test_list_dimensions_with_type():
    dim = DimensionType.GEOGRAPHY.value
    response = check_response(f"/dimensions?dimension_type={dim}")
    result = ListDimensionsResponse(**response.json())
    assert len(result.dimensions)
    for dimension in result.dimensions:
        assert dimension.dimension_type.value == dim


def test_list_dimension_records():
    dim = DimensionType.GEOGRAPHY.value
    query_name = GetProjectBaseDimensionQueryNameResponse(
        **check_response(
            f"/projects/{PROJECT_ID}/dimensions/base_dimension_query_name/{dim}"
        ).json()
    ).dimension_query_name
    dimension = GetDimensionResponse(
        **check_response(
            f"/projects/{PROJECT_ID}/dimensions/dimensions_by_query_name/{query_name}"
        ).json()
    ).dimension
    records = ListDimensionRecordsResponse(
        **check_response(f"/dimensions/records/{dimension.dimension_id}").json()
    ).records
    assert [x for x in records if x["id"] == "06037"]


def test_list_report_types():
    response = ListReportTypesResponse(**check_response("/reports/types").json())
    assert response.types == sorted(list(ReportType), key=lambda x: x.value)


def test_list_table_format_types():
    response = ListTableFormatTypesResponse(**check_response("/table_formats/types").json())
    assert response.types == sorted(list(TableFormatType), key=lambda x: x.value)


# This doesn't work in all environments, especially Eagle. There are conflicts with the
# metastore_db directory.
@pytest.mark.skip
def test_submit_project_query(setup_api_server):
    query = SparkSubmitProjectQueryRequest(
        use_spark_submit=False,
        query=load_data(Path(__file__).parent / "data" / "simple_query.json5"),
    )
    async_task_id = SparkSubmitProjectQueryResponse(
        **check_response("/queries/projects", data=json.loads(query.json())).json()
    ).async_task_id
    status = GetAsyncTaskResponse(
        **check_response(f"/async_tasks/status/{async_task_id}").json()
    ).async_task.status
    complete = status == AsyncTaskStatus.COMPLETE
    cur_time = time.time()
    end_time = cur_time + 30
    while cur_time < end_time and not complete:
        status = GetAsyncTaskResponse(
            **check_response(f"/async_tasks/status/{async_task_id}").json()
        ).async_task.status
        if status == AsyncTaskStatus.COMPLETE:
            complete = True
        else:
            time.sleep(2)
    assert complete
    # Make sure this command also works.
    other_status = (
        ListAsyncTasksResponse(**check_response("/async_tasks/status/").json())
        .async_tasks[-1]
        .status
    )
    assert other_status == status
    json_text = check_response(f"/async_tasks/data/{async_task_id}").text
    df = pd.read_json(json_text, orient="split")
    assert isinstance(df, pd.DataFrame)

    data = check_response(f"/async_tasks/archive_file/{async_task_id}")
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(data.content)
        fp.flush()
        with ZipFile(fp.name) as zipf:
            names = {os.path.basename(x) for x in zipf.namelist()}
            assert "metadata.json" in names
            assert "query.json" in names


def check_response(endpoint, data=None, expected_status_code=200):
    if data is None:
        logger.debug("Send http get %s", endpoint)
        response = client.get(f"{endpoint}")
    else:
        logger.debug("Send http post %s", endpoint)
        response = client.post(f"{endpoint}", json=data)
    assert response.status_code == expected_status_code
    return response
