import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import gettempdir

import pytest

from dsgrid.registry.registry_database import DatabaseConnection, RegistryDatabase
from dsgrid.utils.files import load_data
from dsgrid.utils.run_command import run_command, check_run_command
from dsgrid.utils.spark import init_spark, get_spark_session
from dsgrid.tests.common import (
    TEST_DATASET_DIRECTORY,
    TEST_PROJECT_PATH,
    TEST_PROJECT_REPO,
    TEST_REGISTRY_DATABASE,
    TEST_REGISTRY_PATH,
    TEST_STANDARD_SCENARIOS_PROJECT_REPO,
    TEST_EFS_REGISTRATION_FILE,
)


def pytest_sessionstart(session):
    if not os.listdir(TEST_PROJECT_PATH):
        print(
            "The dsgrid-test-data submodule has not been initialized. "
            "Please run these commands:"
        )
        print("git submodule init")
        print("git submodule update")
        sys.exit(1)

    # Previous versions of this database can cause problems in error conditions.
    path = Path("metastore_db")
    if path.exists():
        shutil.rmtree(path)


@pytest.fixture(scope="session")
def cached_registry():
    """Creates a shared registry that is is only rebuilt after a new commit."""
    data = load_data(TEST_EFS_REGISTRATION_FILE)
    assert data["data_path"] == str(TEST_REGISTRY_PATH)
    conn = DatabaseConnection(**data["conn"])
    assert conn.database == TEST_REGISTRY_DATABASE
    commit_file = TEST_REGISTRY_PATH / "commit.txt"
    latest_commit = _get_latest_commit()

    if (
        TEST_REGISTRY_PATH.exists()
        and commit_file.exists()
        and commit_file.read_text().strip() == latest_commit
    ):
        print(f"Use existing test registry at {TEST_REGISTRY_PATH}.")
    else:
        if TEST_REGISTRY_PATH.exists():
            shutil.rmtree(TEST_REGISTRY_PATH)
        ret = run_command(f"python dsgrid/tests/register.py {TEST_EFS_REGISTRATION_FILE}")
        if ret == 0:
            print("make script returned 0")
            commit_file.write_text(latest_commit + "\n")
        elif TEST_REGISTRY_PATH.exists():
            print("make script returned non-zero:", ret)
            # Delete it because it is invalid.
            shutil.rmtree(TEST_REGISTRY_PATH)
            RegistryDatabase.delete(conn)
            sys.exit(1)

    yield conn


def _get_latest_commit():
    output = {}
    check_run_command("git log -n 1", output=output)
    match = re.search(r"^commit (\w+)", output["stdout"])
    assert match, output
    commit = match.group(1)
    return commit


@pytest.fixture(scope="module")
def setup_api_server():
    yield
    for path in (os.environ["DSGRID_QUERY_OUTPUT_DIR"], os.environ["DSGRID_API_SERVER_STORE_DIR"]):
        if os.path.exists(path):
            shutil.rmtree(path)


@pytest.fixture
def spark_session():
    spark = init_spark("dsgrid_test")
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def make_test_project_dir_module():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    if tmpdir.exists():
        shutil.rmtree(tmpdir)


@pytest.fixture
def make_test_project_dir():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    if tmpdir.exists():
        shutil.rmtree(tmpdir)


@pytest.fixture
def make_standard_scenarios_project_dir():
    tmpdir = _make_project_dir(TEST_STANDARD_SCENARIOS_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    if tmpdir.exists():
        shutil.rmtree(tmpdir)


@pytest.fixture(scope="module")
def make_test_data_dir_module():
    tmpdir = Path(gettempdir()) / "test_data"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    dst_path = tmpdir / "datasets"
    shutil.copytree(Path(TEST_DATASET_DIRECTORY), dst_path)
    yield dst_path
    if tmpdir.exists():
        shutil.rmtree(tmpdir)


@pytest.fixture
def make_test_data_dir(tmp_path):
    dst_path = tmp_path / "datasets"
    shutil.copytree(Path(TEST_DATASET_DIRECTORY), dst_path)
    yield dst_path


def _make_project_dir(project):
    tmpdir = Path(gettempdir()) / "test_project"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")
    return tmpdir


@pytest.fixture
def tmp_registry_db(make_test_project_dir, tmp_path):
    database_name = "tmp-dsgrid"
    conn = DatabaseConnection(database=database_name)
    RegistryDatabase.delete(conn)
    yield make_test_project_dir, tmp_path, database_name
    RegistryDatabase.delete(conn)


@pytest.fixture
def spark_time_zone(request):
    spark = get_spark_session()
    orig = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", request.param)
    yield
    spark.conf.set("spark.sql.session.timeZone", orig)
