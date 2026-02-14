import gc
import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import gettempdir
from typing import Optional

import pytest
from click.testing import CliRunner

from dsgrid.cli.dsgrid import cli
from dsgrid.registry.common import DataStoreType, DatabaseConnection, make_sqlite_url
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.functions import (
    drop_temp_tables_and_views,
    get_current_time_zone,
    set_current_time_zone,
)
from dsgrid.spark.types import use_duckdb
from dsgrid.registry.registry_database import RegistryDatabase
from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import init_spark
from dsgrid.tests.common import (
    TEST_DATASET_DIRECTORY,
    TEST_PROJECT_PATH,
    TEST_PROJECT_REPO,
    TEST_REGISTRY_BASE_PATH,
    TEST_REGISTRY_DATA_PATH,
    TEST_STANDARD_SCENARIOS_PROJECT_REPO,
    TEST_EFS_REGISTRATION_FILE,
    CACHED_TEST_REGISTRY_DB,
)
from dsgrid.tests.make_us_data_registry import update_dataset_config_paths
from dsgrid.utils.files import delete_if_exists, load_data
from dsgrid.tests.make_us_data_registry import make_test_data_registry


def pytest_sessionstart(session):
    if not os.listdir(TEST_PROJECT_PATH):
        print(
            "The dsgrid-test-data submodule has not been initialized. Please run these commands:"
        )
        print("git submodule init")
        print("git submodule update")
        sys.exit(1)

    # Previous versions of this database can cause problems in error conditions.
    path = Path("metastore_db")
    if path.exists():
        shutil.rmtree(path)


def pytest_sessionfinish(session, exitstatus):
    drop_temp_tables_and_views()


@pytest.fixture(scope="session")
def cached_registry():
    """Creates a shared registry that is is only rebuilt after a new commit.
    Tests must not make any changes to this registry.
    Refer to :func:`~mutable_cached_registry` if that is needed.
    """
    commit_file = TEST_REGISTRY_BASE_PATH / "commit.txt"
    latest_commit = _get_latest_commit()
    conn = DatabaseConnection(url=CACHED_TEST_REGISTRY_DB)
    if commit_file.exists() and commit_file.read_text().strip() == latest_commit:
        print(f"Use existing test registry at {TEST_REGISTRY_BASE_PATH}.")
    else:
        delete_if_exists(TEST_REGISTRY_BASE_PATH)
        TEST_REGISTRY_BASE_PATH.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "create-registry",
                conn.url,
                "--data-path",
                str(TEST_REGISTRY_DATA_PATH),
                "--overwrite",
            ],
        )
        assert result.exit_code == 0
        result = runner.invoke(
            cli,
            [
                "--url",
                conn.url,
                "registry",
                "bulk-register",
                str(TEST_EFS_REGISTRATION_FILE),
            ],
        )
        if result.exit_code == 0:
            commit_file.write_text(latest_commit + "\n")
        elif TEST_REGISTRY_DATA_PATH.exists():
            print("bulk-register returned non-zero:", result.exit_code)
            print("Output:", result.output)
            if result.exception:
                import traceback

                print("Exception:")
                traceback.print_exception(
                    type(result.exception),
                    result.exception,
                    result.exception.__traceback__,
                )
            # Delete it because it is invalid.
            delete_if_exists(TEST_REGISTRY_DATA_PATH)
            RegistryDatabase.delete(conn)
            sys.exit(1)

    yield conn


@pytest.fixture(scope="session")
def src_tmp_registry_db(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("tmpdir")
    project_dir = _make_project_dir(TEST_PROJECT_REPO, base_dir=tmp_path) / "dsgrid_project"
    conn = DatabaseConnection.from_file(tmp_path / "tmp_reg.db")
    RegistryDatabase.delete(conn)
    registry_dir = tmp_path_factory.mktemp("registry_data")
    with make_test_data_registry(
        registry_dir,
        project_dir,
        database_url=conn.url,
    ):
        pass  # Manager is created and disposed automatically
    yield conn, project_dir
    RegistryDatabase.delete(conn)


@pytest.fixture
def registry_with_duckdb_store(tmp_path):
    db_file = tmp_path / "duckdb_registry.db"
    url = make_sqlite_url(db_file)
    data_path = tmp_path / "registry_data"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "create-registry",
            url,
            "--data-path",
            str(data_path),
            "--overwrite",
            "--data-store-type",
            DataStoreType.DUCKDB.value,
        ],
    )
    assert result.exit_code == 0
    cmd = [
        "--url",
        url,
        "registry",
        "bulk-register",
        str(TEST_EFS_REGISTRATION_FILE),
    ]
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    conn = DatabaseConnection(url=url)
    yield conn
    # Dispose the database engine and data store before removing files.
    try:
        db = RegistryDatabase.connect(conn)
        db.dispose()
    except Exception:
        pass
    delete_if_exists(data_path)
    delete_if_exists(db_file)


@pytest.fixture()
def mutable_cached_registry(src_tmp_registry_db, tmp_path) -> tuple[RegistryManager, Path]:
    """Creates a copy of the cached_registry. Tests may make changes to the registry."""
    src_conn, src_project_dir = src_tmp_registry_db
    dst_conn = DatabaseConnection.from_file(tmp_path / "dst_registry.db")
    tmp_project_dir = tmp_path / "tmp_project_dir"
    shutil.copytree(src_project_dir, tmp_project_dir)
    RegistryManager.copy(src_conn, dst_conn, tmp_path / "mutable_registry_data")
    mgr = RegistryManager.load(dst_conn)
    try:
        yield mgr, tmp_project_dir
    finally:
        mgr.dispose()
        gc.collect()


def _get_latest_commit():
    output = {}
    check_run_command("git log -n 1", output=output)
    match = re.search(r"^commit (\w+)", output["stdout"])
    assert match, output
    commit = match.group(1)
    return commit


def spark_session():
    spark = init_spark("dsgrid_test")
    yield spark
    if not use_duckdb():
        spark.stop()


@pytest.fixture(scope="module")
def make_test_project_dir_module():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    delete_if_exists(tmpdir)


@pytest.fixture
def make_test_project_dir():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    delete_if_exists(tmpdir)


@pytest.fixture
def make_standard_scenarios_project_dir():
    tmpdir = _make_project_dir(TEST_STANDARD_SCENARIOS_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    delete_if_exists(tmpdir)


@pytest.fixture(scope="module")
def make_test_data_dir_module():
    tmpdir = Path(gettempdir()) / "test_data"
    delete_if_exists(tmpdir)
    os.mkdir(tmpdir)
    dst_path = tmpdir / "datasets"
    shutil.copytree(Path(TEST_DATASET_DIRECTORY), dst_path)
    yield dst_path
    delete_if_exists(tmpdir)


@pytest.fixture
def make_test_data_dir(tmp_path):
    dst_path = tmp_path / "datasets"
    shutil.copytree(Path(TEST_DATASET_DIRECTORY), dst_path)
    yield dst_path


def _make_project_dir(project, base_dir: Optional[Path] = None):
    tmpdir_base = base_dir or Path(gettempdir())
    tmpdir = tmpdir_base / "test_project"
    delete_if_exists(tmpdir)
    tmpdir.mkdir(parents=True)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")

    # Update dataset config paths to be relative to the copied config files
    datasets_dir = tmpdir / "dsgrid_project" / "datasets"
    if datasets_dir.exists():
        # Match both dataset.json5 and dataset_with_dimension_ids.json5 etc.
        for config_file in datasets_dir.rglob("dataset*.json5"):
            try:
                data = load_data(config_file)
                if "dataset_id" in data and "data_layout" in data:
                    update_dataset_config_paths(config_file, data["dataset_id"])
            except Exception:
                # Some config files may not have valid paths; skip them
                pass

    return tmpdir


@pytest.fixture
def tmp_registry_db(make_test_project_dir, tmp_path):
    conn = DatabaseConnection.from_file(tmp_path / "registry.db")
    RegistryDatabase.delete(conn)
    registry_path = tmp_path / "registry"
    registry_path.mkdir()
    yield make_test_project_dir, registry_path, conn.url
    gc.collect()
    RegistryDatabase.delete(conn)


@pytest.fixture
def spark_time_zone(request):
    orig = get_current_time_zone()
    set_current_time_zone(request.param)
    yield
    set_current_time_zone(orig)


@pytest.fixture
def scratch_dir_context(tmp_path):
    with ScratchDirContext(tmp_path) as context:
        yield context
