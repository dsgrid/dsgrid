from pathlib import Path
from tempfile import gettempdir
import os
import pytest
import shutil
import sys

from dsgrid.registry.registry_manager import RegistryManager


PROJECT_REPO = os.environ.get("TEST_PROJECT_REPO")
LOCAL_DATA_DIRECTORY = os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY")


@pytest.fixture
def make_test_project_dir():
    if PROJECT_REPO is None:
        print(
            "You must define the environment TEST_PROJECT_REPO with the path to the "
            "dsgrid-data-UnitedStates repository"
        )
        sys.exit(1)
    if LOCAL_DATA_DIRECTORY is None:
        print(
            "You must define the environment DSGRID_LOCAL_DATA_DIRECTORY with the path to your "
            "copy of datasets."
        )
        sys.exit(1)

    tmpdir = Path(gettempdir()) / "test_us_data"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(Path(PROJECT_REPO) / "dsgrid_project", tmpdir / "dsgrid_project")
    yield tmpdir / "dsgrid_project"
    shutil.rmtree(tmpdir)


def create_local_test_registry(tmpdir):
    path = Path(tmpdir)
    RegistryManager.create(path)
    assert path.exists()
    assert (path / "configs/projects").exists()
    assert (path / "configs/datasets").exists()
    assert (path / "configs/dimensions").exists()
    assert (path / "configs/dimension_mappings").exists()
    return path
