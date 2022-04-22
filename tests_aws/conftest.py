import os
import shutil
from pathlib import Path
from tempfile import gettempdir

import pytest

from dsgrid.tests.common import (
    TEST_DATASET_DIRECTORY,
    TEST_PROJECT_REPO,
)


@pytest.fixture
def make_test_project_dir():
    tmpdir = _make_project_dir(TEST_PROJECT_REPO)
    yield tmpdir / "dsgrid_project"
    shutil.rmtree(tmpdir)


@pytest.fixture
def make_test_data_dir():
    tmpdir = Path(gettempdir()) / "test_data"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    dst_path = tmpdir / "datasets"
    shutil.copytree(Path(TEST_DATASET_DIRECTORY), dst_path)
    yield dst_path
    shutil.rmtree(tmpdir)


def _make_project_dir(project):
    tmpdir = Path(gettempdir()) / "test_project"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(project / "dsgrid_project", tmpdir / "dsgrid_project")
    return tmpdir
