import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pytest

from dsgrid.utils.run_command import check_run_command
from tests.common import replace_dimension_uuids_from_registry


DATA_REPO = os.environ.get("US_DATA_REPO")


@pytest.fixture
def test_data_dir():
    if DATA_REPO is None:
        print(
            "You must define the environment US_DATA_REPO with the path to the "
            "dsgrid-data-UnitedStates repository"
        )
        sys.exit(1)

    tmpdir = Path(gettempdir()) / "test_us_data"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    shutil.copytree(Path(DATA_REPO) / "dsgrid_project", tmpdir / "dsgrid_project")
    yield tmpdir / "dsgrid_project"
    shutil.rmtree(tmpdir)


def create_registry(tmpdir):
    path = Path(tmpdir) / "dsgrid-registry"
    check_run_command(f"dsgrid registry create {path}")
    assert path.exists()
    assert (path / "projects").exists()
    assert (path / "datasets").exists()
    assert (path / "dimensions").exists()
    return path


def test_register_project_and_dataset(test_data_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        regex_project = re.compile(r"Projects.*\n.*test")
        regex_dataset = re.compile(r"Datasets.*\n.*comstock")

        path = create_registry(base_dir)
        dimension_config = test_data_dir / "dimension.toml"
        check_run_command(f"dsgrid registry --path={path} register-dimension {dimension_config}")

        project_config = test_data_dir / "project.toml"
        dataset_config = test_data_dir / "datasets/input/sector_models/comstock/dataset.toml"
        replace_dimension_uuids_from_registry(path, (project_config, dataset_config))

        check_run_command(f"dsgrid registry --path={path} register-project {project_config}")
        output = {}
        check_run_command(f"dsgrid registry --path={path} list", output)
        assert regex_project.search(output["stdout"]) is not None
        assert "test" in output["stdout"]

        check_run_command(
            f"dsgrid registry --path={path} submit-dataset {dataset_config} -p test -l log"
        )
        output = {}
        check_run_command(f"dsgrid registry --path={path} list", output)
        assert regex_dataset.search(output["stdout"]) is not None
