import os
import re
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dsgrid.utils.run_command import check_run_command


DATA_REPO = os.environ.get("US_DATA_REPO")
if DATA_REPO is None:
    print(
        "You must define the environment US_DATA_REPO with the path to the "
        "dsgrid-data-UnitedStates repository"
    )
    sys.exit(1)


def create_registry(tmpdir):
    path = Path(tmpdir) / "dsgrid-registry"
    check_run_command(f"dsgrid registry create {path}")
    assert path.exists()
    assert (path / "projects").exists()
    assert (path / "datasets").exists()
    assert (path / "dimensions").exists()
    return path


@pytest.mark.skip(reason="generated dimension_ids do not match the projects and datasets")
def test_register_project_and_dataset():
    with TemporaryDirectory() as tmpdir:
        regex_project = re.compile(r"Projects.*\n.*test")
        regex_dataset = re.compile(r"Datasets.*\n.*comstock")

        path = create_registry(tmpdir)
        dimension_config = Path(DATA_REPO) / "dsgrid_project" / "dimension.toml"
        check_run_command(f"dsgrid registry --path={path} register-dimension {dimension_config}")

        project_config = Path(DATA_REPO) / "dsgrid_project" / "project.toml"
        check_run_command(f"dsgrid registry --path={path} register-project {project_config}")
        output = {}
        check_run_command(f"dsgrid registry --path={path} list", output)
        assert regex_project.search(output["stdout"]) is not None
        assert "test" in output["stdout"]

        filename = Path("dsgrid_project/datasets/input/sector_models/comstock/dataset.toml")
        dataset_config = Path(DATA_REPO) / filename
        check_run_command(
            f"dsgrid registry --path={path} submit-dataset {dataset_config} -p test -l log"
        )
        output = {}
        check_run_command(f"dsgrid registry --path={path} list", output)
        assert regex_dataset.search(output["stdout"]) is not None


def test_register_dimension():
    with TemporaryDirectory() as tmpdir:
        path = create_registry(tmpdir)
        dimension_config = Path(DATA_REPO) / "dsgrid_project" / "dimension.toml"
        check_run_command(f"dsgrid registry --path={path} register-dimension {dimension_config}")
        output = {}
        check_run_command(f"dsgrid registry --path={path} list", output)
