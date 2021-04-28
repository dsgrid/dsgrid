import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pytest

from dsgrid.utils.run_command import check_run_command, run_command
from tests.common import (
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
)


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
    check_run_command(f"dsgrid-internal create-registry {path}")
    assert path.exists()
    assert (path / "configs/projects").exists()
    assert (path / "configs/datasets").exists()
    assert (path / "configs/dimensions").exists()
    assert (path / "configs/dimension_mappings").exists()
    return path


def test_register_project_and_dataset(test_data_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        path = create_registry(base_dir)
        dataset_dir = Path("datasets/sector_models/comstock")
        dataset_dim_dir = dataset_dir / "dimensions"
        # TODO: The data repo currently does not have valid dimension mappings.
        # Disabling these tests.
        # dimension_mapping_config = test_data_dir / dataset_dim_dir / "dimension_mappings.toml"
        # dimension_mapping_refs = (
        #    test_data_dir / dataset_dim_dir / "dimension_mapping_references.toml"
        # )

        for dim_config_file in (
            test_data_dir / "dimensions.toml",
            # test_data_dir / dataset_dir / "dimension.toml",
        ):
            cmd = f"dsgrid registry --path={path} --offline dimensions register {dim_config_file} -l log"
            check_run_command(cmd)
            # Can't register duplicates.
            # assert run_command(cmd) != 0

        # cmd = f"dsgrid registry --path={path} --offline dimension-mappings register {dimension_mapping_config} -l log"
        # check_run_command(cmd)
        # Can't register duplicates.
        # assert run_command(cmd) != 0

        project_config = test_data_dir / "project.toml"
        dataset_config = test_data_dir / dataset_dir / "dataset.toml"
        # replace_dimension_mapping_uuids_from_registry(
        #    path, (project_config, dimension_mapping_refs)
        # )
        replace_dimension_uuids_from_registry(path, (project_config, dataset_config))

        check_run_command(
            f"dsgrid registry --path={path} --offline datasets register {dataset_config} -l log"
        )
        check_run_command(
            f"dsgrid registry --path={path} --offline projects register {project_config} -l log"
        )
        check_run_command(
            f"dsgrid registry --path={path} --offline projects submit-dataset -d efs-comstock -p test -l log"
        )
        output = {}
        check_run_command(f"dsgrid registry --path={path} --offline list", output)
        regex_project = re.compile(r"test.*1\.0\.0")
        regex_dataset = re.compile(r"efs-comstock.*1\.0\.0")
        assert regex_project.search(output["stdout"]) is not None
        assert regex_dataset.search(output["stdout"]) is not None
