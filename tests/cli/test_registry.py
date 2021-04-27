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

from dsgrid.registry.registry_manager import RegistryManager


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
    RegistryManager.create(path)
    # check_run_command(f"dsgrid registry create {path}")
    assert path.exists()
    assert (path / "data").exists()
    assert (path / "configs/projects").exists()
    assert (path / "configs/datasets").exists()
    assert (path / "configs/dimensions").exists()
    assert (path / "configs/dimension_mappings").exists()
    return path


def test_register_project_and_dataset_cli(test_data_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        regex_project = re.compile(r"Projects.*\n.*test")
        regex_dataset = re.compile(r"Datasets.*\n.*comstock")

        path = create_registry(base_dir)
        dataset_dir = Path("datasets/sector_models/comstock")
        dataset_dim_dir = dataset_dir / "dimensions"
        dimension_mapping_config = test_data_dir / dataset_dim_dir / "dimension_mappings.toml"
        dimension_mapping_refs = (
            test_data_dir / dataset_dim_dir / "dimension_mapping_references.toml"
        )

        # --------------------
        # register dimensions
        # --------------------
        for dim_config_file in (test_data_dir / "dimensions.toml",):

            cmd = f"dsgrid registry --offline --path={str(path)} dimensions register {dim_config_file} -l log"
            check_run_command(cmd)
            # Can't register duplicates.
            assert run_command(cmd) != 0

        # ------------------------------
        # register dimension mappings
        # ------------------------------
        cmd = f"dsgrid registry --path={path} dimension-mappings register {dimension_mapping_config} -l log --offline"
        check_run_command(cmd)
        # Can't register duplicates.
        assert run_command(cmd) != 0

        # ----------------------
        # Update configs
        # ----------------------
        project_config = test_data_dir / "project.toml"
        dataset_config = test_data_dir / dataset_dir / "dataset.toml"
        replace_dimension_mapping_uuids_from_registry(
            path, (project_config, dimension_mapping_refs)
        )
        replace_dimension_uuids_from_registry(path, (project_config, dataset_config))

        # --------------------
        # register project
        # --------------------
        check_run_command(
            f"dsgrid registry --offline --path={path} projects register {project_config} -l log"
        )

        output = {}
        check_run_command(f"dsgrid registry --offline --path={path} list", output)
        assert regex_project.search(output["stdout"]) is not None
        assert "test" in output["stdout"]

        # --------------------
        # submit dataset
        # --------------------
        check_run_command(
            f"dsgrid registry --path={path} datasets submit {dataset_config} "
            "--project-id test "
            "--log-message=test_submission "
            f"--dimension-mapping-files={dimension_mapping_refs} "
            "--offline"
        )

        output = {}
        check_run_command(f"dsgrid registry --offline --path={path} list", output)
        assert regex_dataset.search(output["stdout"]) is not None


def test_register_project_and_dataset(test_data_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        regex_project = re.compile(r"Projects.*\n.*test")
        regex_dataset = re.compile(r"Datasets.*\n.*comstock")

        path = create_registry(base_dir)
        dataset_dir = Path("datasets/sector_models/comstock")
        dataset_dim_dir = dataset_dir / "dimensions"
        dimension_mapping_config = test_data_dir / dataset_dim_dir / "dimension_mappings.toml"
        dimension_mapping_refs = (
            test_data_dir / dataset_dim_dir / "dimension_mapping_references.toml"
        )

        # --------------------
        # register dimensions
        # --------------------
        for dim_config_file in (test_data_dir / "dimensions.toml",):
            manager_load = RegistryManager.load(
                path=str(path), offline_mode=True, dry_run_mode=False
            )
            manager = manager_load.dimension_manager
            submitter = "test"
            manager.register(dim_config_file, submitter, "log")

        # ------------------------------
        # register dimension mappings
        # ------------------------------
        manager = RegistryManager.load(
            path, offline_mode=True, dry_run_mode=False
        ).dimension_mapping_manager
        manager.register(dimension_mapping_config, submitter, "log", force=False)

        # ----------------------
        # Update configs
        # ----------------------
        project_config = test_data_dir / "project.toml"
        dataset_config = test_data_dir / dataset_dir / "dataset.toml"
        replace_dimension_mapping_uuids_from_registry(
            path, (project_config, dimension_mapping_refs)
        )
        replace_dimension_uuids_from_registry(path, (project_config, dataset_config))

        # --------------------
        # register project
        # --------------------
        manager = RegistryManager.load(path, offline_mode=True, dry_run_mode=False)
        submitter = "test"
        manager.register_project(project_config, submitter, "log")

        # --------------------
        # submit dataset
        # --------------------
        manager = RegistryManager.load(path, offline_mode=True, dry_run_mode=False)
        print(f"Registry: {path}")
        print("Projects:")
        for project in manager.list_projects():
            print(f"  - {project}")
        print("\nDatasets:")
        for dataset in manager.list_datasets():
            print(f"  - {dataset}")
        print("\nDimensions:")
        manager.dimension_manager.show()
        manager.dimension_mapping_manager.show()

        # TODO: add asserts to make sure the .show() are not empty
