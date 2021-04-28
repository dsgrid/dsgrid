import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pytest

from dsgrid.exceptions import DSGDuplicateValueRegistered
from dsgrid.registry.common import DatasetRegistryStatus
from dsgrid.registry.registry_manager import RegistryManager
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
    RegistryManager.create(path)
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
        # dimension_mapping_config = test_data_dir / dataset_dim_dir / "dimension_mappings.toml"
        # dimension_mapping_refs = (
        #    test_data_dir / dataset_dim_dir / "dimension_mapping_references.toml"
        # )

        user = "test_user"
        log_message = "registration message"
        manager = RegistryManager.load(path, offline_mode=True)
        for dim_config_file in (
            test_data_dir / "dimensions.toml",
            # TODO: we used to have two of these. What happened to the second?
            # test_data_dir / dataset_dir / "dimensions.toml",
        ):
            dim_mgr = manager.dimension_manager
            dim_mgr.register(dim_config_file, user, log_message)
            assert dim_mgr.list_ids()
            with pytest.raises(DSGDuplicateValueRegistered):
                dim_mgr.register(dim_config_file, user, log_message)

        # dim_mapping_mgr = manager.dimension_mapping_manager
        # dim_mapping_mgr.register(dimension_mapping_config, user, log_message)
        # assert dim_mapping_mgr.list_ids()
        # with pytest.raises(DSGDuplicateValueRegistered):
        #    dim_mapping_mgr.register(dimension_mapping_config, user, log_message)

        project_config_file = test_data_dir / "project.toml"
        dataset_config_file = test_data_dir / dataset_dir / "dataset.toml"
        # replace_dimension_mapping_uuids_from_registry(
        #    path, (project_config_file, dimension_mapping_refs)
        # )
        replace_dimension_uuids_from_registry(path, (project_config_file, dataset_config_file))

        project_mgr = manager.project_manager
        project_mgr.register(project_config_file, user, log_message)
        project_id = "test"
        assert project_mgr.list_ids() == [project_id]
        project_config = project_mgr.get_by_id(project_id)

        dataset_mgr = manager.dataset_manager
        dataset_mgr.register(dataset_config_file, user, log_message)
        dataset_id = "efs-comstock"
        assert dataset_mgr.list_ids() == [dataset_id]
        dataset_config = dataset_mgr.get_by_id(dataset_id)

        project_mgr.submit_dataset(
            project_config.model.project_id,
            dataset_config.model.dataset_id,
            [],  # [dimension_mapping_refs],
            user,
            log_message,
        )
        project_config = project_mgr.get_by_id(project_id)
        dataset = project_config.get_dataset(dataset_id)
        assert dataset.status == DatasetRegistryStatus.REGISTERED
