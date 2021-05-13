import os
import re
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

import pytest
from semver import VersionInfo

from dsgrid.exceptions import DSGDuplicateValueRegistered, DSGInvalidParameter
from dsgrid.registry.common import DatasetRegistryStatus, VersionUpdateType
from dsgrid.registry.dataset_registry_manager import DatasetRegistryManager
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry, make_test_project_dir
from dsgrid.utils.files import dump_data, load_data
from tests.common import (
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
)


def test_register_project_and_dataset(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        path = create_local_test_registry(base_dir)
        dataset_dir = Path("datasets/sector_models/comstock")
        project_dimension_mapping_config = make_test_project_dir / "dimension_mappings.toml"
        dimension_mapping_config = make_test_project_dir / dataset_dir / "dimension_mappings.toml"
        dimension_mapping_refs = (
            make_test_project_dir / dataset_dir / "dimension_mapping_references.toml"
        )

        user = "test_user"
        log_message = "registration message"
        manager = RegistryManager.load(path, offline_mode=True)

        # register dimensions
        dimension_id = None
        for dim_config_file in (
            make_test_project_dir / "dimensions.toml",
            make_test_project_dir / dataset_dir / "dimensions.toml",
        ):
            dim_mgr = manager.dimension_manager
            dim_mgr.register(dim_config_file, user, log_message)
            assert dim_mgr.list_ids()
            if dimension_id is None:
                dimension_id = dim_mgr.list_ids()[0]
            if dim_config_file == make_test_project_dir / "dimensions.toml":
                # The other one has time only - no records.
                with pytest.raises(DSGDuplicateValueRegistered):
                    dim_mgr.register(dim_config_file, user, log_message)

        assert dimension_id is not None

        # register dimension mappings
        dim_mapping_mgr = manager.dimension_mapping_manager
        dim_mapping_mgr.register(project_dimension_mapping_config, user, log_message)
        dim_mapping_mgr.register(dimension_mapping_config, user, log_message)
        dim_mapping_ids = dim_mapping_mgr.list_ids()
        assert dim_mapping_ids
        with pytest.raises(DSGDuplicateValueRegistered):
            dim_mapping_mgr.register(dimension_mapping_config, user, log_message)
        dimension_mapping_id = dim_mapping_ids[0]

        # The project/dataset config files need to use the UUIDs that were just registered.
        # Make the modifications in our local copy.
        project_config_file = make_test_project_dir / "project.toml"
        dataset_config_file = make_test_project_dir / dataset_dir / "dataset.toml"
        replace_dimension_mapping_uuids_from_registry(
            path, (project_config_file, dimension_mapping_refs)
        )
        replace_dimension_uuids_from_registry(path, (project_config_file, dataset_config_file))

        # Register project and dataset and connect them together.
        project_id = "test"
        dataset_id = "efs_comstock"
        project_mgr = manager.project_manager
        dataset_mgr = manager.dataset_manager
        dimension_mgr = manager.dimension_manager
        dimension_mapping_mgr = manager.dimension_mapping_manager

        register_project(project_mgr, project_config_file, project_id, user, log_message)
        register_dataset(dataset_mgr, dataset_config_file, dataset_id, user, log_message)

        project_config = project_mgr.get_by_id(project_id)
        dataset_config = dataset_mgr.get_by_id(dataset_id)
        submit_dataset(
            project_mgr,
            project_config,
            dataset_config,
            [dimension_mapping_refs],
            user,
            log_message,
        )

        # Test updates to all configs.
        check_config_update(base_dir, project_mgr, project_id, user, VersionInfo.parse("1.1.0"))
        check_config_update(base_dir, dataset_mgr, dataset_id, user, VersionInfo.parse("1.0.0"))
        dim_dir = base_dir / "dimensions"
        os.makedirs(dim_dir)
        check_config_update(dim_dir, dimension_mgr, dimension_id, user, VersionInfo.parse("1.0.0"))
        check_config_update(
            dim_dir, dimension_mapping_mgr, dimension_mapping_id, user, VersionInfo.parse("1.0.0")
        )


def register_project(project_mgr, config_file, project_id, user, log_message):
    project_mgr.register(config_file, user, log_message)
    assert project_mgr.list_ids() == [project_id]


def register_dataset(dataset_mgr, config_file, dataset_id, user, log_message):
    dataset_mgr.register(config_file, user, log_message)
    assert dataset_mgr.list_ids() == [dataset_id]


def submit_dataset(
    project_mgr, project_config, dataset_config, dimension_mapping_refs, user, log_message
):
    project_id = project_config.config_id
    dataset_id = dataset_config.config_id
    project_mgr.submit_dataset(
        project_id,
        dataset_id,
        dimension_mapping_refs,
        user,
        log_message,
    )
    project_config = project_mgr.get_by_id(project_id, VersionInfo.parse("1.1.0"))
    dataset = project_config.get_dataset(dataset_id)
    assert dataset.status == DatasetRegistryStatus.REGISTERED

    # The old one should still be there.
    project_config = project_mgr.get_by_id(project_id, VersionInfo.parse("1.0.0"))
    dataset = project_config.get_dataset(dataset_id)
    assert dataset.status == DatasetRegistryStatus.UNREGISTERED


def check_config_update(tmpdir, mgr, config_id, user, version):
    config_file = Path(tmpdir) / mgr.registry_class().config_filename()
    assert not config_file.exists()
    try:
        mgr.dump(config_id, tmpdir)
        assert config_file.exists()
        config_data = load_data(config_file)
        config_data["description"] += "; updated description"
        dump_data(config_data, config_file)
        with pytest.raises(DSGInvalidParameter):
            mgr.update(
                config_file,
                "invalid_config_id",
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version,
            )
        with pytest.raises(DSGInvalidParameter):
            mgr.update(
                config_file,
                config_id,
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version.bump_patch(),
            )

        mgr.update(
            config_file,
            config_id,
            user,
            VersionUpdateType.PATCH,
            "update to description",
            version,
        )
        assert mgr.get_current_version(config_id) == version.bump_patch()
    finally:
        if config_file.exists():
            os.remove(config_file)
