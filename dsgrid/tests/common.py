import fileinput
import getpass
import os
import re
from pathlib import Path

import pytest

from dsgrid.exceptions import DSGInvalidParameter, DSGInvalidOperation
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.common import VersionUpdateType
from dsgrid.utils.files import dump_data, load_data

TEST_PROJECT_PATH = Path(__file__).absolute().parent.parent.parent / "dsgrid-test-data"
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_efs"
TEST_STANDARD_SCENARIOS_PROJECT_REPO = TEST_PROJECT_PATH / "standard_scenarios_2021"
TEST_DATASET_DIRECTORY = TEST_PROJECT_PATH / "datasets"
TEST_REGISTRY = Path("tests/data/registry")
TEST_EFS_REGISTRATION_FILE = Path("tests/data/test_efs_registration.json")
AWS_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REMOTE_REGISTRY = "s3://nrel-dsgrid-registry-test"


def create_local_test_registry(tmpdir):
    path = Path(tmpdir)
    RegistryManager.create(path)
    assert path.exists()
    assert (path / "configs/projects").exists()
    assert (path / "configs/datasets").exists()
    assert (path / "configs/dimensions").exists()
    assert (path / "configs/dimension_mappings").exists()
    return path


def replace_dimension_mapping_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_mapping_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_dimension_mapping_uuids(filename, uuids)


def read_dimension_mapping_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir) / "configs"
    mappings = {}
    regex = re.compile(
        r"(?P<from_dimension>[-\w]+)__(?P<to_dimension>[-\w]+)__(?P<uuid>[-0-9a-f]+)"
    )
    path = dir_name / "dimension_mappings"
    for item in fs_intf.listdir(path, directories_only=True, exclude_hidden=True):
        assert (path / item).is_dir(), str(path / item)
        match = regex.search(item)
        assert match, item
        data = match.groupdict()
        from_dimension = data["from_dimension"]
        to_dimension = data["to_dimension"]
        item_uuid = data["uuid"]
        key = (from_dimension, to_dimension)
        assert key not in mappings, item
        mappings[key] = item_uuid

    return mappings


def replace_dimension_mapping_uuids(filename, uuids):
    regex = re.compile(
        r"mapping_id: \"(?P<from_dimension>[-\w]+)__(?P<to_dimension>[-\w]+)__(?P<uuid>[-0-9a-f]+)\""
    )
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            if line.strip().startswith("/"):
                continue
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                from_dimension = match.groupdict()["from_dimension"]
                to_dimension = match.groupdict()["to_dimension"]
                new_uuid = uuids[(from_dimension, to_dimension)]
                print(f'      mapping_id: "{from_dimension}__{to_dimension}__{new_uuid}",')


def replace_dimension_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_dimension_uuids(filename, uuids)


def read_dimension_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir) / "configs"
    mappings = {}
    regex = re.compile(r"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)")
    dim_base_path = dir_name / "dimensions"
    for dim_type in fs_intf.listdir(dim_base_path, directories_only=True, exclude_hidden=True):
        dim_path = dim_base_path / dim_type
        for dim in fs_intf.listdir(dim_path, directories_only=True, exclude_hidden=True):
            match = regex.search(dim)
            assert match, dim
            data = match.groupdict()
            dim_type = data["dimension_type"]
            dim_uuid = data["uuid"]
            assert dim_type not in mappings, dim_type
            mappings[dim_type] = dim_uuid

    return mappings


def replace_dimension_uuids(filename, uuids):
    regex = re.compile(r"dimension_id: \"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)\"")
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            if line.strip().startswith("/"):
                continue
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                dimension_type = match.groupdict()["dimension_type"]
                new_uuid = uuids[dimension_type]
                print(f'      dimension_id: "{dimension_type}__{new_uuid}",')


def check_configs_update(base_dir, manager):
    """Runs an update on one of each type of config.

    Parameters
    ----------
    base_dir : Path
    manager : RegistryManager

    Returns
    -------
    list
        Each updated config and new version: [(Updated ID, new version)]
        For the dimension element the tuple is (Updated ID, dimension type, new version).
        Order is dimension ID, dimension mapping ID, dataset ID, project ID

    """
    update_dir = base_dir / "updates"
    user = getpass.getuser()

    updated_ids = []
    for mgr in (
        manager.dimension_manager,
        manager.dimension_mapping_manager,
        manager.dataset_manager,
        manager.project_manager,
    ):
        config_id = mgr.list_ids()[0]
        version = mgr.get_current_version(config_id)
        check_config_update(update_dir, mgr, config_id, user, version)
        new_version = mgr.get_current_version(config_id)
        if isinstance(mgr, DimensionRegistryManager):
            config = mgr.get_by_id(config_id)
            updated_ids.append((config_id, config.model.dimension_type, new_version))
        else:
            updated_ids.append((config_id, new_version))

    return updated_ids


def check_config_update(base_dir, mgr, config_id, user, version):
    """Runs basic positive and negative update tests for the config.

    Parameters
    ----------
    base_dir : str
    mgr : RegistryManagerBase
    config_id : str
    user : str
    version : VersionInfo

    """
    config_file = Path(base_dir) / mgr.registry_class().config_filename()
    assert not config_file.exists()
    try:
        mgr.dump(config_id, base_dir)
        with pytest.raises(DSGInvalidOperation):
            mgr.dump(config_id, base_dir)
        mgr.dump(config_id, base_dir, force=True)
        assert config_file.exists()
        config_data = load_data(config_file)
        config_data["description"] += "; updated description"
        dump_data(config_data, config_file)
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                "invalid_config_id",
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version,
            )
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                config_id,
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version.bump_patch(),
            )

        mgr.update_from_file(
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
