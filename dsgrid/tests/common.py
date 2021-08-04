from pathlib import Path
from tempfile import gettempdir
import fileinput
import os
import pytest
import re
import shutil
import sys

from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.registry.registry_manager import RegistryManager

PROJECT_REPO = os.environ.get("TEST_PROJECT_REPO")
LOCAL_DATA_DIRECTORY = os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY")


@pytest.fixture
def make_test_project_dir():
    if PROJECT_REPO is None:
        print(
            "You must define the environment TEST_PROJECT_REPO with the path to the "
            "dsgrid-project-EFS repository"
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


@pytest.fixture
def make_test_data_dir():
    if LOCAL_DATA_DIRECTORY is None:
        print(
            "You must define the environment DSGRID_LOCAL_DATA_DIRECTORY with the path to your "
            "copy of datasets."
        )
        sys.exit(1)

    tmpdir = Path(gettempdir()) / "test_data"
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    dst_path = tmpdir / "datasets"
    shutil.copytree(Path(LOCAL_DATA_DIRECTORY), dst_path)
    yield dst_path
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


def replace_dimension_mapping_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_mapping_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_dimension_mapping_uuids(filename, uuids)


def read_dimension_mapping_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir) / "configs"
    mappings = {}
    regex = re.compile(
        r"(?P<from_dimension>[-\w]+)__(?P<to_dimension>[-\w]+)__(?P<uuid>[-0-9a-f]+)$"
    )
    path = dir_name / "dimension_mappings"
    for item in fs_intf.listdir(path, directories_only=True, exclude_hidden=True):
        assert os.path.isdir(path / item), str(path / item)
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
        r"mapping_id = \"(?P<from_dimension>[-\w]+)__(?P<to_dimension>[-\w]+)__(?P<uuid>[-0-9a-f]+)\""
    )
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                from_dimension = match.groupdict()["from_dimension"]
                to_dimension = match.groupdict()["to_dimension"]
                new_uuid = uuids[(from_dimension, to_dimension)]
                print(f'mapping_id = "{from_dimension}__{to_dimension}__{new_uuid}"')


def replace_dimension_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_dimension_uuids(filename, uuids)


def read_dimension_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir) / "configs"
    mappings = {}
    regex = re.compile(r"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)$")
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
    regex = re.compile(r"dimension_id = \"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)\"")
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                dimension_type = match.groupdict()["dimension_type"]
                new_uuid = uuids[dimension_type]
                print(f'dimension_id = "{dimension_type}__{new_uuid}"')
