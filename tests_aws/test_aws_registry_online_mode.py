from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
import shutil
import getpass
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
import getpass
import uuid

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.cloud.s3_storage_interface import S3StorageInterface

DATA_REPO = os.environ.get("US_DATA_REPO")
S3_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REGISTRY = "s3://nrel-dsgrid-registry-test"


@pytest.fixture
def test_project_dir():
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


def create_local_registry(tmpdir):
    path = Path(tmpdir) / "dsgrid-registry"
    RegistryManager.create(path)
    assert path.exists()
    assert (path / "configs/projects").exists()
    assert (path / "configs/datasets").exists()
    assert (path / "configs/dimensions").exists()
    assert (path / "configs/dimension_mappings").exists()
    return path


def create_empty_remote_registry(s3_filesystem):
    """Create fresh remote registry for testing that includes all base dirs"""
    contents = s3_filesystem.rglob(s3_filesystem._bucket)
    assert len(contents) == 0, "remote registry is not empty"
    for folder in (
        "configs/datasets",
        "configs/projects",
        "configs/dimensions",
        "configs/dimension_mappings",
        "data",
    ):
        s3_filesystem.mkdir(f"{folder}")
    for dim_type in DimensionType:
        s3_filesystem.mkdir(f"configs/dimensions/{dim_type.value}")
    assert s3_filesystem.listdir() == ["configs", "data"]


def clean_remote_registry(s3_filesystem, prefix=""):
    """Hard delete all object versions and markers in the versioned s3 test registry bucket; add root folders"""
    s3 = s3_filesystem._client
    bucket = s3_filesystem._bucket
    for i in range(2):
        versions = s3.list_object_versions(Bucket=bucket, Prefix=prefix)
        keys = [
            k for k in versions.keys() if k in ("Versions", "VersionIdMarker", "DeleteMarkers")
        ]
        for key in keys:
            for v in versions[key]:
                if key != prefix:
                    s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
    assert s3_filesystem.listdir() == []


def test_aws_registry_workflow_online_mode(test_project_dir):
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            log_message = "test"
            s3_cloud_storage = S3StorageInterface(
                local_path=base_dir,
                remote_path=TEST_REGISTRY,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=S3_PROFILE_NAME,
            )

            path = create_local_registry(base_dir)
            assert LocalFilesystem().listdir(path)
            dataset_dir = Path("datasets/sector_models/comstock")

            clean_remote_registry(s3_cloud_storage._s3_filesystem)
            create_empty_remote_registry(s3_cloud_storage._s3_filesystem)

            manager = RegistryManager.load(path=path, remote_path=TEST_REGISTRY, user=submitter)

            for dim_config_file in (
                test_project_dir / "dimensions.toml",
                test_project_dir / dataset_dir / "dimensions.toml",
            ):
                manager.dimension_manager.register(dim_config_file, submitter, log_message)

            # check that we didn't push unexpected things...
            s3 = manager.dimension_manager.cloud_interface._s3_filesystem
            assert s3.listdir("") == ["configs", "data"]
            for dim_type in DimensionType:
                dimtype = dim_type.value
                files = s3.listdir(f"configs/dimensions/{dim_type.value}")
                for file in files:
                    assert s3.listdir(f"configs/dimensions/{dim_type.value}/{file}") == [
                        "1.0.0",
                        "registry.toml",
                    ], (
                        s3.listdir(f"configs/dimensions/{dim_type.value}/{file}")
                        == ["1.0.0", "registry.toml"],
                        f"configs/dimensions/{dim_type.value}/{file}",
                        file,
                    )

    finally:
        clean_remote_registry(s3_cloud_storage._s3_filesystem)


def test_pull_delete_local():
    """test that sync_pull is deleting local files not on registry"""
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        submitter = getpass.getuser()
        log_message = "test"
        path = create_local_registry(base_dir)
        len_fs = len(LocalFilesystem().listdir(path))
        LocalFilesystem().touch(Path(path) / "junk")
        LocalFilesystem().touch(Path(path) / "configss")
        assert len(LocalFilesystem().listdir(path)) > len_fs


def test_pull_exclude():
    """test that sync_pull is excluding certain patterns"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            log_message = "test"
            path = create_local_registry(base_dir)
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REGISTRY,
                local_path=path,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=S3_PROFILE_NAME,
            )
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            create_empty_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface._s3_filesystem.touch("exclude")
            s3_cloudinterface._s3_filesystem.touch("configs/.exclude")
            s3_cloudinterface._s3_filesystem.touch("include")
            s3_cloudinterface.sync_pull(
                remote_path=TEST_REGISTRY, local_path=path, exclude=["**exclude**"]
            )
            contents = LocalFilesystem().rglob(Path(path))
            for x in contents:
                assert "exclude" not in str(x), f"file '{str(x)}' contains exclude"
    finally:
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_push_exclude():
    """test that sync_pull is excluding certain patterns"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            log_message = "test"
            path = create_local_registry(base_dir)
            LocalFilesystem().listdir(Path(path))
            LocalFilesystem().touch(Path(path) / "exclude")
            LocalFilesystem().touch(Path(path) / "include")
            LocalFilesystem().touch(Path(path) / "configs/dimensions" / ".exclude")
            LocalFilesystem().touch(Path(path) / "configs/dimensions" / ".include")
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REGISTRY,
                local_path=path,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=S3_PROFILE_NAME,
            )
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface.sync_push(
                remote_path=TEST_REGISTRY, local_path=path, exclude=["**exclude**"]
            )
            contents = s3_cloudinterface._s3_filesystem.rglob()
            for x in contents:
                assert "exclude" not in x, f"file '{x}' contains exclude"
    finally:
        clean_remote_registry(s3_cloudinterface._s3_filesystem)
