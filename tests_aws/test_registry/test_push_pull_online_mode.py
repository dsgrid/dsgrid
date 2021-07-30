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

from dsgrid.tests.common import create_local_test_registry

from .common import clean_remote_registry, create_empty_remote_registry

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.cloud.s3_storage_interface import S3StorageInterface


PROJECT_REPO = os.environ.get("TEST_PROJECT_REPO")
S3_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REGISTRY = "s3://nrel-dsgrid-registry-test"


def test_pull_delete_local():
    """test that sync_pull is deleting local files not on registry"""
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        submitter = getpass.getuser()
        log_message = "test"
        path = create_local_test_registry(base_dir)
        len_fs = len(LocalFilesystem().listdir(path))
        LocalFilesystem().touch(Path(path) / "junk")
        LocalFilesystem().touch(Path(path) / "configss")
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REGISTRY,
            local_path=path,
            user=submitter,
            uuid=str(uuid.uuid4()),
            profile=S3_PROFILE_NAME,
        )
        s3_cloudinterface.sync_pull(remote_path=TEST_REGISTRY, local_path=path, delete_local=True)
        assert len(LocalFilesystem().listdir(path)) == 0


def test_pull_exclude():
    """test that sync_pull is excluding certain patterns"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            log_message = "test"
            path = create_local_test_registry(base_dir)
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
    """test that sync_push is excluding certain patterns"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            log_message = "test"
            path = create_local_test_registry(base_dir)
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
