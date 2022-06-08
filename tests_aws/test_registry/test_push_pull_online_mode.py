from dsgrid.filesystem.local_filesystem import LocalFilesystem
import getpass
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.tests.common import create_local_test_registry, AWS_PROFILE_NAME, TEST_REMOTE_REGISTRY
from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from .common import clean_remote_registry, create_empty_remote_registry


def test_pull_delete_local():
    """test that sync_pull is deleting local files not on registry"""
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        submitter = getpass.getuser()
        path = create_local_test_registry(base_dir)
        LocalFilesystem().touch(Path(path) / "junk")
        LocalFilesystem().touch(Path(path) / "configss")
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=path,
            user=submitter,
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        s3_cloudinterface.sync_pull(
            remote_path=TEST_REMOTE_REGISTRY, local_path=path, delete_local=True
        )
        assert len(LocalFilesystem().listdir(path)) == 0


def test_pull_exclude():
    """test that sync_pull is excluding certain patterns"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            path = create_local_test_registry(base_dir)
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=path,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            create_empty_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface._s3_filesystem.touch("exclude")
            s3_cloudinterface._s3_filesystem.touch("configs/.exclude")
            s3_cloudinterface._s3_filesystem.touch("include")
            s3_cloudinterface.sync_pull(
                remote_path=TEST_REMOTE_REGISTRY, local_path=path, exclude=["**exclude**"]
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
            path = create_local_test_registry(base_dir)
            LocalFilesystem().listdir(Path(path))
            LocalFilesystem().touch(Path(path) / "exclude")
            LocalFilesystem().touch(Path(path) / "include")
            LocalFilesystem().touch(Path(path) / "configs/dimensions" / ".exclude")
            LocalFilesystem().touch(Path(path) / "configs/dimensions" / ".include")
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=path,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface.sync_push(
                remote_path=TEST_REMOTE_REGISTRY, local_path=path, exclude=["**exclude**"]
            )
            contents = s3_cloudinterface._s3_filesystem.rglob()
            for x in contents:
                assert "exclude" not in x, f"file '{x}' contains exclude"
    finally:
        clean_remote_registry(s3_cloudinterface._s3_filesystem)
