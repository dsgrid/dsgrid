from pathlib import Path
import pytest
from tempfile import TemporaryDirectory

from dsgrid.tests.common import AWS_PROFILE_NAME, TEST_REMOTE_REGISTRY
from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.exceptions import DSGRegistryLockError, DSGMakeLockError
from dsgrid.registry.registry_manager import RegistryManager


def test_make_and_release_lock():
    s3 = S3StorageInterface(
        local_path="",
        remote_path=TEST_REMOTE_REGISTRY,
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with s3.make_lock_file_managed(f"{TEST_REMOTE_REGISTRY}/configs/.locks/test.lock"):
        assert s3._s3_filesystem.path(f"{TEST_REMOTE_REGISTRY}/configs/.locks/test.lock").exists()
    assert s3._s3_filesystem.listdir("configs/.locks") == []


def test_sync_push_fail_if_lock_exists():
    s3 = S3StorageInterface(
        local_path="",
        remote_path=TEST_REMOTE_REGISTRY,
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        lock_file = f"{TEST_REMOTE_REGISTRY}/configs/.locks/dimensions.lock"
        with s3.make_lock_file_managed(lock_file):
            assert s3._s3_filesystem.path(lock_file).exists()
            s3 = s3.check_lock_file(lock_file)
            manager = RegistryManager.create(
                path=base_dir / "dsgrid-registry", user="test", remote_path=TEST_REMOTE_REGISTRY
            )
            try:
                manager.dimension_manager.cloud_interface = make_cloud_storage_interface(
                    base_dir / "dsgrid-registry",
                    remote_path=TEST_REMOTE_REGISTRY,
                    user="test",
                    offline=False,
                    uuid="0",
                )
                with pytest.raises(DSGRegistryLockError):
                    manager.dimension_manager.cloud_interface.check_lock_file(lock_file)
                with pytest.raises(DSGRegistryLockError):
                    manager.dimension_manager.sync_push(
                        base_dir / "dsgrid-registry/configs/dimensions/geography/test/1.0.0"
                    )
            finally:
                manager.dispose()


def test_bad_lockfile():
    s3 = S3StorageInterface(
        local_path="",
        remote_path=TEST_REMOTE_REGISTRY,
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with pytest.raises(DSGMakeLockError):
        for bad_lockfile in (
            f"{TEST_REMOTE_REGISTRY}/configs/.locks/test.locks",
            f"{TEST_REMOTE_REGISTRY}/configs/.lock/test.lock",
        ):
            with s3.make_lock_file_managed(bad_lockfile):
                pass
