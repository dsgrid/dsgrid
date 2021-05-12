from pathlib import Path
import pytest
from tempfile import TemporaryDirectory, gettempdir

from dsgrid.cloud.factory import make_cloud_storage_interface
from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.exceptions import DSGRegistryLockError, DSGMakeLockError
from dsgrid.registry.registry_manager import RegistryManager

AWS_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REGISTRY = "s3://nrel-dsgrid-registry-test"


def test_make_and_release_lock():
    s3 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with s3.make_lock_file("s3://nrel-dsgrid-registry-test/configs/.locks/test.lock"):
        assert s3._s3_filesystem.s3_path(
            "s3://nrel-dsgrid-registry-test/configs/.locks/test.lock"
        ).exists()
    assert s3._s3_filesystem.listdir("configs/.locks") == []


def test_sync_push_fail_if_lock_exists():

    s3 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        lock_file = "s3://nrel-dsgrid-registry-test/configs/.locks/dimensions.lock"
        with s3.make_lock_file(lock_file):
            assert s3._s3_filesystem.s3_path(lock_file).exists()
            s3 = s3.check_lock_file(lock_file)
            manager = RegistryManager.create(
                path=base_dir / "dsgrid-registry", user="test", remote_path=TEST_REGISTRY
            )
            manager.dimension_manager.cloud_interface = make_cloud_storage_interface(
                base_dir / "dsgrid-registry",
                remote_path=TEST_REGISTRY,
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


def test_bad_lockfile():
    s3 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    with pytest.raises(DSGMakeLockError):
        for bad_lockfile in (
            "s3://nrel-dsgrid-registry-test/configs/.locks/test.locks",
            "s3://nrel-dsgrid-registry-test/configs/.lock/test.lock",
        ):
            with s3.make_lock_file(bad_lockfile):
                pass
