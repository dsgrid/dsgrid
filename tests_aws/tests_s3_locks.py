import pytest

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.exceptions import DSGRegistryLockError

AWS_PROFILE_NAME = "nrel-aws-dsgrid"


def test_make_and_release_lock():
    s3 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="mmooney",
        profile=AWS_PROFILE_NAME,
    )
    with s3.make_lock(directory="s3://nrel-dsgrid-registry-test"):
        contents = s3._s3_filesystem.listdir()
    assert s3._s3_filesystem.listdir() == []


def test_sync_push_and_pull_fail_if_lock_exists():
    s3 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="mmooney",
        profile=AWS_PROFILE_NAME,
    )
    s3_v2 = S3StorageInterface(
        local_path="",
        remote_path="s3://nrel-dsgrid-registry-test",
        uuid="123",
        user="mmooney",
        profile=AWS_PROFILE_NAME,
    )
    with s3.make_lock(directory="s3://nrel-dsgrid-registry-test"):
        contents = s3._s3_filesystem.listdir()
        with pytest.raises(DSGRegistryLockError):
            s3_v2.sync_push(remote_path="s3://nrel-dsgrid-registry-test", local_path="")
        with pytest.raises(DSGRegistryLockError):
            s3_v2.sync_pull(remote_path="s3://nrel-dsgrid-registry-test", local_path="")
