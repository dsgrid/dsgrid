import pytest
import uuid

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.tests.common import TEST_PROJECT_PATH, TEST_REMOTE_REGISTRY, AWS_PROFILE_NAME
from tests_aws.test_registry.common import clean_remote_registry

local_registry = TEST_PROJECT_PATH / "test_registry_data_sync" / "registry"
local_registry_data_sync = TEST_PROJECT_PATH / "test_registry_data_sync" / "data_sync_registry"
remote_registry = TEST_REMOTE_REGISTRY

s3_cloudinterface = S3StorageInterface(
    remote_path=TEST_REMOTE_REGISTRY,
    local_path=local_registry,
    user="Test",
    uuid=str(uuid.uuid4()),
    profile=AWS_PROFILE_NAME,
)


def test_data_sync_project_id(s3_cloudinterface, local_registry_data_sync, remote_registry):

    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        project_id = "efs_2018"
        dataset_id = None
        registry_manager.data_sync(project_id, dataset_id)

        assert (
            len(
                LocalFilesystem().listdir(local_registry_data_sync / "data", directories_only=True)
            )
            == 2
        ), "Datasets downloaded should be 2"
    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_dataset_id():
    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        project_id = None
        dataset_id = "efs_comstock"
        registry_manager.data_sync(project_id, dataset_id)

        assert (
            len(
                LocalFilesystem().listdir(local_registry_data_sync / "data", directories_only=True)
            )
            == 1
        ), "Datasets downloaded should be 1"

    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id_and_dataset_id():
    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        project_id = "efs_2018"
        dataset_id = "efs_comstock"
        registry_manager.data_sync(project_id, dataset_id)

        assert (
            len(
                LocalFilesystem().listdir(local_registry_data_sync / "data", directories_only=True)
            )
            == 1
        ), "Datasets downloaded should be 1"

    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_project_id():
    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        with pytest.raises(ValueError):
            project_id = "bad_test"
            dataset_id = None
            registry_manager.data_sync(project_id, dataset_id)

    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_dataset_id():
    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        with pytest.raises(ValueError):
            project_id = None
            dataset_id = "bad_test"
            registry_manager.data_sync(project_id, dataset_id)

    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id_and_bad_dataset_id():
    try:
        s3_cloudinterface.sync_push(local_path=local_registry, remote_path=TEST_REMOTE_REGISTRY)
        registry_manager = RegistryManager.load(
            local_registry_data_sync,
            remote_registry,
            offline_mode=False,
            dry_run_mode=False,
            no_prompts=True,
        )

        with pytest.raises(ValueError):
            project_id = "efs_2018"
            dataset_id = "bad_test"
            registry_manager.data_sync(project_id, dataset_id)

    finally:
        LocalFilesystem().rm_tree(local_registry_data_sync)
        clean_remote_registry(s3_cloudinterface._s3_filesystem)
