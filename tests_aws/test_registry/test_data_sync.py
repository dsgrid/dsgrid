import pytest
import uuid
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import json5

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.common import SYNC_EXCLUDE_LIST
from dsgrid.tests.common import (
    TEST_REMOTE_REGISTRY,
    AWS_PROFILE_NAME,
    TEST_DATASET_DIRECTORY,
)
from tests_aws.test_registry.common import clean_remote_registry

from dsgrid.tests.make_us_data_registry import make_test_data_registry


@pytest.fixture
def local_registry(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        with make_test_data_registry(str(base_dir), make_test_project_dir, TEST_DATASET_DIRECTORY):
            # create resstock data
            shutil.copytree(
                base_dir / "data" / "test_efs_comstock", base_dir / "data" / "test_efs_resstock"
            )
            with open(base_dir / "data" / "test_efs_resstock" / "registry.json5") as f:
                registry_json5 = json5.load(f)
            registry_json5["dataset_id"] = "test_efs_resstock"
            with open(base_dir / "data" / "test_efs_resstock" / "registry.json5", "w") as f:
                json5.dump(registry_json5, f)

            # create resstock dataset config
            shutil.copytree(
                base_dir / "configs" / "datasets" / "test_efs_comstock",
                base_dir / "configs" / "datasets" / "test_efs_resstock",
            )
            with open(
                base_dir / "configs" / "datasets" / "test_efs_resstock" / "registry.json5"
            ) as f:
                registry_json5 = json5.load(f)
            registry_json5["dataset_id"] = "test_efs_resstock"
            with open(
                base_dir / "configs" / "datasets" / "test_efs_resstock" / "registry.json5", "w"
            ) as f:
                json5.dump(registry_json5, f)

            # add ressotck dataset to the project config
            with open(
                base_dir / "configs" / "projects" / "test_efs" / "1.1.0" / "project.json5"
            ) as f:
                registry_json5 = json5.load(f)
            x = registry_json5["datasets"][0].copy()
            x["dataset_id"] = "test_efs_resstock"
            registry_json5["datasets"] = [registry_json5["datasets"][0], x]
            with open(
                base_dir / "configs" / "projects" / "test_efs" / "1.1.0" / "project.json5", "w"
            ) as f:
                json5.dump(registry_json5, f)

            yield base_dir


def test_data_sync_project_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                project_id = "test_efs"
                dataset_id = None
                registry_manager.data_sync(project_id, dataset_id)

                assert (
                    len(
                        LocalFilesystem().listdir(
                            Path(local_registry_data_sync) / "data", directories_only=True
                        )
                    )
                    == 2
                ), "Datasets downloaded should be 2"
        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                project_id = None
                dataset_id = "test_efs_comstock"
                registry_manager.data_sync(project_id, dataset_id)

                assert (
                    len(
                        LocalFilesystem().listdir(
                            Path(local_registry_data_sync) / "data", directories_only=True
                        )
                    )
                    == 1
                ), "Datasets downloaded should be 1"

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id_and_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                project_id = "test_efs"
                dataset_id = "test_efs_comstock"
                registry_manager.data_sync(project_id, dataset_id)

                assert (
                    len(
                        LocalFilesystem().listdir(
                            Path(local_registry_data_sync) / "data", directories_only=True
                        )
                    )
                    == 1
                ), "Datasets downloaded should be 1"

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_project_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                with pytest.raises(DSGValueNotRegistered):
                    project_id = "Bad_ID"
                    dataset_id = None
                    registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                with pytest.raises(DSGValueNotRegistered):
                    project_id = None
                    dataset_id = "bad_test"
                    registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id_and_bad_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                with pytest.raises(DSGValueNotRegistered):
                    project_id = "test_efs"
                    dataset_id = "bad_test"
                    registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id2(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                project_id = "test_efs"
                dataset_id = None
                registry_manager.data_sync(project_id, dataset_id)

                assert (
                    len(
                        LocalFilesystem().listdir(
                            Path(local_registry_data_sync) / "data", directories_only=True
                        )
                    )
                    == 2
                ), "Datasets downloaded should be 2"
        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_project_id_with_dataset_lock(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        s3_cloudinterface = S3StorageInterface(
            remote_path=TEST_REMOTE_REGISTRY,
            local_path=local_registry,
            user="Test",
            uuid=str(uuid.uuid4()),
            profile=AWS_PROFILE_NAME,
        )
        try:
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )

            with RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                no_prompts=True,
            ) as registry_manager:
                with s3_cloudinterface.make_lock_file_managed(
                    f"{TEST_REMOTE_REGISTRY}/configs/datasets/test_efs_comstock/.locks/test.lock"
                ):
                    project_id = "test_efs"
                    dataset_id = None
                    registry_manager.data_sync(project_id, dataset_id)

                    assert (
                        len(
                            LocalFilesystem().listdir(
                                Path(local_registry_data_sync) / "data", directories_only=True
                            )
                        )
                        == 2
                    ), "Datasets downloaded should be 1"
        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
