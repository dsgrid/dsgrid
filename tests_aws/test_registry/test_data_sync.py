import pytest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
import shutil
import toml

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY, SYNC_EXCLUDE_LIST
from dsgrid.tests.common import (
    TEST_PROJECT_PATH,
    TEST_REMOTE_REGISTRY,
    AWS_PROFILE_NAME,
    TEST_DATASET_DIRECTORY,
    make_test_project_dir,
)
from tests_aws.test_registry.common import clean_remote_registry

from dsgrid.tests.make_us_data_registry import make_test_data_registry


@pytest.fixture
def local_registry(make_test_project_dir):
    with TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        make_test_data_registry(str(base_dir), make_test_project_dir, TEST_DATASET_DIRECTORY)

        # create resstock data
        shutil.copytree(
            base_dir / "data" / "test_efs_comstock", base_dir / "data" / "test_efs_resstock"
        )
        registry_toml = toml.load(base_dir / "data" / "test_efs_resstock" / "registry.toml")
        registry_toml["dataset_id"] = "test_efs_resstock"
        with open(base_dir / "data" / "test_efs_resstock" / "registry.toml", "w") as f:
            toml.dump(registry_toml, f)

        # create resstock dataset config
        shutil.copytree(
            base_dir / "configs" / "datasets" / "test_efs_comstock",
            base_dir / "configs" / "datasets" / "test_efs_resstock",
        )
        registry_toml = toml.load(
            base_dir / "configs" / "datasets" / "test_efs_resstock" / "registry.toml"
        )
        registry_toml["dataset_id"] = "test_efs_resstock"
        with open(
            base_dir / "configs" / "datasets" / "test_efs_resstock" / "registry.toml", "w"
        ) as f:
            toml.dump(registry_toml, f)

        # add ressotck dataset to the project config
        registry_toml = toml.load(
            base_dir / "configs" / "projects" / "test_efs" / "1.1.0" / "project.toml"
        )
        x = registry_toml["datasets"][0].copy()
        x["dataset_id"] = "test_efs_resstock"
        registry_toml["datasets"] = [registry_toml["datasets"][0], x]
        with open(
            base_dir / "configs" / "projects" / "test_efs" / "1.1.0" / "project.toml", "w"
        ) as f:
            toml.dump(registry_toml, f)

        yield base_dir


def test_data_sync_project_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

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
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

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
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

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
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

            with pytest.raises(DSGValueNotRegistered):
                project_id = "Bad_ID"
                dataset_id = None
                registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_bad_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

            with pytest.raises(DSGValueNotRegistered):
                project_id = None
                dataset_id = "bad_test"
                registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id_and_bad_dataset_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

            with pytest.raises(DSGValueNotRegistered):
                project_id = "test_efs"
                dataset_id = "bad_test"
                registry_manager.data_sync(project_id, dataset_id)

        finally:
            LocalFilesystem().rm_tree(local_registry_data_sync)
            clean_remote_registry(s3_cloudinterface._s3_filesystem)


def test_data_sync_project_id(local_registry):
    with TemporaryDirectory() as local_registry_data_sync:
        try:
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )
            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

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
        try:
            clean_remote_registry(s3_cloudinterface._s3_filesystem)
            s3_cloudinterface = S3StorageInterface(
                remote_path=TEST_REMOTE_REGISTRY,
                local_path=local_registry,
                user="Test",
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )
            s3_cloudinterface.sync_push(
                local_path=local_registry,
                remote_path=TEST_REMOTE_REGISTRY,
                exclude=SYNC_EXCLUDE_LIST,
            )

            registry_manager = RegistryManager.load(
                local_registry_data_sync,
                TEST_REMOTE_REGISTRY,
                offline_mode=False,
                dry_run_mode=False,
                no_prompts=True,
            )

            with s3_cloudinterface.make_lock_file(
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
