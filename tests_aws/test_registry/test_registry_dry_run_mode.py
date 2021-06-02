from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
import getpass
from pathlib import Path
from tempfile import TemporaryDirectory
import getpass
import uuid

from .common import clean_remote_registry

from dsgrid.tests.common import create_local_test_registry
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.cloud.s3_storage_interface import S3StorageInterface

DATA_REPO = os.environ.get("US_DATA_REPO")
S3_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REGISTRY = "s3://nrel-dsgrid-registry-test"


def test_dry_run_mode(make_test_project_dir):
    """test that registry dry_run_mode does not push registry changes to remote registry"""
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
            path = create_local_test_registry(base_dir)
            assert LocalFilesystem().listdir(path)
            dataset_dir = Path("datasets/sector_models/comstock")
            clean_remote_registry(s3_cloud_storage._s3_filesystem)
            manager = RegistryManager.load(
                path=path,
                remote_path=TEST_REGISTRY,
                user=submitter,
                no_prompts=True,
                dry_run_mode=True,
            )
            for dim_config_file in (
                make_test_project_dir / "dimensions.toml",
                make_test_project_dir / dataset_dir / "dimensions.toml",
            ):
                manager.dimension_manager.register(dim_config_file, submitter, log_message)
            assert s3_cloud_storage._s3_filesystem.listdir() == []
    finally:
        clean_remote_registry(s3_cloud_storage._s3_filesystem)
