from dsgrid.filesystem.local_filesystem import LocalFilesystem
import os
import shutil
import getpass
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
import getpass
import uuid


from dsgrid.tests.common import create_local_test_registry, make_test_project_dir

from .common import clean_remote_registry, create_empty_remote_registry

from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.cloud.s3_storage_interface import S3StorageInterface

S3_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REGISTRY = "s3://nrel-dsgrid-registry-test"


def test_aws_registry_workflow_online_mode(make_test_project_dir):
    """Test the registry workflow where online_mode=True, using the test remote registry"""
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

            # create temp local registry
            path = create_local_test_registry(base_dir)
            assert LocalFilesystem().listdir(path)
            dataset_dir = Path("datasets/sector_models/comstock")

            # refresh remote test registry (clean/ create empty)
            clean_remote_registry(s3_cloud_storage._s3_filesystem)
            create_empty_remote_registry(s3_cloud_storage._s3_filesystem)

            manager = RegistryManager.load(
                path=path, remote_path=TEST_REGISTRY, user=submitter, no_prompts=True
            )

            # test registry dimensions for project and dataset
            for dim_config_file in (
                make_test_project_dir / "dimensions.toml",
                make_test_project_dir / dataset_dir / "dimensions.toml",
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

            # TODO: finish workflow when ready

    finally:
        clean_remote_registry(s3_cloud_storage._s3_filesystem)
