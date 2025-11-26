import getpass
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from dsgrid.tests.common import (
    check_configs_update,
    AWS_PROFILE_NAME,
    TEST_REMOTE_REGISTRY,
)

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.dimension.base_models import DimensionType
from dsgrid.tests.make_us_data_registry import make_test_data_registry
from .common import clean_remote_registry, create_empty_remote_registry


def test_aws_registry_workflow_online_mode(make_test_project_dir, make_test_data_dir):
    """Test the registry workflow where online_mode=True, using the test remote registry"""
    try:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            submitter = getpass.getuser()
            s3_cloud_storage = S3StorageInterface(
                local_path=base_dir,
                remote_path=TEST_REMOTE_REGISTRY,
                user=submitter,
                uuid=str(uuid.uuid4()),
                profile=AWS_PROFILE_NAME,
            )

            # refresh remote test registry (clean/ create empty)
            clean_remote_registry(s3_cloud_storage._s3_filesystem)
            create_empty_remote_registry(s3_cloud_storage._s3_filesystem)

            with make_test_data_registry(
                base_dir,
                make_test_project_dir,
                dataset_path=make_test_data_dir,
                include_datasets=True,
                offline_mode=False,
            ) as manager:
                dataset_dir = make_test_project_dir / "datasets" / "modeled" / "comstock"
                assert dataset_dir.exists()
                dimension_mapping_refs = dataset_dir / "dimension_mapping_references.json5"
                assert dimension_mapping_refs.exists()

                # TODO: finish workflow when ready (submit dataset)

                # check that we didn't push unexpected things...
                s3 = manager.dimension_manager.cloud_interface._s3_filesystem
                assert s3.listdir("") == ["configs", "data"]
                check_configs_dimensions(s3)
                check_configs_dimension_mappings(s3)
                check_configs_projects_and_datasets(s3)
                check_data(s3)
                updated_ids = check_configs_update(base_dir, manager)
                check_dimension_version(s3, *updated_ids[0])
                check_dimension_mapping_version(s3, *updated_ids[1])
                check_dataset_version(s3, *updated_ids[2])
                check_project_version(s3, *updated_ids[3])
    finally:
        clean_remote_registry(s3_cloud_storage._s3_filesystem)


def check_configs_dimensions(s3):
    for dim_type in DimensionType:
        files = s3.listdir(f"configs/dimensions/{dim_type.value}")
        for file in files:
            assert s3.listdir(f"configs/dimensions/{dim_type.value}/{file}") == [
                "1.0.0",
                "registry.json5",
            ], (
                s3.listdir(f"configs/dimensions/{dim_type.value}/{file}")
                == ["1.0.0", "registry.json5"],
                f"configs/dimensions/{dim_type.value}/{file}",
                file,
            )


def check_configs_dimension_mappings(s3):
    path = "configs/dimension_mappings"
    folders = s3.listdir(path)
    for folder in folders:
        files = s3.listdir(f"{path}/{folder}")
        for file in files:
            assert file in ("1.0.0", "registry.json5")
            if file != "registry.json5":
                files2 = s3.listdir(f"{path}/{folder}/{file}")
                for file in files2:
                    assert len(files2) == 2
                    extensions = [file.split(".")[-1] for file in files2]
                    for extension in extensions:
                        assert extension in ("csv", "json5")


def check_configs_projects_and_datasets(s3):
    paths = ("configs/projects", "configs/datasets")
    for path in paths:
        folders = s3.listdir(path)
        for folder in folders:
            files = s3.listdir(f"{path}/{folder}")
            for file in files:
                if "projects" in path:
                    assert file in ("1.0.0", "1.1.0", "registry.json5")
                    # project.toml
                    expected_file_count = 1
                else:
                    assert file in ("1.0.0", "registry.json5")
                    expected_file_count = 1  # dataset.json5
                if file != "registry.json5":
                    files2 = s3.listdir(f"{path}/{folder}/{file}")
                    for file in files2:
                        assert len(files2) == expected_file_count
                        extensions = [file.split(".")[-1] for file in files2]
                        for extension in extensions:
                            assert extension in ("csv", "json5")


def check_dimension_version(s3, config_id, dimension_type, version):
    assert "dimension.json5" in s3.listdir(
        f"configs/dimensions/{dimension_type.value}/{config_id}/{version}"
    )


def check_dimension_mapping_version(s3, config_id, version):
    assert "dimension_mapping.json5" in s3.listdir(
        f"configs/dimension_mappings/{config_id}/{version}"
    )


def check_dataset_version(s3, config_id, version):
    assert "dataset.json5" in s3.listdir(f"configs/datasets/{config_id}/{version}")


def check_project_version(s3, config_id, version):
    assert "project.json5" in s3.listdir(f"configs/projects/{config_id}/{version}")


def check_data(s3):
    path = "data/"
    folders = s3.listdir(path)
    for folder in folders:
        files = s3.listdir(f"{path}/{folder}")
        for file in files:
            assert file in ("1.0.0", "registry.json5")
            if file != "registry.json5":
                files2 = s3.listdir(f"{path}/{folder}/{file}")
                for file in files2:
                    assert len(files2) == 2
                    extensions = [file.split(".")[-1] for file in files2]
                    for extension in extensions:
                        assert extension in ("csv", "json")
