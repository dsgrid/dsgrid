import getpass
import os
from pathlib import Path

import pytest
from semver import VersionInfo

from dsgrid.exceptions import DSGInvalidParameter, DSGInvalidOperation
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.project_registry_manager import ProjectRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.common import DataStoreType, DatabaseConnection, VersionUpdateType
from dsgrid.utils.files import dump_data, load_data

TEST_PROJECT_PATH = Path(__file__).absolute().parents[2] / "dsgrid-test-data"
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_efs"
TEST_STANDARD_SCENARIOS_PROJECT_REPO = TEST_PROJECT_PATH / "standard_scenarios_2021"
TEST_DATASET_DIRECTORY = TEST_PROJECT_PATH / "datasets"
TEST_REGISTRY_DATABASE = "cached-test-dsgrid"
TEST_REGISTRY_BASE_PATH = Path("tests/data/registry")
TEST_REGISTRY_DATA_PATH = Path("tests/data/registry/registry_data")
TEST_EFS_REGISTRATION_FILE = Path("tests/data/test_efs_registration.json5")
# AWS_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REMOTE_REGISTRY = "s3://nrel-dsgrid-registry-test"
CACHED_TEST_REGISTRY_DB = f"sqlite:///{TEST_REGISTRY_BASE_PATH}/cached_registry.db"
STANDARD_SCENARIOS_PROJECT_REPO = Path(__file__).parents[2] / "dsgrid-project-StandardScenarios"
IEF_PROJECT_REPO = Path(__file__).parents[2] / "dsgrid-project-IEF"
SIMPLE_STANDARD_SCENARIOS = TEST_PROJECT_PATH / "filtered_registries" / "simple_standard_scenarios"
SIMPLE_STANDARD_SCENARIOS_REGISTRY_DB = (
    f"sqlite:///{TEST_PROJECT_PATH}/filtered_registries/simple_standard_scenarios/registry.db"
)


def create_local_test_registry(
    tmpdir: Path, conn=None, data_store_type: DataStoreType = DataStoreType.FILESYSTEM
):
    if conn is None:
        conn = DatabaseConnection(url=f"sqlite:///{tmpdir}/dsgrid-test.db")
    data_path = tmpdir / "registry_data"
    mgr = RegistryManager.create(conn, data_path, data_store_type=data_store_type, overwrite=True)
    mgr.dispose()
    return data_path


def check_configs_update(base_dir, manager):
    """Runs an update on one of each type of config.

    Parameters
    ----------
    base_dir : Path
    manager : RegistryManager

    Returns
    -------
    list
        Each updated config and new version: [(Updated ID, new version)]
        For the dimension element the tuple is (Updated ID, dimension type, new version).
        Order is dimension ID, dimension mapping ID, dataset ID, project ID

    """
    update_dir = base_dir / "updates"
    user = getpass.getuser()

    updated_ids = []
    for mgr in (
        manager.dimension_manager,
        manager.dimension_mapping_manager,
        manager.dataset_manager,
        manager.project_manager,
    ):
        config_id = mgr.list_ids()[0]
        version = mgr.get_latest_version(config_id)
        check_config_update(update_dir, mgr, config_id, user, version)
        new_version = mgr.get_latest_version(config_id)
        if isinstance(mgr, DimensionRegistryManager):
            config = mgr.get_by_id(config_id)
            updated_ids.append((config_id, config.model.dimension_type, new_version))
        else:
            updated_ids.append((config_id, new_version))

    return updated_ids


def check_config_update(base_dir, mgr: ProjectRegistryManager, config_id, user, version):
    """Runs basic positive and negative update tests for the config.

    Parameters
    ----------
    base_dir : str
    mgr : RegistryManagerBase
    config_id : str
    user : str
    version : str

    """
    config_file = Path(base_dir) / mgr.config_class().config_filename()
    assert not config_file.exists()
    try:
        mgr.dump(config_id, base_dir, force=True)
        with pytest.raises(DSGInvalidOperation):
            mgr.dump(config_id, base_dir)
        mgr.dump(config_id, base_dir, force=True)
        assert config_file.exists()
        config_data = load_data(config_file)
        config_data["description"] += "; updated description"
        dump_data(config_data, config_file)
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                "invalid_config_id",
                user,
                VersionUpdateType.PATCH,
                "update to description",
                version,
            )
        with pytest.raises(DSGInvalidParameter):
            mgr.update_from_file(
                config_file,
                config_id,
                user,
                VersionUpdateType.PATCH,
                "update to description",
                str(VersionInfo.parse(version).bump_patch()),
            )

        mgr.update_from_file(
            config_file,
            config_id,
            user,
            VersionUpdateType.PATCH,
            "update to description",
            version,
        )
        assert (
            VersionInfo.parse(mgr.get_latest_version(config_id))
            == VersionInfo.parse(version).bump_patch()
        )
    finally:
        if config_file.exists():
            os.remove(config_file)
