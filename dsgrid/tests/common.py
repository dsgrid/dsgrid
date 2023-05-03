import getpass
import os
from pathlib import Path

import pytest
from semver import VersionInfo

from dsgrid.exceptions import DSGInvalidParameter, DSGInvalidOperation
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.common import VersionUpdateType
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.utils.files import dump_data, load_data

TEST_PROJECT_PATH = Path(__file__).absolute().parent.parent.parent / "dsgrid-test-data"
TEST_PROJECT_REPO = TEST_PROJECT_PATH / "test_efs"
TEST_STANDARD_SCENARIOS_PROJECT_REPO = TEST_PROJECT_PATH / "standard_scenarios_2021"
TEST_DATASET_DIRECTORY = TEST_PROJECT_PATH / "datasets"
TEST_REGISTRY_DATABASE = "cached-test-dsgrid"
TEST_REGISTRY_PATH = Path("tests/data/registry")
TEST_EFS_REGISTRATION_FILE = Path("tests/data/test_efs_registration.json5")
AWS_PROFILE_NAME = "nrel-aws-dsgrid"
TEST_REMOTE_REGISTRY = "s3://nrel-dsgrid-registry-test"


def create_local_test_registry(tmpdir, conn=None):
    if conn is None:
        conn = DatabaseConnection(database="test-dsgrid")
    data_path = Path(tmpdir)
    RegistryManager.create(conn, data_path)
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


def check_config_update(base_dir, mgr, config_id, user, version):
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


def map_dimension_names_to_ids(dimension_mgr):
    mapping = {}
    for dim in dimension_mgr.db.dimensions:
        if dim["name"] in mapping:
            assert mapping[dim["name"]] == dim["dimension_id"], dim
        mapping[dim["name"]] = dim["dimension_id"]
    return mapping


def map_dimension_ids_to_names(dimension_mgr):
    mapping = {}
    for dim in dimension_mgr.db.dimensions:
        assert dim["dimension_id"] not in mapping, dim
        mapping[dim["dimension_id"]] = dim["name"]
    return mapping


def map_dimension_mapping_names_to_ids(dimension_mapping_mgr, dim_id_to_name):
    mapping = {}
    for dmap in dimension_mapping_mgr.db.dimension_mappings:
        key = (
            dim_id_to_name[dmap["from_dimension"]["dimension_id"]],
            dim_id_to_name[dmap["to_dimension"]["dimension_id"]],
        )
        if key in mapping:
            assert mapping[key] == dmap["mapping_id"], dmap
        mapping[key] = dmap["mapping_id"]
    return mapping


def replace_dimension_names_with_current_ids(filename, mappings):
    data = load_data(filename)

    def perform_replacements(mappings, dimensions):
        changed = False
        for ref in dimensions:
            if "name" in ref:
                ref["dimension_id"] = mappings[ref.pop("name")]
                changed = True
        return changed

    changed = False
    if "dimension_references" in data:
        # This is True for a dataset config file.
        if perform_replacements(mappings, data["dimension_references"]):
            changed = True

    if "dimensions" in data and "base_dimension_references" in data["dimensions"]:
        # This is True for a project config file.
        if perform_replacements(mappings, data["dimensions"]["base_dimension_references"]):
            changed = True
        if perform_replacements(mappings, data["dimensions"]["supplemental_dimension_references"]):
            changed = True

    if "mappings" in data:
        # This is True for a dimension mappings file.
        for mapping in data["mappings"]:
            if perform_replacements(
                mappings, [mapping["from_dimension"], mapping["to_dimension"]]
            ):
                changed = True

    if changed:
        dump_data(data, filename, indent=2)


def replace_dimension_mapping_names_with_current_ids(filename, mappings):
    data = load_data(filename)

    def perform_replacements(mappings, references):
        changed = False
        for ref in references:
            if "mapping_names" in ref:
                item = ref.pop("mapping_names")
                ref["mapping_id"] = mappings[(item["from"], item["to"])]
                changed = True
        return changed

    changed = False
    if "dimension_mappings" in data:
        # This is True for a project config file.
        refs = data["dimension_mappings"]["base_to_supplemental_references"]
        if perform_replacements(mappings, refs):
            changed = True

    if "references" in data:
        # This is True for a dataset-to-project dimension mapping reference file.
        if perform_replacements(mappings, data["references"]):
            changed = True

    if changed:
        dump_data(data, filename, indent=2)
