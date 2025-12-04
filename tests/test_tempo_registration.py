import getpass
import logging
from pathlib import Path

import pytest

from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry
from dsgrid.utils.files import load_data
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)


logger = logging.getLogger()


# This is disabled because the test data is not finalized.
@pytest.mark.skip
def test_register_project_and_dataset(
    make_standard_scenarios_project_dir, cached_registry, tmp_path
):
    pass
    # conn = cached_registry
    # base_dir = Path(tmpdir)
    # manager = make_registry_for_tempo(
    #     base_dir, make_standard_scenarios_project_dir, conn, TEST_DATASET_DIRECTORY
    # )
    # TODO: not working yet
    # dataset_mgr = manager.dataset_manager
    # config_ids = dataset_mgr.list_ids()
    # assert len(config_ids) == 1
    # assert config_ids[0] == "tempo_standard_scenarios_2021"


def make_registry_for_tempo(registry_path, src_dir, conn) -> RegistryManager:
    """Creates a local registry to test registration of TEMPO dimensions and dataset.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    conn : DatabaseConnection
        Database connection for the registry.

    """
    create_local_test_registry(registry_path, conn=conn)
    dataset_dir = Path("datasets/modeled/tempo_standard_scenarios_2021")
    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(conn, offline_mode=True)
    dim_mgr = manager.dimension_manager
    dim_mgr.register_from_config(src_dir / src_dir / "dimensions.json5", user, log_message)
    dim_mgr.register_from_config(src_dir / dataset_dir / "dimensions.json5", user, log_message)

    project_config_file = src_dir / "project.json5"
    project_id = load_data(project_config_file)["project_id"]
    dataset_config_file = src_dir / dataset_dir / "dataset.json5"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    mappings = map_dimension_names_to_ids(manager.dimension_manager)
    for filename in (project_config_file, dataset_config_file):
        replace_dimension_names_with_current_ids(filename, mappings)

    manager.dataset_manager.register(dataset_config_file, user, log_message)
    manager.project_manager.register(project_config_file, user, log_message)
    manager.project_manager.submit_dataset(
        project_id,
        dataset_id,
        [],
        user,
        log_message,
    )
    return manager
