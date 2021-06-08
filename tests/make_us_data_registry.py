import getpass
import logging
import os
import shutil
import tempfile
from pathlib import Path

import click

from dsgrid.loggers import setup_logging
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry
from tests.common import replace_dimension_uuids_from_registry
from tests.common import replace_dimension_mapping_uuids_from_registry


logger = logging.getLogger(__name__)


def make_us_data_registry(registry_path, repo) -> RegistryManager:
    """Creates a local registry with the dsgrid-data-UnitedStates repository for testing."""
    path = create_local_test_registry(registry_path)
    dataset_dir = Path("datasets/sector_models/comstock")
    project_dimension_mapping_config = repo / "dimension_mappings.toml"
    dimension_mapping_config = repo / dataset_dir / "dimension_mappings.toml"
    dimension_mapping_refs = repo / dataset_dir / "dimension_mapping_references.toml"

    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(path, offline_mode=True)

    for dim_config_file in (
        repo / "dimensions.toml",
        repo / dataset_dir / "dimensions.toml",
    ):
        dim_mgr = manager.dimension_manager
        dim_mgr.register(dim_config_file, user, log_message)

    replace_dimension_uuids_from_registry(
        path, (project_dimension_mapping_config, dimension_mapping_config)
    )
    dim_mapping_mgr = manager.dimension_mapping_manager
    dim_mapping_mgr.register(project_dimension_mapping_config, user, log_message)
    dim_mapping_mgr.register(dimension_mapping_config, user, log_message)

    project_config_file = repo / "project.toml"
    dataset_config_file = repo / dataset_dir / "dataset.toml"
    replace_dimension_mapping_uuids_from_registry(
        path, (project_config_file, dimension_mapping_refs)
    )
    replace_dimension_uuids_from_registry(path, (project_config_file, dataset_config_file))

    manager.project_manager.register(project_config_file, user, log_message)
    manager.dataset_manager.register(dataset_config_file, user, log_message)
    manager.project_manager.submit_dataset(
        "test",
        "efs_comstock",
        [dimension_mapping_refs],
        user,
        log_message,
    )
    return manager


@click.command()
@click.argument("registry-path", type=Path)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete registry-path if it exists.",
)
@click.option(
    "-d",
    "--data-repo",
    envvar="US_DATA_REPO",
    required=True,
    help="path to dsgrid-data-UnitedStates registry. Override with the environment variable US_DATA_REPO",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(registry_path, force, data_repo, verbose):
    """Creates a local registry with the dsgrid-data-UnitedStates repository for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", "dsgrid_us.log", console_level=level, file_level=level, mode="a")
    if registry_path.exists():
        if force:
            shutil.rmtree(registry_path)
        else:
            print(f"{registry_path} already exists. Use --force to overwrite.")
    os.makedirs(registry_path)
    tmp_data_repo = Path(tempfile.gettempdir()) / "tmp_us_data_repo"
    if tmp_data_repo.exists():
        shutil.rmtree(tmp_data_repo)
    shutil.copytree(data_repo, tmp_data_repo)
    make_us_data_registry(registry_path, tmp_data_repo / "dsgrid_project")


if __name__ == "__main__":
    run()
