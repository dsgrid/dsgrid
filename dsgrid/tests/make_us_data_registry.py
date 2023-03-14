import getpass
import logging
import os
import shutil
import tempfile
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    TEST_DATASET_DIRECTORY,
    TEST_REMOTE_REGISTRY,
)
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data
from dsgrid.tests.common import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
    TEST_PROJECT_REPO,
)


logger = logging.getLogger(__name__)


def make_test_data_registry(
    registry_path,
    src_dir,
    database_name="test-dsgrid",
    dataset_path=None,
    include_projects=True,
    include_datasets=True,
    offline_mode=True,
) -> RegistryManager:
    """Creates a local registry from a dsgrid project source directory for testing.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    dataset_path : Path | None
        If None, use "DSGRID_LOCAL_DATA_DIRECTORY" env variable.
    include_projects : bool
        If False, do not register any projects.
    include_datasets : bool
        If False, do not register any datasets.
    offline_mode : bool
        If False, use the test remote registry.
    """
    if not include_projects and include_datasets:
        raise Exception("If include_datasets is True then include_projects must also be True.")

    if dataset_path is None:
        dataset_path = os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY", TEST_DATASET_DIRECTORY)
    dataset_path = Path(dataset_path)
    conn = DatabaseConnection(database=database_name)
    create_local_test_registry(registry_path, conn=conn)
    dataset_dir = Path("datasets/modeled/comstock")

    user = getpass.getuser()
    log_message = "Initial registration"
    if offline_mode:
        manager = RegistryManager.load(conn, offline_mode=offline_mode)
    else:
        manager = RegistryManager.load(
            conn, remote_path=TEST_REMOTE_REGISTRY, offline_mode=offline_mode
        )

    project_config_file = src_dir / "project.json5"
    project_id = load_data(project_config_file)["project_id"]
    dataset_config_file = src_dir / dataset_dir / "dataset.json5"
    dataset_mapping_file = src_dir / dataset_dir / "dimension_mappings.json5"
    if not dataset_mapping_file.exists():
        dataset_mapping_file = None
    dataset_id = load_data(dataset_config_file)["dataset_id"]

    if include_projects:
        print("\n 1. register project: \n")
        manager.project_manager.register(
            project_config_file,
            user,
            log_message,
        )
    if include_datasets:
        print("\n 2. register dataset: \n")
        mappings = map_dimension_names_to_ids(manager.dimension_manager)
        replace_dimension_names_with_current_ids(dataset_config_file, mappings)
        manager.dataset_manager.register(
            dataset_config_file,
            dataset_path / dataset_id,
            user,
            log_message,
        )
        print("\n 3. submit dataset to project\n")
        manager.project_manager.submit_dataset(
            project_id,
            dataset_id,
            user,
            log_message,
            dimension_mapping_file=dataset_mapping_file,
        )
    return manager


@click.command()
@click.argument("registry-path", type=Path, default=f"{Path.home()}/.dsgrid-test-registry")
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Delete registry-path if it exists.",
)
@click.option(
    "-p",
    "--project-dir",
    default=TEST_PROJECT_REPO,
    required=True,
    help="path to a project repository",
)
@click.option(
    "-d",
    "--dataset-dir",
    default=TEST_DATASET_DIRECTORY,
    required=True,
    help="path to your local datasets",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(registry_path, force, project_dir, dataset_dir, verbose):
    """Creates a local registry from a dsgrid project source directory for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = Path("test_dsgrid_project.log")
    check_log_file_size(log_file, no_prompts=True)
    setup_logging("dsgrid", log_file, console_level=level, file_level=level, mode="a")
    if registry_path.exists():
        if force:
            shutil.rmtree(registry_path)
        else:
            print(f"{registry_path} already exists. Use --force to overwrite.")
    os.makedirs(registry_path)
    tmp_project_dir = Path(tempfile.gettempdir()) / "tmp_test_project_dir"
    if tmp_project_dir.exists():
        shutil.rmtree(tmp_project_dir)
    shutil.copytree(project_dir, tmp_project_dir)
    try:
        make_test_data_registry(registry_path, tmp_project_dir / "dsgrid_project", dataset_dir)
    finally:
        timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
