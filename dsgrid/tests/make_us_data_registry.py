import getpass
import logging
import os
import shutil
import tempfile
from pathlib import Path

import rich_click as click

from dsgrid.cli.common import path_callback
from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.common import DataStoreType, DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    TEST_DATASET_DIRECTORY,
    TEST_REMOTE_REGISTRY,
    TEST_PROJECT_REPO,
)
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)


logger = logging.getLogger(__name__)


def make_test_data_registry(
    registry_path,
    src_dir,
    dataset_path=None,
    include_projects=True,
    include_datasets=True,
    offline_mode=True,
    database_url: str | None = None,
    data_store_type: DataStoreType = DataStoreType.FILESYSTEM,
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
    data_store_type: DataStoreType
        Type of store to use for the registry data.
    """
    if not include_projects and include_datasets:
        raise Exception("If include_datasets is True then include_projects must also be True.")

    if dataset_path is None:
        dataset_path = os.environ.get("DSGRID_LOCAL_DATA_DIRECTORY", TEST_DATASET_DIRECTORY)
    dataset_path = Path(dataset_path)
    url = f"sqlite:///{registry_path}/registry.db" if database_url is None else database_url
    conn = DatabaseConnection(url=url)
    create_local_test_registry(registry_path, conn=conn, data_store_type=data_store_type)
    dataset_dirs = [
        Path("datasets/modeled/comstock"),
        Path("datasets/modeled/comstock_unpivoted"),
    ]

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
    dataset_config_files = [src_dir / path / "dataset.json5" for path in dataset_dirs]
    dataset_mapping_files = [src_dir / path / "dimension_mappings.json5" for path in dataset_dirs]
    for i, filename in enumerate(dataset_mapping_files):
        if not filename.exists():
            dataset_mapping_files[i] = None
    dataset_ids = [load_data(config_file)["dataset_id"] for config_file in dataset_config_files]

    if include_projects:
        print("\n 1. register project: \n")
        manager.project_manager.register(
            project_config_file,
            user,
            log_message,
        )
    if include_datasets:
        for i, dataset_config_file in enumerate(dataset_config_files):
            dataset_id = dataset_ids[i]
            print(f"\n 2. register dataset {dataset_id}: \n")
            dataset_mapping_file = dataset_mapping_files[i]
            mappings = map_dimension_names_to_ids(manager.dimension_manager)
            replace_dimension_names_with_current_ids(dataset_config_file, mappings)
            manager.dataset_manager.register(
                dataset_config_file,
                dataset_path / dataset_id,
                user,
                log_message,
            )
            print(f"\n 3. submit dataset {dataset_id} to project\n")
            manager.project_manager.submit_dataset(
                project_id,
                dataset_id,
                user,
                log_message,
                dimension_mapping_file=dataset_mapping_file,
            )
    return manager


@click.command()
@click.argument(
    "registry-path",
    type=Path,
    default=f"{Path.home()}/.dsgrid-test-registry",
    callback=path_callback,
)
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
    help="path to a project repository",
    callback=path_callback,
)
@click.option(
    "-d",
    "--dataset-dir",
    default=TEST_DATASET_DIRECTORY,
    help="path to your local datasets",
    callback=path_callback,
)
@click.option(
    "-t",
    "--data-store-type",
    type=click.Choice([x.value for x in DataStoreType]),
    default=DataStoreType.FILESYSTEM.value,
    show_default=True,
    help="Type of store to use for the registry data.",
    callback=lambda *x: DataStoreType(x[2]),
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def run(
    registry_path: Path,
    force: bool,
    project_dir: Path,
    dataset_dir: Path,
    data_store_type: DataStoreType,
    verbose: bool,
):
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
        make_test_data_registry(
            registry_path,
            tmp_project_dir / "dsgrid_project",
            dataset_dir,
            data_store_type=data_store_type,
        )
    finally:
        timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
