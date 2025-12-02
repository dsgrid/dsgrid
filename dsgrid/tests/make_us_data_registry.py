import contextlib
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
    TEST_REMOTE_REGISTRY,
    TEST_PROJECT_REPO,
    TEST_DATASET_DIRECTORY,
)
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.id_remappings import (
    map_dimension_names_to_ids,
    replace_dimension_names_with_current_ids,
)


logger = logging.getLogger(__name__)


def _find_file_with_stem(directory: Path, stem: str) -> Path | None:
    """Find a file in directory with the given stem, regardless of extension."""
    for path in directory.iterdir():
        if path.stem == stem:
            return path
    return None


def update_dataset_config_paths(config_file: Path, dataset_id: str) -> None:
    """Update the data file paths in a dataset config to be relative to the config file.

    Parameters
    ----------
    config_file : Path
        Path to the dataset configuration file.
    dataset_id : str
        The dataset ID, used to locate the data files in TEST_DATASET_DIRECTORY.
    """
    data = load_data(config_file)
    if "table_schema" not in data:
        return

    table_schema = data["table_schema"]
    config_dir = config_file.parent.resolve()
    dataset_data_dir = (TEST_DATASET_DIRECTORY / dataset_id).resolve()

    if "data_file" in table_schema:
        stem = Path(table_schema["data_file"]["path"]).stem
        data_file_path = _find_file_with_stem(dataset_data_dir, stem)
        if data_file_path is None:
            msg = f"Could not find data file with stem '{stem}' in {dataset_data_dir}"
            raise FileNotFoundError(msg)
        relative_path = os.path.relpath(data_file_path, config_dir)
        table_schema["data_file"]["path"] = relative_path

    if "lookup_data_file" in table_schema and table_schema["lookup_data_file"] is not None:
        stem = Path(table_schema["lookup_data_file"]["path"]).stem
        lookup_file_path = _find_file_with_stem(dataset_data_dir, stem)
        if lookup_file_path is None:
            msg = f"Could not find lookup file with stem '{stem}' in {dataset_data_dir}"
            raise FileNotFoundError(msg)
        relative_path = os.path.relpath(lookup_file_path, config_dir)
        table_schema["lookup_data_file"]["path"] = relative_path

    if "missing_associations" in table_schema and table_schema["missing_associations"] is not None:
        items = []
        for item in table_schema["missing_associations"]:
            stem = Path(item).stem
            missing_path = _find_file_with_stem(dataset_data_dir, stem)
            if missing_path is None:
                msg = (
                    f"Could not find missing associations with stem '{stem}' in {dataset_data_dir}"
                )
                raise FileNotFoundError(msg)
            relative_path = os.path.relpath(missing_path, config_dir)
            items.append(relative_path)
        table_schema["missing_associations"] = items

    dump_data(data, config_file)


@contextlib.contextmanager
def make_test_data_registry(
    registry_path,
    src_dir,
    include_projects=True,
    include_datasets=True,
    offline_mode=True,
    database_url: str | None = None,
    data_store_type: DataStoreType = DataStoreType.FILESYSTEM,
):
    """Creates a local registry from a dsgrid project source directory for testing.

    This is a context manager that yields the RegistryManager and disposes it on exit.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    include_projects : bool
        If False, do not register any projects.
    include_datasets : bool
        If False, do not register any datasets.
    offline_mode : bool
        If False, use the test remote registry.
    data_store_type: DataStoreType
        Type of store to use for the registry data.

    Yields
    ------
    RegistryManager
    """
    if not include_projects and include_datasets:
        msg = "If include_datasets is True then include_projects must also be True."
        raise Exception(msg)
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

    try:
        project_config_file = src_dir / "project.json5"
        project_id = load_data(project_config_file)["project_id"]
        dataset_config_files = [src_dir / path / "dataset.json5" for path in dataset_dirs]
        dataset_mapping_files = [
            src_dir / path / "dimension_mappings.json5" for path in dataset_dirs
        ]
        for i, filename in enumerate(dataset_mapping_files):
            if not filename.exists():
                dataset_mapping_files[i] = None
        dataset_ids = [
            load_data(config_file)["dataset_id"] for config_file in dataset_config_files
        ]

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
                update_dataset_config_paths(dataset_config_file, dataset_id)
                manager.dataset_manager.register(
                    dataset_config_file,
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
        yield manager
    finally:
        manager.dispose()


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
        with make_test_data_registry(
            registry_path,
            tmp_project_dir / "dsgrid_project",
            data_store_type=data_store_type,
        ):
            pass  # Manager is created and disposed in context manager
    finally:
        timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
