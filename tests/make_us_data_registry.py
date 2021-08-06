import getpass
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry, TEST_DATASET_DIRECTORY
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data, dump_data
from dsgrid.tests.common import replace_dimension_uuids_from_registry, TEST_PROJECT_REPO
from dsgrid.tests.common import replace_dimension_mapping_uuids_from_registry


logger = logging.getLogger(__name__)


def make_test_data_registry(
    registry_path, src_dir, dataset_path=None, include_datasets=True
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
    include_datasets : bool
        If False, do not register any datasets.

    """
    if dataset_path is None:
        dataset_path = os.environ["DSGRID_LOCAL_DATA_DIRECTORY"]
    path = create_local_test_registry(registry_path)
    dataset_dir = Path("datasets/sector_models/comstock")
    project_dimension_mapping_config = src_dir / "dimension_mappings.toml"
    dimension_mapping_config = src_dir / dataset_dir / "dimension_mappings.toml"
    dimension_mapping_refs = src_dir / dataset_dir / "dimension_mapping_references.toml"

    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(path, offline_mode=True)

    for dim_config_file in (
        src_dir / "dimensions.toml",
        src_dir / dataset_dir / "dimensions.toml",
    ):
        dim_mgr = manager.dimension_manager
        dim_mgr.register(dim_config_file, user, log_message)

    replace_dimension_uuids_from_registry(
        path, (project_dimension_mapping_config, dimension_mapping_config)
    )
    dim_mapping_mgr = manager.dimension_mapping_manager
    dim_mapping_mgr.register(project_dimension_mapping_config, user, log_message)
    dim_mapping_mgr.register(dimension_mapping_config, user, log_message)

    project_config_file = src_dir / "project.toml"
    project_id = load_data(project_config_file)["project_id"]
    dataset_config_file = src_dir / dataset_dir / "dataset.toml"
    dataset_id = load_data(dataset_config_file)["dataset_id"]
    replace_dataset_path(dataset_config_file, dataset_path=dataset_path)
    replace_dimension_mapping_uuids_from_registry(
        path, (project_config_file, dimension_mapping_refs)
    )
    replace_dimension_uuids_from_registry(path, (project_config_file, dataset_config_file))

    manager.project_manager.register(project_config_file, user, log_message)
    if include_datasets:
        manager.dataset_manager.register(dataset_config_file, user, log_message)
        manager.project_manager.submit_dataset(
            project_id,
            dataset_id,
            [dimension_mapping_refs],
            user,
            log_message,
        )
    return manager


def replace_dataset_path(dataset_config_file, dataset_path):
    config = load_data(dataset_config_file)
    src_data = Path(dataset_path) / config["dataset_id"]
    config["path"] = str(src_data)
    dump_data(config, dataset_config_file)
    logger.info("Replaced dataset path in %s with %s", dataset_config_file, src_data)


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
    make_test_data_registry(registry_path, tmp_project_dir / "dsgrid_project", dataset_dir)
    timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
