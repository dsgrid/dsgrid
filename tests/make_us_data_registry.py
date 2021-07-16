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
from dsgrid.tests.common import create_local_test_registry, LOCAL_DATA_DIRECTORY
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data, dump_data
from dsgrid.tests.common import replace_dimension_uuids_from_registry
from dsgrid.tests.common import replace_dimension_mapping_uuids_from_registry


logger = logging.getLogger(__name__)


def make_us_data_registry(registry_path, repo) -> RegistryManager:
    """Creates a local registry with the dsgrid-project-EFS repository for testing."""
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
    replace_dataset_path(dataset_config_file)
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


def replace_dataset_path(dataset_config_file):
    if LOCAL_DATA_DIRECTORY is None:
        print(
            "You must define the environment DSGRID_LOCAL_DATA_DIRECTORY with the path to your "
            "copy of datasets."
        )
        sys.exit(1)
    config = load_data(dataset_config_file)
    src_data = Path(LOCAL_DATA_DIRECTORY) / config["dataset_id"]
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
    "--project-repo",
    envvar="TEST_PROJECT_REPO",
    required=True,
    help="path to dsgrid-project-EFS registry. Override with the environment variable TEST_PROJECT_REPO",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(registry_path, force, project_repo, verbose):
    """Creates a local registry with the dsgrid-project-EFS repository for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = Path("dsgrid_us.log")
    check_log_file_size(log_file, no_prompts=True)
    setup_logging("dsgrid", log_file, console_level=level, file_level=level, mode="a")
    if registry_path.exists():
        if force:
            shutil.rmtree(registry_path)
        else:
            print(f"{registry_path} already exists. Use --force to overwrite.")
    os.makedirs(registry_path)
    tmp_project_repo = Path(tempfile.gettempdir()) / "tmp_test_project_repo"
    if tmp_project_repo.exists():
        shutil.rmtree(tmp_project_repo)
    shutil.copytree(project_repo, tmp_project_repo)
    make_us_data_registry(registry_path, tmp_project_repo / "dsgrid_project")
    timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
