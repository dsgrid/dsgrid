import getpass
import logging
import os
import shutil
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data
from dsgrid.tests.common import replace_dimension_uuids_from_registry


logger = logging.getLogger(__name__)


def make_standard_scenarios_registry(
    registry_path,
    src_dir,
    dataset_path=None,
    include_projects=True,
    include_datasets=True,
) -> RegistryManager:
    """Creates a local registry from a dsgrid-project-StandardScenarios source directory.

    Parameters
    ----------
    registry_path : Path
        Path in which the registry will be created.
    src_dir : Path
        Path containing source config files
    dataset_path : Path | None
    include_projects : bool
        If False, do not register any projects.
    include_datasets : bool
        If False, do not register any datasets.

    """
    if not include_projects and include_datasets:
        raise Exception("If include_datasets is True then include_projects must also be True.")

    path = create_local_test_registry(registry_path)
    project_config_file = src_dir / "project.json5"
    dataset_base = Path("datasets/modeled")
    dataset_ids = (
        "comstock_conus_2022_reference",
        "resstockconus_2022_reference",
        "tempo_conus_2022",
    )
    dataset_dirs = ("comstock", "resstock", "tempo")

    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(path, offline_mode=True)

    project_id = load_data(project_config_file)["project_id"]
    if include_projects:
        manager.project_manager.register(project_config_file, user, log_message)

    dim_uuid_replacements = []
    dim_mapping_uuid_replacements = []
    for dataset_dir in dataset_dirs:
        dataset_config_file = src_dir / dataset_base / dataset_dir / "dataset.json5"
        dim_uuid_replacements.append(dataset_config_file)
        dim_mapping_uuid_replacements.append(dataset_config_file)

    replace_dimension_uuids_from_registry(path, dim_uuid_replacements)

    if include_datasets:
        dataset_path = Path(dataset_path)
        for dataset_id, dataset_dir in zip(dataset_ids, dataset_dirs):
            config_path = src_dir / dataset_base / dataset_dir
            dataset_config_file = config_path / "dataset.json5"
            manager.dataset_manager.register(
                dataset_config_file, dataset_path / dataset_id, user, log_message
            )
            dimension_mapping_file = config_path / "dimension_mappings.json5"
            if not dimension_mapping_file.exists():
                dimension_mapping_file = None
            manager.project_manager.submit_dataset(
                project_id,
                dataset_id,
                user,
                log_message,
                dimension_mapping_file=dimension_mapping_file,
            )
    return manager


@click.command()
@click.argument("registry-path", type=Path, default=f"{Path.home()}/.standard-scenarios-registry")
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
    required=True,
    help="path to a project repository",
)
@click.option(
    "-d",
    "--dataset-path",
    default=None,
    help="path to your local datasets",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(registry_path, force, project_dir, dataset_path, verbose):
    """Creates a local registry from a dsgrid project source directory for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = Path("standard_scenarios_project.log")
    check_log_file_size(log_file, no_prompts=True)
    setup_logging("dsgrid", log_file, console_level=level, file_level=level, mode="a")
    if registry_path.exists():
        if force:
            shutil.rmtree(registry_path)
        else:
            print(f"{registry_path} already exists. Use --force to overwrite.")
    os.makedirs(registry_path)

    # Use a path that will be accessible to all workers across compute nodes.
    tmp_project_dir = Path(".") / "tmp_project_dir"
    try:
        if tmp_project_dir.exists():
            shutil.rmtree(tmp_project_dir)
        shutil.copytree(project_dir, tmp_project_dir)
        include_datasets = dataset_path is not None
        make_standard_scenarios_registry(
            registry_path,
            tmp_project_dir / "dsgrid_project",
            dataset_path=dataset_path,
            include_datasets=include_datasets,
        )
    finally:
        shutil.rmtree(tmp_project_dir)
        timer_stats_collector.log_stats()


if __name__ == "__main__":
    run()
