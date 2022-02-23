import getpass
import logging
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import create_local_test_registry
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.utils.files import load_data, dump_data
from dsgrid.tests.common import (
    replace_dimension_uuids_from_registry,
    replace_dimension_mapping_uuids_from_registry,
)


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
    efs_repo = os.environ.get("DSGRID_EFS_REPO")
    if efs_repo is None:
        # Required because StandardScenarios uses dimensions from this project.
        print(
            "You must define the path to a local copy of dsgrid-project-EFS in the env "
            "variable DSGRID_EFS_REPO",
            file=sys.stderr,
        )
        sys.exit(1)

    path = create_local_test_registry(registry_path)
    project_config_file = src_dir / "project.toml"
    dataset_dir = Path("datasets/sector_models")
    dataset_ids = ("conus_2022_reference_comstock", "conus_2022_reference_resstock")
    dataset_dirs = ("comstock", "resstock")
    project_dimension_mapping_config = src_dir / "dimension_mappings.toml"

    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(path, offline_mode=True)
    dim_mgr = manager.dimension_manager
    dim_mapping_mgr = manager.dimension_mapping_manager

    dimension_files = [src_dir / "dimensions.toml", f"{efs_repo}/dsgrid_project/dimensions.toml"]
    dimension_mapping_files = [project_dimension_mapping_config]
    dim_uuid_replacements = [project_config_file, project_dimension_mapping_config]
    dim_mapping_uuid_replacements = [project_config_file]
    mapping_refs = defaultdict(list)
    for dataset_id in dataset_dirs:  # Yes, this is odd.
        dim_config_file = src_dir / dataset_dir / dataset_id / "dimensions.toml"
        dimension_files.append(dim_config_file)
        dim_mapping_config_file = src_dir / dataset_dir / dataset_id / "dimension_mappings.toml"
        if dim_mapping_config_file.exists():
            dimension_mapping_files.append(dim_mapping_config_file)

        dataset_config_file = src_dir / dataset_dir / dataset_id / "dataset.toml"
        dim_uuid_replacements.append(dataset_config_file),
        dim_mapping_uuid_replacements.append(dataset_config_file)
        if dim_mapping_config_file.exists():
            dim_uuid_replacements.append(dim_mapping_config_file)
        dimension_mapping_refs = (
            src_dir / dataset_dir / dataset_id / "dimension_mapping_references.toml"
        )
        if dimension_mapping_refs.exists():
            dim_mapping_uuid_replacements.append(dimension_mapping_refs)
            mapping_refs[dataset_id].append(dimension_mapping_refs)

    for filename in dimension_files:
        dim_mgr.register(filename, user, log_message)

    replace_dimension_uuids_from_registry(path, dim_uuid_replacements)

    for filename in dimension_mapping_files:
        dim_mapping_mgr.register(filename, user, log_message)
    replace_dimension_mapping_uuids_from_registry(path, dim_mapping_uuid_replacements)

    project_id = load_data(project_config_file)["project_id"]
    if include_projects:
        manager.project_manager.register(project_config_file, user, log_message)

    if include_datasets:
        dataset_path = Path(dataset_path)
        for dataset_id, ddir in zip(dataset_ids, dataset_dirs):
            dataset_config_file = src_dir / dataset_dir / ddir / "dataset.toml"
            replace_dataset_path(dataset_config_file, dataset_path=dataset_path)

            manager.dataset_manager.register(dataset_config_file, user, log_message)
            manager.project_manager.submit_dataset(
                project_id,
                dataset_id,
                mapping_refs[ddir],
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
        assert not tmp_project_dir.exists()
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
        timer_stats_collector.log_stats()
    finally:
        shutil.rmtree(tmp_project_dir)


if __name__ == "__main__":
    run()
