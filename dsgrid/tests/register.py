import getpass
import logging
import shutil
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    map_dimension_names_to_ids,
    map_dimension_ids_to_names,
    map_dimension_mapping_names_to_ids,
    replace_dimension_names_with_current_ids,
    replace_dimension_mapping_names_with_current_ids,
)
from dsgrid.tests.registration_models import RegistrationModel, create_registration
from dsgrid.utils.timing import timer_stats_collector


logger = logging.getLogger(__name__)


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(input_file, verbose):
    """Registers projects and datasets in a local registry for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = Path("dsgrid_registration.log")
    check_log_file_size(log_file, no_prompts=True)
    logger = setup_logging("dsgrid", log_file, console_level=level, file_level=level, mode="a")
    registration = create_registration(input_file)
    if registration.create_registry:
        if registration.data_path.exists():
            shutil.rmtree(registration.data_path)
        create_local_test_registry(registration.data_path, conn=registration.conn)

    tmp_files = []
    try:
        _run_registration(registration, tmp_files)
    finally:
        for path in tmp_files:
            path.unlink()
        # Raise the console level so that timer stats only go to the log file.
        for i, handler in enumerate(logger.handlers):
            if handler.name == "console":
                handler.setLevel(logging.WARNING)
                break

        timer_stats_collector.log_stats()


def _run_registration(registration: RegistrationModel, tmp_files):
    user = getpass.getuser()
    log_message = "Initial registration"
    manager = RegistryManager.load(registration.conn, offline_mode=True)
    project_mgr = manager.project_manager
    dataset_mgr = manager.dataset_manager
    dim_mgr = manager.dimension_manager
    dim_mapping_mgr = manager.dimension_mapping_manager

    for project in registration.projects:
        if project.register_project:
            project_mgr.register(project.config_file, user, log_message)

        for dataset in project.datasets:
            refs_file = None
            config_file = None
            if dataset.register_dataset:
                if dataset.replace_dimension_names_with_ids:
                    mappings = map_dimension_names_to_ids(dim_mgr)
                    orig = dataset.config_file
                    config_file = orig.with_stem(orig.name + "__tmp")
                    shutil.copyfile(orig, config_file)
                    tmp_files.append(config_file)
                    replace_dimension_names_with_current_ids(config_file, mappings)
                else:
                    config_file = dataset.config_file

            if dataset.submit_to_project:
                if (
                    dataset.replace_dimension_mapping_names_with_ids
                    and dataset.dimension_mapping_references_file is not None
                ):
                    dim_id_to_name = map_dimension_ids_to_names(manager.dimension_manager)
                    mappings = map_dimension_mapping_names_to_ids(dim_mapping_mgr, dim_id_to_name)
                    orig = dataset.dimension_mapping_references_file
                    refs_file = orig.with_stem(orig.name + "__tmp")
                    shutil.copyfile(orig, refs_file)
                    tmp_files.append(refs_file)
                    replace_dimension_mapping_names_with_current_ids(refs_file, mappings)
                else:
                    refs_file = dataset.dimension_mapping_references_file

            if dataset.register_dataset and dataset.submit_to_project:
                # If something fails in the submit stage, dsgrid will delete the dataset.
                project_mgr.register_and_submit_dataset(
                    config_file,
                    dataset.dataset_path,
                    project.project_id,
                    user,
                    log_message,
                    dimension_mapping_file=dataset.dimension_mapping_file,
                    dimension_mapping_references_file=refs_file,
                    autogen_reverse_supplemental_mappings=dataset.autogen_reverse_supplemental_mappings,
                )
            elif dataset.register_dataset:
                dataset_mgr.register(
                    config_file,
                    dataset.dataset_path,
                    user,
                    log_message,
                )
            elif dataset.submit_to_project:
                project_mgr.submit_dataset(
                    project.project_id,
                    dataset.dataset_id,
                    user,
                    log_message,
                    dimension_mapping_file=dataset.dimension_mapping_file,
                    dimension_mapping_references_file=refs_file,
                    autogen_reverse_supplemental_mappings=dataset.autogen_reverse_supplemental_mappings,
                )


if __name__ == "__main__":
    run()
