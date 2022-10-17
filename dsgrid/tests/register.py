import getpass
import logging
import shutil
from pathlib import Path

import click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    replace_dimension_mapping_uuids_from_registry,
    replace_dimension_uuids_from_registry,
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
        if registration.registry_path.exists():
            shutil.rmtree(registration.registry_path)
        create_local_test_registry(registration.registry_path)

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
    reg_path = registration.registry_path
    manager = RegistryManager.load(reg_path, offline_mode=True)
    project_mgr = manager.project_manager
    dataset_mgr = manager.dataset_manager

    for project in registration.projects:
        if project.register_project:
            project_mgr.register(project.config_file, user, log_message)

        for dataset in project.datasets:
            if dataset.register_dataset:
                if dataset.fix_dimension_uuids:
                    suffix = dataset.config_file.suffix
                    config_file = Path(str(dataset.config_file).replace(suffix, "__tmp" + suffix))
                    config_file = shutil.copyfile(dataset.config_file, config_file)
                    tmp_files.append(config_file)
                    replace_dimension_uuids_from_registry(reg_path, [config_file])
                else:
                    config_file = dataset.config_file
                dataset_mgr.register(
                    config_file,
                    dataset.dataset_path,
                    user,
                    log_message,
                )

            if dataset.submit_to_project:
                if dataset.fix_dimension_mapping_uuids:
                    suffix = dataset.dimension_mapping_file.suffix
                    mapping_file = Path(
                        str(dataset.dimension_mapping_file).replace(suffix, "__tmp" + suffix)
                    )
                    mapping_file = shutil.copyfile(dataset.dimension_mapping_file, mapping_file)
                    tmp_files.append(mapping_file)
                    replace_dimension_mapping_uuids_from_registry(reg_path, [mapping_file])
                else:
                    mapping_file = dataset.dimension_mapping_file
                project_mgr.submit_dataset(
                    project.project_id,
                    dataset.dataset_id,
                    user,
                    log_message,
                    dimension_mapping_file=mapping_file,
                )
                pass


if __name__ == "__main__":
    run()
