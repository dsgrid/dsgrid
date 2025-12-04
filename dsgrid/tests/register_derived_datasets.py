import logging
import shutil
from pathlib import Path

import rich_click as click

from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.query.models import ProjectQueryModel
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.timing import timer_stats_collector


logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "query_files",
    nargs=-1,
    type=click.Path(exists=True),
    callback=lambda *x: [Path(y) for y in x[2]],
)
@click.option(
    "-r", "--registry-path", required=True, callback=lambda *x: Path(x[2]), help="Path to registry"
)
@click.option(
    "-o",
    "--output",
    default="query_output",
    show_default=True,
    type=click.Path(),
    help="Output directory for query results",
    callback=lambda *x: Path(x[2]),
)
@click.option(
    "-p",
    "--project-id",
    default="dsgrid_conus_2022",
    show_default=True,
    type=str,
    help="Project ID",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def run(query_files, project_id, registry_path, output, verbose):
    """Registers derived datasets in a local registry for testing."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = Path("dsgrid_registration.log")
    check_log_file_size(log_file, no_prompts=True)
    logger = setup_logging(__name__, log_file, console_level=level, file_level=level, mode="a")
    try:
        _run_registration(query_files, project_id, registry_path, output)
    finally:
        # Raise the console level so that timer stats only go to the log file.
        for _, handler in enumerate(logger.handlers):
            if handler.name == "console":
                handler.setLevel(logging.WARNING)
                break

        timer_stats_collector.log_stats()


def _run_registration(
    query_files: list[Path], project_id: str, registry_path: Path, query_output_dir: Path
):
    log_message = "Submit derived dataset"
    query_output_dir.mkdir(exist_ok=True)
    derived_dataset_config_dir = query_output_dir / "derived_dataset_configs"
    if derived_dataset_config_dir.exists():
        shutil.rmtree(derived_dataset_config_dir)
    derived_dataset_config_dir.mkdir()
    for query_file in query_files:
        logger.info("Register derived dataset from %s", query_file)
        query = ProjectQueryModel.from_file(query_file)
        dataset_id = query.project.dataset.dataset_id
        dataset_config_dir = derived_dataset_config_dir / dataset_id
        dataset_config_file = dataset_config_dir / DatasetRegistry.config_filename()

        create_cmd = (
            f"dsgrid query project run --registry-path={registry_path} "
            f"-o {query_output_dir} {query_file}"
        )

        config_cmd = (
            f"dsgrid query project create-derived-dataset-config "
            f"--registry-path={registry_path} {query_output_dir / dataset_id} {dataset_config_dir}"
        )

        submit_cmd = (
            f"dsgrid registry --path {registry_path} projects "
            f"register-and-submit-dataset -c {dataset_config_file} -p {project_id} "
            f"-l '{log_message}' -d {query_output_dir / dataset_id}"
        )

        for cmd in (create_cmd, config_cmd, submit_cmd):
            logger.info(cmd)
            check_run_command(cmd)


if __name__ == "__main__":
    run()
