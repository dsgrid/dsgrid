"""Main CLI command for dsgrid."""

import logging
from pathlib import Path

import rich_click as click

import dsgrid
from chronify.utils.path_utils import check_overwrite
from dsgrid.common import LOCAL_REGISTRY
from dsgrid.registry.common import DatabaseConnection, DataStoreType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.cli.common import get_log_level_from_str, handle_scratch_dir
from dsgrid.cli.config import config
from dsgrid.cli.download import download
from dsgrid.cli.query import query
from dsgrid.cli.registry import registry
from dsgrid.loggers import setup_logging, check_log_file_size, disable_console_logging


logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "-c",
    "--console-level",
    default=dsgrid.runtime_config.console_level,
    show_default=True,
    help="Console log level.",
)
@click.option(
    "-f",
    "--file-level",
    default=dsgrid.runtime_config.file_level,
    show_default=True,
    help="File log level.",
)
@click.option("-l", "--log-file", type=Path, default="dsgrid.log", help="Log to this file.")
@click.option(
    "-n", "--no-prompts", default=False, is_flag=True, show_default=True, help="Do not prompt."
)
@click.option(
    "--timings/--no-timings",
    default=dsgrid.runtime_config.timings,
    is_flag=True,
    show_default=True,
    help="Enable tracking of function timings.",
)
@click.option(
    "-u",
    "--url",
    type=str,
    default=dsgrid.runtime_config.database_url,
    envvar="DSGRID_REGISTRY_DATABASE_URL",
    help="Database URL. Ex: http://localhost:8529",
)
@click.option(
    "-r",
    "--reraise-exceptions",
    is_flag=True,
    default=dsgrid.runtime_config.reraise_exceptions,
    show_default=True,
    help="Re-raise any dsgrid exception. Default is to log the exception and exit.",
)
@click.option(
    "-s",
    "--scratch-dir",
    default=dsgrid.runtime_config.scratch_dir,
    callback=handle_scratch_dir,
    help="Base directory for dsgrid temporary directories. Must be accessible on all compute "
    "nodes. Defaults to the current directory.",
)
@click.pass_context
def cli(
    ctx,
    console_level,
    file_level,
    log_file,
    no_prompts,
    timings,
    url,
    reraise_exceptions,
    scratch_dir,
):
    """dsgrid commands"""
    if timings:
        timer_stats_collector.enable()
    else:
        timer_stats_collector.disable()
    path = Path(log_file)
    check_log_file_size(path, no_prompts=no_prompts)
    ctx.params["console_level"] = get_log_level_from_str(console_level)
    ctx.params["file_level"] = get_log_level_from_str(file_level)
    setup_logging(
        "dsgrid",
        path,
        console_level=ctx.params["console_level"],
        file_level=ctx.params["file_level"],
        mode="a",
    )


@cli.result_callback()
def callback(*args, **kwargs):
    with disable_console_logging(name="dsgrid"):
        timer_stats_collector.log_stats()


_create_registry_epilog = """
Examples:\n
$ dsgrid create-registry sqlite:////projects/dsgrid/my_project/registry.db -p /projects/dsgrid/my_project/registry-data\n
"""


@click.command(name="create-registry", epilog=_create_registry_epilog)
@click.argument("url")
@click.option(
    "-p",
    "--data-path",
    default=LOCAL_REGISTRY,
    show_default=True,
    callback=lambda *x: Path(x[2]),
    help="Local dsgrid registry data path. Must not contain the registry file listed in URL.",
)
@click.option(
    "-f",
    "--overwrite",
    "--force",
    is_flag=True,
    default=False,
    help="Delete registry_path and the database if they already exist.",
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
def create_registry(url: str, data_path: Path, overwrite: bool, data_store_type: DataStoreType):
    """Create a new registry."""
    check_overwrite(data_path, overwrite)
    conn = DatabaseConnection(url=url)
    RegistryManager.create(conn, data_path, overwrite=overwrite, data_store_type=data_store_type)


cli.add_command(config)
cli.add_command(create_registry)
cli.add_command(download)
cli.add_command(query)
cli.add_command(registry)
