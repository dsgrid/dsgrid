"""CLI for dsgrid admin commands (testing purposes only)."""

import logging
from pathlib import Path

import rich_click as click

from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.filter_registry_manager import FilterRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import load_data


logger = logging.getLogger(__name__)
_config = DsgridRuntimeConfig.load()


@click.group()
@click.option("-l", "--log-file", default="dsgrid_admin.log", type=str, help="Log to this file.")
@click.option(
    "-n", "--no-prompts", default=False, is_flag=True, show_default=True, help="Do not prompt."
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def cli(log_file, no_prompts, verbose):
    """dsgrid-admin commands (for testing purposes only)"""
    path = Path(log_file)
    level = logging.DEBUG if verbose else logging.INFO
    check_log_file_size(path, no_prompts=no_prompts)
    setup_logging("dsgrid", path, console_level=level, file_level=level, mode="a")


@click.command()
@click.option(
    "--src-database-url",
    required=True,
    help="Source dsgrid registry database URL.",
)
@click.option(
    "--dst-database-url",
    default="dsgrid",
    required=True,
    help="Destination dsgrid registry database URL.",
)
@click.argument("dst_data_path", type=click.Path(exists=False), callback=lambda *x: Path(x[2]))
@click.argument("config_file", type=click.Path(exists=True), callback=lambda *x: Path(x[2]))
@click.option(
    "-m",
    "--mode",
    default="data-symlinks",
    type=click.Choice(["copy", "data-symlinks", "rsync"]),
    show_default=True,
    help="Controls whether to copy all data, make symlinks to data files, or sync data with the "
    "rsync utility (not available on Windows).",
)
@click.option(
    "-f",
    "--overwrite",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite dst_registry_path if it already exists. Does not apply if using rsync.",
)
def make_filtered_registry(
    src_database_url,
    dst_database_url,
    dst_data_path: Path,
    config_file: Path,
    mode,
    overwrite,
):
    """Make a filtered registry for testing purposes."""
    simple_model = RegistrySimpleModel(**load_data(config_file))
    src_conn = DatabaseConnection(url=src_database_url)
    dst_conn = DatabaseConnection(url=dst_database_url)
    RegistryManager.copy(
        src_conn,
        dst_conn,
        dst_data_path,
        mode=mode,
        force=overwrite,
    )
    with FilterRegistryManager.load(dst_conn, offline_mode=True, use_remote_data=False) as mgr:
        mgr.filter(simple_model=simple_model)


cli.add_command(make_filtered_registry)
