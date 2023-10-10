"""CLI commands to manage the dsgrid runtime configuration"""

import logging

import click

from dsgrid.dsgrid_rc import DsgridRuntimeConfig


logger = logging.getLogger(__name__)


@click.group()
def config():
    """Config commands"""


@click.command()
@click.option(
    "--timings/--no-timings",
    default=False,
    is_flag=True,
    show_default=True,
    help="Enable tracking of function timings.",
)
@click.option(
    "-N",
    "--database-name",
    type=str,
    default=None,
    help="Database name",
)
@click.option(
    "-u",
    "--database-url",
    type=str,
    default=None,
    help="Database URL. Ex: http://localhost:8529",
)
@click.option(
    "-o",
    "--offline",
    is_flag=True,
    default=False,
    show_default=True,
    help="Run registry commands in offline mode. WARNING: any commands you perform in offline "
    "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
    "commands will not be officially synced with the remote registry",
)
@click.option(
    "--console-level",
    default="info",
    show_default=True,
    help="Console log level.",
)
@click.option(
    "--file-level",
    default="info",
    show_default=True,
    help="File log level.",
)
def create(
    timings,
    database_name,
    database_url,
    offline,
    console_level,
    file_level,
):
    """Create a local dsgrid runtime configuration file."""
    dsgrid_config = DsgridRuntimeConfig(
        timings=timings,
        database_name=database_name,
        database_url=database_url,
        offline=offline,
        console_level=console_level,
        file_level=file_level,
    )
    dsgrid_config.dump()


config.add_command(create)
