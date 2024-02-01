"""CLI commands to manage the dsgrid runtime configuration"""

import getpass
import logging

import click

from dsgrid.common import DEFAULT_DB_PASSWORD
from dsgrid.cli.common import handle_scratch_dir
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
    "--url",
    type=str,
    default=None,
    help="Database URL. Ex: http://localhost:8529",
)
@click.option(
    "-U",
    "--username",
    type=str,
    default=getpass.getuser(),
    help="Database username",
)
@click.option(
    "-P",
    "--password",
    prompt=True,
    hide_input=True,
    type=str,
    default=DEFAULT_DB_PASSWORD,
    help="Database username",
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
@click.option(
    "-r",
    "--reraise-exceptions",
    is_flag=True,
    default=False,
    show_default=True,
    help="Re-raise any dsgrid exception. Default is to log the exception and exit.",
)
@click.option(
    "-s",
    "--scratch-dir",
    default=None,
    callback=handle_scratch_dir,
    help="Base directory for dsgrid temporary directories. Must be accessible on all compute "
    "nodes. Defaults to the current directory.",
)
def create(
    timings,
    database_name,
    url,
    username,
    password,
    offline,
    console_level,
    file_level,
    reraise_exceptions,
    scratch_dir,
):
    """Create a local dsgrid runtime configuration file."""
    dsgrid_config = DsgridRuntimeConfig(
        timings=timings,
        database_name=database_name,
        database_url=url,
        database_user=username,
        database_password=password,
        offline=offline,
        console_level=console_level,
        file_level=file_level,
        reraise_exceptions=reraise_exceptions,
        scratch_dir=scratch_dir,
    )
    dsgrid_config.dump()


config.add_command(create)
