"""Main CLI command for dsgrid."""

import logging

import click

from dsgrid.cli.download import download
from dsgrid.cli.query import query
from dsgrid.cli.registry import registry
from dsgrid.cli.submit import submit
from dsgrid.loggers import setup_logging


logger = logging.getLogger(__name__)


@click.group()
@click.option("-l", "--log-file", type=str, help="Log to this file. Default is console-only.")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def cli(log_file, verbose):
    """dsgrid commands"""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", log_file, console_level=level, file_level=level)


cli.add_command(download)
cli.add_command(query)
cli.add_command(registry)
cli.add_command(submit)
