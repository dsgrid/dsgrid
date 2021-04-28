"""Main CLI command for dsgrid."""

import logging

import click

from dsgrid.loggers import setup_logging
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


@click.group()
@click.option("-l", "--log-file", type=str, help="Log to this file. Default is console-only.")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def cli(log_file, verbose):
    """dsgrid-internal commands"""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", log_file, console_level=level, file_level=level)


@click.command()
@click.argument("registry_path")
def create_registry(registry_path):
    """Create a new registry."""
    RegistryManager.create(registry_path)


cli.add_command(create_registry)
