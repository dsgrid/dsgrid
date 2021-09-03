"""Main CLI command for dsgrid."""

import logging
import shutil
import sys
from pathlib import Path

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
@click.option(
    "-f", "--force", is_flag=True, default=False, help="Delete registry_path if it already exists."
)
def create_registry(registry_path, force):
    """Create a new registry."""
    if Path(registry_path).exists():
        if force:
            shutil.rmtree(registry_path)
        else:
            print(f"{registry_path} already exists. Set --force to overwrite.", file=sys.stderr)
            sys.exit(1)
    RegistryManager.create(registry_path)


cli.add_command(create_registry)
