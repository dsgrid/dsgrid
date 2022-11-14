"""Main CLI command for dsgrid."""

import logging
from pathlib import Path

import click

from dsgrid.utils.timing import timer_stats_collector
from dsgrid.cli.download import download
from dsgrid.cli.install_notebooks import install_notebooks
from dsgrid.cli.query import query
from dsgrid.cli.registry import registry
from dsgrid.loggers import setup_logging, check_log_file_size, disable_console_logging


logger = logging.getLogger(__name__)


@click.group()
@click.option("-l", "--log-file", type=Path, default="dsgrid.log", help="Log to this file.")
@click.option(
    "-n", "--no-prompts", default=False, is_flag=True, show_default=True, help="Do not prompt."
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
@click.pass_context
def cli(ctx, log_file, no_prompts, verbose):
    """dsgrid commands"""
    path = Path(log_file)
    check_log_file_size(path, no_prompts=no_prompts)
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", path, console_level=level, file_level=level, mode="a")


@cli.result_callback()
def callback(*args, **kwargs):
    with disable_console_logging(name="dsgrid"):
        timer_stats_collector.log_stats()


cli.add_command(download)
cli.add_command(install_notebooks)
cli.add_command(query)
cli.add_command(registry)
