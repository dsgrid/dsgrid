"""Main CLI command for dsgrid."""

import logging

import click

from dsgrid.utils.timing import timer_stats_collector
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


@cli.resultcallback()
def callback(*args, **kwargs):
    # Raise the console level so that timer stats only go to the log file.
    dsgrid_logger = logging.getLogger("dsgrid")
    for i, handler in enumerate(dsgrid_logger.handlers):
        if handler.name == "console":
            handler.setLevel(logging.WARNING)
            break

    timer_stats_collector.log_stats()

    # Leave the console logger changed because the process will now exit.


cli.add_command(download)
cli.add_command(query)
cli.add_command(registry)
cli.add_command(submit)
