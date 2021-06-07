"""Main CLI command for dsgrid."""

import logging
import os
from pathlib import Path

import click

from dsgrid.utils.timing import timer_stats_collector
from dsgrid.cli.download import download
from dsgrid.cli.query import query
from dsgrid.cli.registry import registry
from dsgrid.cli.submit import submit
from dsgrid.loggers import setup_logging


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
    if log_file.exists():
        size_mb = log_file.stat().st_size / (1024 * 1024)
        limit_mb = 10
        if size_mb > limit_mb and not no_prompts:
            msg = f"The log file {log_file} has exceeded {limit_mb} MiB. Delete it? [Y] >>> "
            val = input(msg)
            if val == "" or val.lower() == "y":
                os.remove(log_file)

    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", log_file, console_level=level, file_level=level, mode="a")


@cli.result_callback()
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
