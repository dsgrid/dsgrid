"""Main CLI command for dsgrid."""

import logging
from pathlib import Path

import rich_click as click

import dsgrid
from dsgrid.utils.timing import timer_stats_collector
from dsgrid.cli.common import get_log_level_from_str, handle_scratch_dir
from dsgrid.cli.config import config
from dsgrid.cli.download import download
from dsgrid.cli.install_notebooks import install_notebooks
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
# Offline mode is permanently on because we do not currently support a remote, official registry.
# @click.option(
#     "--offline/--online",
#     is_flag=True,
#     default=dsgrid.runtime_config.offline,
#     show_default=True,
#     help="Run registry commands in offline mode. WARNING: any commands you perform in offline "
#     "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
#     "commands will not be officially synced with the remote registry",
# )
@click.option(
    "--timings/--no-timings",
    default=dsgrid.runtime_config.timings,
    is_flag=True,
    show_default=True,
    help="Enable tracking of function timings.",
)
# Server-related options are commented-out because the registry is currently only
# supported in SQLite. If/when we add postgres support, these can be added back.
# @click.option(
#    "-U",
#    "--username",
#    type=str,
#    default=dsgrid.runtime_config.database_user,
#    help="Database username",
# )
# @click.option(
#    "-P",
#    "--password",
#    prompt=True,
#    hide_input=True,
#    cls=OptionPromptPassword,
#    help="dsgrid registry password. Will prompt unless it is passed or the username matches the "
#    "runtime config file.",
# )
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
    # offline,
    timings,
    # username,
    # password,
    url,
    reraise_exceptions,
    scratch_dir,
):
    """dsgrid commands"""
    if timings:
        timer_stats_collector.enable()
    else:
        timer_stats_collector.disable()
    ctx.params["offline"] = True
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


cli.add_command(config)
cli.add_command(download)
cli.add_command(install_notebooks)
cli.add_command(query)
cli.add_command(registry)
