import logging
import sys
from pathlib import Path
from typing import Any

import rich_click as click

from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.exceptions import DSGBaseException


logger = logging.getLogger(__name__)


def check_output_directory(path: Path, fs_interface, force: bool):
    """Ensures that the parameter path is an empty directory.

    Parameters
    ----------
    path : Path
    fs_interface : FilesystemInterface
    force : bool
        If False and the directory exists and has content, exit.
    """
    if path.exists():
        if not bool(path.iterdir()):
            return
        if force:
            fs_interface.rm_tree(path)
        else:
            print(
                f"{path} already exists. Choose a different name or pass --force to overwrite it.",
                file=sys.stderr,
            )
            sys.exit(1)

    path.mkdir()


def get_log_level_from_str(level):
    """Convert a log level string to logging type."""
    match level:
        case "debug":
            return logging.DEBUG
        case "info":
            return logging.INFO
        case "warning":
            return logging.WARNING
        case "error":
            return logging.ERROR
        case _:
            msg = f"Unsupported level={level}"
            raise Exception(msg)


def get_value_from_context(ctx, field) -> Any:
    """Get the field value from the root of a click context."""
    return ctx.find_root().params[field]


def handle_dsgrid_exception(ctx, func, *args, **kwargs) -> tuple[Any, int]:
    """Handle any dsgrid exceptions as specified by the CLI parameters."""
    res = None
    try:
        res = func(*args, **kwargs)
        return res, 0
    except DSGBaseException:
        exc_type, exc_value, exc_tb = sys.exc_info()
        filename = exc_tb.tb_frame.f_code.co_filename
        line = exc_tb.tb_lineno
        msg = f'{func.__name__} failed: exception={exc_type.__name__} message="{exc_value}" {filename=} {line=}'
        logger.error(msg)
        if ctx.find_root().params["reraise_exceptions"]:
            raise
        return res, 1


def handle_scratch_dir(*args):
    """Handle the user input for scratch_dir. If a path is passed, ensure it exists."""
    val = args[2]
    if val is None:
        return val
    path = Path(val)
    if not path.exists:
        msg = f"scratch-dir={path} does not exist"
        raise ValueError(msg)
    return path


def path_callback(*args) -> Path | None:
    """Ensure that a Path CLI option value is returned as a Path object."""
    val = args[2]
    if val is None:
        return val
    return Path(val)


# Copied from
# https://stackoverflow.com/questions/45868549/creating-a-click-option-with-prompt-that-shows-only-if-default-value-is-empty
# and modified for our desired password behavior.


class OptionPromptPassword(click.Option):
    """Custom class that only prompts for the password if the user set a different username value
    than what is in the runtime config file."""

    def get_default(self, ctx, **kwargs):
        config = DsgridRuntimeConfig.load()
        username = ctx.find_root().params.get("username")
        if username != config.database_user:
            return None
        return config.database_password

    def prompt_for_value(self, ctx):
        default = self.get_default(ctx)

        if default is None:
            return super().prompt_for_value(ctx)

        return default
