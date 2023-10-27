import logging
import sys
from pathlib import Path

import click

from dsgrid.dsgrid_rc import DsgridRuntimeConfig


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
            raise Exception(f"Unsupported level={level}")


def get_value_from_context(ctx, field) -> str:
    """Get the field value from the root of a click context."""
    return ctx.find_root().params[field]


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
