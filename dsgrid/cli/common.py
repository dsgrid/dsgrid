import logging
import sys
from pathlib import Path


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
