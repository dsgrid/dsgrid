import sys
from pathlib import Path


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
