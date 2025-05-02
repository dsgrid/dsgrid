import shutil
import sys
from pathlib import Path

import rich_click as click

import dsgrid

NOTEBOOKS_DIRNAME = "dsgrid-notebooks"


@click.command()
@click.option(
    "-p",
    "--path",
    default=Path.home(),
    show_default=True,
    type=click.Path(),
    help="Path to install dsgrid notebooks.",
    callback=lambda _, __, x: Path(x) / NOTEBOOKS_DIRNAME,
)
@click.option(
    "-f",
    "--overwrite",
    "--force",
    default=False,
    show_default=True,
    is_flag=True,
    help="If true, overwrite existing files.",
)
def install_notebooks(path, overwrite):
    """Install dsgrid notebooks to a local path."""
    src_path = Path(dsgrid.__path__[0]) / "notebooks"
    if not src_path.exists():
        print(f"Unexpected error: dsgrid notebooks are not stored in {src_path}", file=sys.stderr)
        sys.exit(1)

    path.mkdir(exist_ok=True, parents=True)
    to_copy = []
    existing = []
    for src_file in src_path.iterdir():
        if src_file.suffix in (".ipynb", ".sh"):
            dst = path / src_file.name
            if dst.exists() and not overwrite:
                existing.append(dst)
            else:
                to_copy.append((src_file, dst))
    if existing:
        print(
            f"Existing files: {[str(x) for x in existing]}. "
            "Choose a different location or set overwrite=true to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not to_copy:
        print("No notebook files found", file=sys.stderr)
        sys.exit(1)

    for src, dst in to_copy:
        shutil.copyfile(src, dst)
        print(f"Installed {dst}")
