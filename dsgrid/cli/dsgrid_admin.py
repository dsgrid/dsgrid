"""Main CLI command for dsgrid."""

import logging
import shutil
import sys
from pathlib import Path

import click

from dsgrid.common import LOCAL_REGISTRY, REMOTE_REGISTRY
from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


"""
Click Group Definitions
"""


@click.group()
@click.option("-l", "--log-file", default="dsgrid_admin.log", type=str, help="Log to this file.")
@click.option(
    "-n", "--no-prompts", default=False, is_flag=True, show_default=True, help="Do not prompt."
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def cli(log_file, no_prompts, verbose):
    """dsgrid-admin commands"""
    path = Path(log_file)
    level = logging.DEBUG if verbose else logging.INFO
    check_log_file_size(path, no_prompts=no_prompts)
    setup_logging("dsgrid", path, console_level=level, file_level=level, mode="a")


@click.group()
@click.option(
    "--path",
    default=LOCAL_REGISTRY,
    show_default=True,
    envvar="DSGRID_REGISTRY_PATH",
    help="path to dsgrid registry. Override with the environment variable DSGRID_REGISTRY_PATH",
)
@click.option(
    "--remote-path",
    default=REMOTE_REGISTRY,
    show_default=True,
    help="path to dsgrid remote registry",
)
@click.option(
    "--offline",
    "-o",
    is_flag=True,
    default=False,
    help="run in registry commands in offline mode. WARNING: any commands you perform in offline "
    "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
    "commands will not be officially synced with the remote registry",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    help="run registry commands in dry-run mode without writing to the local or remote registry",
)
@click.pass_context
def registry(ctx, path, remote_path, offline, dry_run):
    """Manage a registry."""
    no_prompts = ctx.parent.params["no_prompts"]
    if "--help" in sys.argv:
        ctx.obj = None
    else:
        ctx.obj = RegistryManager.load(
            path, remote_path, offline_mode=offline, dry_run_mode=dry_run, no_prompts=no_prompts
        )


@click.group()
@click.pass_obj
def dimensions(registry_manager):
    """Dimension subcommands"""


@click.group()
@click.pass_obj
def dimension_mappings(registry_manager):
    """Dimension mapping subcommands"""


@click.group()
@click.pass_obj
def projects(registry_manager):
    """Project subcommands"""


@click.group()
@click.pass_obj
def datasets(registry_manager):
    """Dataset subcommands"""


"""
Registry Commands
"""


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


"""
Dimension Commands
"""


@click.command(name="remove")
@click.argument("dimension-id")
@click.pass_obj
def remove_dimension(registry_manager, dimension_id):
    """Remove a dimension from the dsgrid repository."""
    registry_manager.dimension_manager.remove(dimension_id)


"""
Dimension Mapping Commands
"""


@click.command(name="remove")
@click.argument("dimension-mapping-id")
@click.pass_obj
def remove_dimension_mapping(registry_manager, dimension_mapping_id):
    """Remove a dimension mapping from the dsgrid repository."""
    registry_manager.dimension_mapping_manager.remove(dimension_mapping_id)


"""
Project Commands
"""


@click.command(name="remove")
@click.argument("project-id")
@click.pass_obj
def remove_project(registry_manager, project_id):
    """Remove a project from the dsgrid repository."""
    registry_manager.project_manager.remove(project_id)


"""
Dataset Commands
"""


@click.command(name="remove")
@click.argument("dataset-id")
@click.pass_obj
def remove_dataset(registry_manager, dataset_id):
    """Remove a dataset from the dsgrid repository."""
    registry_manager.dataset_manager.remove(dataset_id)


cli.add_command(registry)
cli.add_command(create_registry)

registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(projects)
registry.add_command(datasets)

dimensions.add_command(remove_dimension)
dimension_mappings.add_command(remove_dimension_mapping)
projects.add_command(remove_project)
datasets.add_command(remove_dataset)
