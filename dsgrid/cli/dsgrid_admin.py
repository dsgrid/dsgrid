"""Main CLI command for dsgrid."""

import logging
import shutil
import sys
from pathlib import Path

import click

from dsgrid.common import LOCAL_REGISTRY, REMOTE_REGISTRY
from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.filter_registry_manager import FilterRegistryManager
from dsgrid.utils.files import load_data


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
    "--url",
    default="http://localhost:8529",
    show_default=True,
    envvar="DSGRID_REGISTRY_DATABASE",
    help="dsgrid registry database URL. Override with the environment variable DSGRID_REGISTRY_DATABASE",
)
@click.option(
    "--db-name",
    default="dsgrid",
    show_default=True,
    help="dsgrid registry database name.",
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
@click.pass_context
def registry(ctx, url, db_name, remote_path, offline):
    """Manage a registry."""
    no_prompts = ctx.parent.params["no_prompts"]
    if "--help" in sys.argv:
        ctx.obj = None
    else:
        conn = DatabaseConnection.from_url(url, database=db_name)
        ctx.obj = RegistryManager.load(
            conn, remote_path, offline_mode=offline, no_prompts=no_prompts
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
@click.argument("db_name")
@click.option(
    "-p",
    "--data-path",
    default=LOCAL_REGISTRY,
    show_default=True,
    callback=lambda *x: Path(x[2]),
    help="local dsgrid registry data path.",
)
@click.option(
    "--url",
    default="http://localhost:8529",
    show_default=True,
    envvar="DSGRID_REGISTRY_DATABASE",
    help="dsgrid registry database URL. Override with the environment variable DSGRID_REGISTRY_DATABASE",
)
@click.option(
    "-f", "--force", is_flag=True, default=False, help="Delete registry_path if it already exists."
)
def create_registry(db_name, url, data_path, force):
    """Create a new registry."""
    if data_path.exists():
        if force:
            shutil.rmtree(data_path)
        else:
            print(f"{data_path} already exists. Set --force to overwrite.", file=sys.stderr)
            sys.exit(1)

    conn = DatabaseConnection.from_url(url, database=db_name)
    RegistryManager.create(conn, data_path)


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


@click.command()
@click.option(
    "--url",
    default="http://localhost:8529",
    show_default=True,
    envvar="DSGRID_REGISTRY_DATABASE",
    help="dsgrid registry database URL. Override with the environment variable DSGRID_REGISTRY_DATABASE",
)
@click.option(
    "--src-db-name",
    required=True,
    help="Source dsgrid registry database name.",
)
@click.option(
    "--dst-db-name",
    default="dsgrid",
    required=True,
    help="Destination dsgrid registry database name.",
)
@click.argument("dst_data_path", type=click.Path(exists=False), callback=lambda *x: Path(x[2]))
@click.argument("config_file", type=click.Path(exists=True), callback=lambda *x: Path(x[2]))
@click.option(
    "-m",
    "--mode",
    default="data-symlinks",
    type=click.Choice(["copy", "data-symlinks", "rsync"]),
    show_default=True,
    help="Controls whether to copy all data, make symlinks to data files, or sync data with the "
    "rsync utility (not available on Windows).",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite dst_registry_path if it already exists. Does not apply if using rsync.",
)
def make_filtered_registry(
    url: str,
    src_db_name,
    dst_db_name,
    dst_data_path: Path,
    config_file: Path,
    mode,
    force,
):
    """Make a filtered registry for testing purposes."""
    simple_model = RegistrySimpleModel(**load_data(config_file))
    src_conn = DatabaseConnection.from_url(url, database=src_db_name)
    dst_conn = DatabaseConnection.from_url(url, database=dst_db_name)
    RegistryManager.copy(
        src_conn,
        dst_conn,
        dst_data_path,
        mode=mode,
        force=force,
    )
    mgr = FilterRegistryManager.load(dst_conn, offline_mode=True, use_remote_data=False)
    mgr.filter(simple_model=simple_model)


cli.add_command(registry)
cli.add_command(create_registry)
cli.add_command(make_filtered_registry)

registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(projects)
registry.add_command(datasets)

dimensions.add_command(remove_dimension)
dimension_mappings.add_command(remove_dimension_mapping)
projects.add_command(remove_project)
datasets.add_command(remove_dataset)
