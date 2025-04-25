"""Main CLI command for dsgrid."""

import logging
import shutil
import sys
from pathlib import Path

import rich_click as click

from dsgrid.cli.common import get_value_from_context
from dsgrid.common import LOCAL_REGISTRY, REMOTE_REGISTRY
from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.loggers import setup_logging, check_log_file_size
from dsgrid.registry.common import DatabaseConnection, DatasetRegistryStatus, VersionUpdateType
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.filter_registry_manager import FilterRegistryManager
from dsgrid.utils.files import load_data


logger = logging.getLogger(__name__)
_config = DsgridRuntimeConfig.load()


"""
Click Group Definitions
"""


@click.group()
# Server-related options are commented-out because the registry is currently only
# supported in SQLite. If/when we add postgres support, these can be added back.
# @click.option(
#    "-U",
#    "--username",
#    default=_config.database_user,
#    show_default=True,
#    help="dsgrid registry user name",
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
    "--url",
    default=_config.database_url,
    show_default=True,
    envvar="DSGRID_REGISTRY_DATABASE_URL",
    help="dsgrid registry database URL. Override with the environment variable "
    "DSGRID_REGISTRY_DATABASE_URL",
)
@click.option("-l", "--log-file", default="dsgrid_admin.log", type=str, help="Log to this file.")
@click.option(
    "-n", "--no-prompts", default=False, is_flag=True, show_default=True, help="Do not prompt."
)
@click.option(
    "--offline/--online",
    is_flag=True,
    default=_config.offline,
    show_default=True,
    help="run in registry commands in offline mode. WARNING: any commands you perform in offline "
    "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
    "commands will not be officially synced with the remote registry",
)
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
# def cli(url, username, password, log_file, no_prompts, offline, verbose):
def cli(url, log_file, no_prompts, offline, verbose):
    """dsgrid-admin commands"""
    path = Path(log_file)
    level = logging.DEBUG if verbose else logging.INFO
    check_log_file_size(path, no_prompts=no_prompts)
    setup_logging("dsgrid", path, console_level=level, file_level=level, mode="a")


@click.group()
@click.option(
    "--remote-path",
    default=REMOTE_REGISTRY,
    show_default=True,
    help="path to dsgrid remote registry",
)
@click.pass_context
def registry(ctx, remote_path):
    """Manage a registry."""
    no_prompts = ctx.parent.params["no_prompts"]
    if "--help" in sys.argv:
        ctx.obj = None
    else:
        conn = DatabaseConnection(
            url=get_value_from_context(ctx, "url"),
            # username=get_value_from_context(ctx, "username"),
            # password=get_value_from_context(ctx, "password"),
        )
        ctx.obj = RegistryManager.load(
            conn,
            remote_path,
            offline_mode=get_value_from_context(ctx, "offline"),
            no_prompts=no_prompts,
        )


@click.group()
@click.pass_obj
def dimensions(registry_manager: RegistryManager):
    """Dimension subcommands"""


@click.group()
@click.pass_obj
def dimension_mappings(registry_manager: RegistryManager):
    """Dimension mapping subcommands"""


@click.group()
@click.pass_obj
def projects(registry_manager: RegistryManager):
    """Project subcommands"""


@click.group()
@click.pass_obj
def datasets(registry_manager: RegistryManager):
    """Dataset subcommands"""


"""
Registry Commands
"""

_create_epilog = """
Examples:\n
$ dsgrid-admin registry create sqlite:////projects/dsgrid/my_project/registry.db -p /projects/dsgrid/my_project/registry-data\n
"""


@click.command(epilog=_create_epilog)
@click.argument("url")
@click.option(
    "-p",
    "--data-path",
    default=LOCAL_REGISTRY,
    show_default=True,
    callback=lambda *x: Path(x[2]),
    help="Local dsgrid registry data path. Must not contain the registry file listed in URL.",
)
@click.option(
    "-f",
    "--overwrite",
    "--force",
    is_flag=True,
    default=False,
    help="Delete registry_path and the database if they already exist.",
)
@click.pass_context
def create_registry(ctx, url, data_path, overwrite):
    """Create a new registry."""
    if data_path.exists():
        if overwrite:
            shutil.rmtree(data_path)
        else:
            print(f"{data_path} already exists. Set --overwrite to overwrite.", file=sys.stderr)
            sys.exit(1)

    conn = DatabaseConnection(
        url=url,  # This may change if/when we support a server database.
        # url=get_value_from_context(ctx, "url"),
        # username=get_value_from_context(ctx, "username"),
        # password=get_value_from_context(ctx, "password"),
    )
    RegistryManager.create(conn, data_path, overwrite=overwrite)


"""
Dimension Commands
"""


@click.command(name="remove")
@click.argument("dimension-id")
@click.pass_obj
def remove_dimension(registry_manager: RegistryManager, dimension_id: str):
    """Remove a dimension from the dsgrid repository."""
    registry_manager.dimension_manager.remove(dimension_id)


"""
Dimension Mapping Commands
"""


@click.command(name="remove")
@click.argument("dimension-mapping-id")
@click.pass_obj
def remove_dimension_mapping(registry_manager: RegistryManager, dimension_mapping_id: str):
    """Remove a dimension mapping from the dsgrid repository."""
    registry_manager.dimension_mapping_manager.remove(dimension_mapping_id)


"""
Project Commands
"""


@click.command(name="remove")
@click.argument("project-id")
@click.pass_obj
def remove_project(registry_manager: RegistryManager, project_id: str):
    """Remove a project from the dsgrid repository."""
    registry_manager.project_manager.remove(project_id)


"""
Dataset Commands
"""


@click.command(name="remove")
@click.argument("dataset-ids", nargs=-1)
@click.pass_obj
def remove_datasets(registry_manager: RegistryManager, dataset_ids: list[str]):
    """Remove one or more datasets from the dsgrid repository."""
    dataset_mgr = registry_manager.dataset_manager
    project_mgr = registry_manager.project_manager

    # Ensure that all dataset IDs are valid before removing any of them.
    for dataset_id in dataset_ids:
        dataset_mgr.get_by_id(dataset_id)

    for dataset_id in dataset_ids:
        registry_manager.dataset_manager.remove(dataset_id)

    dataset_ids_set = set(dataset_ids)
    for project_id in project_mgr.list_ids():
        config = project_mgr.get_by_id(project_id)
        removed_dataset_ids = []
        for dataset in config.iter_datasets():
            if (
                dataset.dataset_id in dataset_ids_set
                and dataset.status == DatasetRegistryStatus.REGISTERED
            ):
                dataset.status = DatasetRegistryStatus.UNREGISTERED
                dataset.mapping_references.clear()
                removed_dataset_ids.append(dataset.dataset_id)
        if removed_dataset_ids:
            ids = ", ".join(removed_dataset_ids)
            msg = (
                f"Set status for datasets {ids} to unregistered in project {project_id} "
                "after removal."
            )
            project_mgr.update(config, VersionUpdateType.MAJOR, msg)


@click.command()
@click.option(
    "--src-database-url",
    required=True,
    help="Source dsgrid registry database URL.",
)
@click.option(
    "--dst-database-url",
    default="dsgrid",
    required=True,
    help="Destination dsgrid registry database URL.",
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
    "--overwrite",
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Overwrite dst_registry_path if it already exists. Does not apply if using rsync.",
)
@click.pass_context
def make_filtered_registry(
    ctx,
    src_database_url,
    dst_database_url,
    dst_data_path: Path,
    config_file: Path,
    mode,
    overwrite,
):
    """Make a filtered registry for testing purposes."""
    simple_model = RegistrySimpleModel(**load_data(config_file))
    # username = get_value_from_context(ctx, "username")
    # password = get_value_from_context(ctx, "password")
    src_conn = DatabaseConnection(
        url=src_database_url,
        # username=username,
        # password=password,
    )
    dst_conn = DatabaseConnection(
        url=dst_database_url,
        # username=username,
        # password=password,
    )
    RegistryManager.copy(
        src_conn,
        dst_conn,
        dst_data_path,
        mode=mode,
        force=overwrite,
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
datasets.add_command(remove_datasets)
