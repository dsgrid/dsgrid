"""Manages a dsgrid registry."""

import getpass
import logging
import sys
from pathlib import Path

import rich_click as click
from rich import print
from semver import VersionInfo

from chronify.utils.path_utils import check_overwrite

from dsgrid.cli.common import (
    get_value_from_context,
    handle_dsgrid_exception,
    path_callback,
)
from dsgrid.common import LOCAL_REGISTRY, REMOTE_REGISTRY
from dsgrid.config.dataset_config import DataSchemaType
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.config.common import SUPPORTED_METRIC_TYPES
from dsgrid.config.project_config import ProjectConfig
from dsgrid.registry.bulk_register import bulk_register
from dsgrid.registry.common import (
    DatabaseConnection,
    DatasetRegistryStatus,
    DataStoreType,
    VersionUpdateType,
)
from dsgrid.registry.dataset_config_generator import generate_config_from_dataset
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.registry.project_config_generator import generate_project_config
from dsgrid.utils.filters import ACCEPTED_OPS


logger = logging.getLogger(__name__)


def _version_info_callback(*args) -> VersionInfo | None:
    val = args[2]
    if val is None:
        return val
    return VersionInfo.parse(val)


def _version_info_required_callback(*args) -> VersionInfo:
    val = args[2]
    return VersionInfo.parse(val)


def _version_update_callback(*args) -> VersionUpdateType:
    val = args[2]
    return VersionUpdateType(val)


"""
Click Group Definitions
"""


@click.group()
# @click.option(
#     "--remote-path",
#     default=REMOTE_REGISTRY,
#     show_default=True,
#     help="path to dsgrid remote registry",
# )
@click.pass_context
def registry(ctx):
    """Manage a registry."""
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
        # database=get_value_from_context(ctx, "database_name"),
        # username=get_value_from_context(ctx, "username"),
        # password=get_value_from_context(ctx, "password"),
    )
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    no_prompts = ctx.parent.params["no_prompts"]
    offline = get_value_from_context(ctx, "offline")
    if "--help" in sys.argv:
        ctx.obj = None
    else:
        ctx.obj = RegistryManager.load(
            conn,
            REMOTE_REGISTRY,
            offline_mode=offline,
            no_prompts=no_prompts,
            scratch_dir=scratch_dir,
        )


@registry.result_callback()
@click.pass_context
def cleanup_registry_manager(ctx, result, **kwargs):
    """Cleanup the registry manager after the command finishes."""
    if ctx.obj is not None:
        ctx.obj.dispose()


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


# TODO: Support registry file reads without syncing using something like sfs3
@click.command(name="list")
@click.pass_obj
def list_(registry_manager):
    """List the contents of a registry."""
    print(f"Registry: {registry_manager.path}")
    registry_manager.show()


_create_epilog = """
Examples:\n
$ dsgrid registry create sqlite:////projects/dsgrid/my_project/registry.db -p /projects/dsgrid/my_project/registry-data\n
"""


@click.command(name="create", epilog=_create_epilog)
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
@click.option(
    "-t",
    "--data-store-type",
    type=click.Choice([x.value for x in DataStoreType]),
    default=DataStoreType.FILESYSTEM.value,
    show_default=True,
    help="Type of store to use for the registry data.",
    callback=lambda *x: DataStoreType(x[2]),
)
def create_registry(url: str, data_path: Path, overwrite: bool, data_store_type: DataStoreType):
    """Create a new registry."""
    check_overwrite(data_path, overwrite)
    conn = DatabaseConnection(url=url)
    RegistryManager.create(conn, data_path, overwrite=overwrite, data_store_type=data_store_type)


"""
Dimension Commands
"""


_list_dimensions_epilog = """
Examples:\n
$ dsgrid registry dimensions list\n
$ dsgrid registry dimensions list -f "Type == sector"\n
$ dsgrid registry dimensions list -f "Submitter == username"\n
"""


@click.command(name="list", epilog=_list_dimensions_epilog)
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    Filter table with a case-insensitive expression in the format 'column operation value',
    accepts multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    """,
)
@click.pass_obj
def list_dimensions(registry_manager, filter):
    """List the registered dimensions."""
    registry_manager.dimension_manager.show(filters=filter)


_register_dimensions_epilog = """
Examples:\n
$ dsgrid registry dimensions register -l "Register dimensions for my-project" dimensions.json5\n
"""


@click.command(name="register", epilog=_register_dimensions_epilog)
@click.argument("dimension-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
@click.pass_context
def register_dimensions(
    ctx,
    registry_manager: RegistryManager,
    dimension_config_file: Path,
    log_message: str,
):
    """Register new dimensions with the dsgrid repository. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dimension.html#dsgrid.config.dimensions.DimensionsConfigModel
    """
    manager = registry_manager.dimension_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx, manager.register, dimension_config_file, submitter, log_message
    )
    if res[1] != 0:
        ctx.exit(res[1])


_dump_dimension_epilog = """
Examples:\n
$ dsgrid registry dimensions dump 8c575746-18fa-4b65-bf1f-516079d634a5\n
"""


@click.command(name="dump", epilog=_dump_dimension_epilog)
@click.argument("dimension-id")
@click.option(
    "-v",
    "--version",
    callback=_version_info_callback,
    help="Version to dump; defaults to latest",
)
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True),
    help="Directory in which to create config and data files",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_obj
@click.pass_context
def dump_dimension(ctx, registry_manager, dimension_id, version, directory, force):
    """Dump a dimension config file (and any related data) from the registry."""
    manager = registry_manager.dimension_manager
    res = handle_dsgrid_exception(
        ctx, manager.dump, dimension_id, Path(directory), version=version, force=force
    )
    if res[1] != 0:
        ctx.exit(res[1])


_show_dimension_epilog = """
Examples:\n
$ dsgrid registry dimensions show 8c575746-18fa-4b65-bf1f-516079d634a5
"""


@click.command(name="show", epilog=_show_dimension_epilog)
@click.argument("dimension-id", type=str)
@click.option(
    "-v",
    "--version",
    default="1.0.0",
    show_default=True,
    callback=_version_info_required_callback,
    help="Version to show.",
)
@click.pass_obj
@click.pass_context
def show_dimension(
    ctx,
    registry_manager: RegistryManager,
    dimension_id: str,
    version: str,
):
    """Show an existing dimension in the registry."""
    manager = registry_manager.dimension_manager
    res = handle_dsgrid_exception(
        ctx,
        manager.get_by_id,
        dimension_id,
        version,
    )
    if res[1] != 0:
        ctx.exit(res[1])
    dim = res[0]
    print(
        f"""id={dim.model.dimension_id}
type={dim.model.dimension_type.value}
name={dim.model.name}
description={dim.model.description}
"""
    )
    if dim.model.dimension_type != DimensionType.TIME:
        records = dim.get_records_dataframe()
        records.show(n=4000)


_update_dimension_epilog = """
Examples:\n
$ dsgrid registry dimensions update -d 8c575746-18fa-4b65-bf1f-516079d634a5 -l "Update county dimension" -u major -v 1.0.0 dimension.json5\n
"""


@click.command(name="update", epilog=_update_dimension_epilog)
@click.argument("dimension-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-d",
    "--dimension-id",
    required=True,
    type=str,
    help="dimension ID",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.option(
    "-t",
    "--update-type",
    required=True,
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
@click.option(
    "-v",
    "--version",
    required=True,
    callback=_version_info_required_callback,
    help="Version to update; must be the current version.",
)
@click.pass_obj
@click.pass_context
def update_dimension(
    ctx,
    registry_manager: RegistryManager,
    dimension_config_file: Path,
    dimension_id: str,
    log_message: str,
    update_type: VersionUpdateType,
    version: str,
):
    """Update an existing dimension in the registry."""
    manager = registry_manager.dimension_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx,
        manager.update_from_file,
        dimension_config_file,
        dimension_id,
        submitter,
        update_type,
        log_message,
        version,
    )
    if res[1] != 0:
        ctx.exit(res[1])


@click.command(name="remove")
@click.argument("dimension-id")
@click.pass_obj
def remove_dimension(registry_manager: RegistryManager, dimension_id: str):
    """Remove a dimension from the dsgrid repository."""
    registry_manager.dimension_manager.remove(dimension_id)


"""
Dimension Mapping Commands
"""


_list_dimension_mappings_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings list\n
$ dsgrid registry dimension-mappings list -f "Type [From, To] contains geography" -f "Submitter == username"\n
"""


@click.command(name="list", epilog=_list_dimension_mappings_epilog)
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    Filter table with a case-insensitive expression in the format 'column operation value',
    accepts multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    """,
)
@click.pass_obj
def list_dimension_mappings(registry_manager, filter):
    """List the registered dimension mappings."""
    registry_manager.dimension_mapping_manager.show(filters=filter)


_register_dimension_mappings_epilog = """
Examples:\\
$ dsgrid registry dimension-mappings register -l "Register dimension mappings for my-project" dimension_mappings.json5\n
"""


@click.command(name="register", epilog=_register_dimension_mappings_epilog)
@click.argument(
    "dimension-mapping-config-file",
    type=click.Path(exists=True),
    callback=path_callback,
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
@click.pass_context
def register_dimension_mappings(
    ctx,
    registry_manager: RegistryManager,
    dimension_mapping_config_file: Path,
    log_message: str,
):
    """Register new dimension mappings with the dsgrid repository. The contents of the JSON/JSON5
    file must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mappings_config.DimensionMappingsConfigModel
    """
    submitter = getpass.getuser()
    manager = registry_manager.dimension_mapping_manager
    res = handle_dsgrid_exception(
        ctx, manager.register, dimension_mapping_config_file, submitter, log_message
    )
    if res[1] != 0:
        ctx.exit(res[1])


_dump_dimension_mapping_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings dump 8c575746-18fa-4b65-bf1f-516079d634a5\n
"""


@click.command(name="dump", epilog=_dump_dimension_mapping_epilog)
@click.argument("dimension-mapping-id")
@click.option(
    "-v",
    "--version",
    callback=_version_info_callback,
    help="Version to dump; defaults to latest",
)
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True),
    help="Directory in which to create config and data files",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_obj
@click.pass_context
def dump_dimension_mapping(
    ctx,
    registry_manager: RegistryManager,
    dimension_mapping_id: str,
    version: str,
    directory: Path,
    force: bool,
):
    """Dump a dimension mapping config file (and any related data) from the registry."""
    manager = registry_manager.dimension_mapping_manager
    res = handle_dsgrid_exception(
        ctx,
        manager.dump,
        dimension_mapping_id,
        Path(directory),
        version=version,
        force=force,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_show_dimension_mapping_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings show 8c575746-18fa-4b65-bf1f-516079d634a5
"""


@click.command(name="show", epilog=_show_dimension_mapping_epilog)
@click.argument("mapping-id", type=str)
@click.option(
    "-v",
    "--version",
    default="1.0.0",
    show_default=True,
    callback=_version_info_required_callback,
    help="Version to show.",
)
@click.pass_obj
@click.pass_context
def show_dimension_mapping(
    ctx,
    registry_manager: RegistryManager,
    mapping_id: str,
    version: str,
):
    """Show an existing dimension mapping in the registry."""
    manager = registry_manager.dimension_mapping_manager
    res = handle_dsgrid_exception(
        ctx,
        manager.get_by_id,
        mapping_id,
        version,
    )
    if res[1] != 0:
        ctx.exit(res[1])
    mapping = res[0]
    from_dim = registry_manager.dimension_manager.get_by_id(
        mapping.model.from_dimension.dimension_id
    )
    to_dim = registry_manager.dimension_manager.get_by_id(mapping.model.to_dimension.dimension_id)
    print(
        f"""
type={mapping.model.from_dimension.dimension_type}
from_id={mapping.model.from_dimension.dimension_id}
from_name={from_dim.model.name}
from_description={from_dim.model.description}
to_id={mapping.model.to_dimension.dimension_id}
to_name={to_dim.model.name}
to_description={to_dim.model.description}
"""
    )
    records = mapping.get_records_dataframe()
    records.show(n=4000)


_update_dimension_mapping_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings update \\ \n
    -d 8c575746-18fa-4b65-bf1f-516079d634a5 \\ \n
    -l "Swap out the state to county mapping for my-dataset to that-project" \\ \n
    -u major \\ \n
    -v 1.0.0 dimension_mappings.json5"
"""


@click.command(name="update", epilog=_update_dimension_mapping_epilog)
@click.argument(
    "dimension-mapping-config-file",
    type=click.Path(exists=True),
    callback=path_callback,
)
@click.option(
    "-d",
    "--dimension-mapping-id",
    required=True,
    type=str,
    help="dimension mapping ID",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.option(
    "-t",
    "--update-type",
    required=True,
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
@click.option(
    "-v",
    "--version",
    required=True,
    callback=_version_info_required_callback,
    help="Version to update; must be the current version.",
)
@click.pass_obj
def update_dimension_mapping(
    registry_manager: RegistryManager,
    dimension_mapping_config_file: Path,
    dimension_mapping_id: str,
    log_message: str,
    update_type: VersionUpdateType,
    version: str,
):
    """Update an existing dimension mapping registry. The contents of the JSON/JSON5 file must
    match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.mapping_tables.MappingTableModel
    """
    manager = registry_manager.dimension_mapping_manager
    submitter = getpass.getuser()
    manager.update_from_file(
        dimension_mapping_config_file,
        dimension_mapping_id,
        submitter,
        update_type,
        log_message,
        version,
    )


@click.command(name="remove")
@click.argument("dimension-mapping-id")
@click.pass_obj
def remove_dimension_mapping(registry_manager: RegistryManager, dimension_mapping_id: str):
    """Remove a dimension mapping from the dsgrid repository."""
    registry_manager.dimension_mapping_manager.remove(dimension_mapping_id)


"""
Project Commands
"""


_list_projects_epilog = """
Examples:\n
$ dsgrid registry projects list\n
$ dsgrid registry projects list -f "ID contains efs"\n
"""


@click.command(name="list", epilog=_list_projects_epilog)
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    Filter table with a case-insensitive expression in the format 'column operation value',
    accepts multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    """,
)
@click.pass_obj
@click.pass_context
def list_projects(ctx, registry_manager, filter):
    """List the registered projects."""
    res = handle_dsgrid_exception(ctx, registry_manager.project_manager.show, filters=filter)
    if res[1] != 0:
        ctx.exit(res[1])


_register_project_epilog = """
Examples:\n
$ dsgrid registry projects register -l "Register project my-project" project.json5\n
"""


@click.command(name="register", epilog=_register_project_epilog)
@click.argument("project-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
@click.pass_context
def register_project(
    ctx,
    registry_manager,
    project_config_file,
    log_message,
):
    """Register a new project with the dsgrid repository. The contents of the JSON/JSON5 file must
    match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.project_config.ProjectConfigModel
    """
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx,
        registry_manager.project_manager.register,
        project_config_file,
        submitter,
        log_message,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_submit_dataset_epilog = """
Examples:\n
$ dsgrid registry projects submit-dataset \\ \n
    -p my-project-id \\ \n
    -d my-dataset-id \\ \n
    -m dimension_mappings.json5 \\ \n
    -l "Submit dataset my-dataset to project my-project."\n
"""


@click.command(epilog=_submit_dataset_epilog)
@click.option(
    "-d",
    "--dataset-id",
    required=True,
    type=str,
    help="dataset identifier",
)
@click.option(
    "-p",
    "--project-id",
    required=True,
    type=str,
    help="project identifier",
)
@click.option(
    "-m",
    "--dimension-mapping-file",
    type=click.Path(exists=True),
    show_default=True,
    help="Dimension mapping file. Must match the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mappings_config.DimensionMappingsConfigModel",
    callback=path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered. Must mach the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mapping_base.DimensionMappingReferenceListModel",
    callback=path_callback,
)
@click.option(
    "-a",
    "--autogen-reverse-supplemental-mappings",
    type=click.Choice([x.value for x in DimensionType]),
    callback=lambda _, __, x: [DimensionType(y) for y in x],
    multiple=True,
    help="For any dimension listed here, if the dataset's dimension is a project's supplemental "
    "dimension and no mapping is provided, create a reverse mapping from that supplemental "
    "dimension.",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.pass_obj
def submit_dataset(
    registry_manager: RegistryManager,
    dataset_id: str,
    project_id: str,
    dimension_mapping_file: Path,
    dimension_mapping_references_file: Path,
    autogen_reverse_supplemental_mappings: list[DimensionType],
    log_message: str,
):
    """Submit a dataset to a dsgrid project."""
    submitter = getpass.getuser()
    manager = registry_manager.project_manager
    manager.submit_dataset(
        project_id,
        dataset_id,
        submitter,
        log_message,
        dimension_mapping_file=dimension_mapping_file,
        dimension_mapping_references_file=dimension_mapping_references_file,
        autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
    )


_register_and_submit_dataset_epilog = """
Examples:\n
$ dsgrid registry projects register-and-submit-dataset \\ \n
    -c dataset.json5 \\ \n
    -p my-project-id \\ \n
    -m dimension_mappings.json5 \\ \n
    -l "Register and submit dataset my-dataset to project my-project." \n

$ dsgrid registry projects register-and-submit-dataset \\ \n
    -c dataset.json5 \\ \n
    --data-base-dir /path/to/data \\ \n
    -p my-project-id \\ \n
    -m dimension_mappings.json5 \\ \n
    -l "Register and submit dataset my-dataset to project my-project." \n
"""


@click.command(epilog=_register_and_submit_dataset_epilog)
@click.option(
    "-c",
    "--dataset-config-file",
    required=True,
    type=click.Path(exists=True),
    callback=path_callback,
    help="Dataset config file (must include table_schema with data_file paths)",
)
@click.option(
    "-m",
    "--dimension-mapping-file",
    type=click.Path(exists=True),
    help="Dimension mapping file. Must match the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mappings_config.DimensionMappingsConfigModel",
    callback=path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered. Must mach the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mapping_base.DimensionMappingReferenceListModel",
    callback=path_callback,
)
@click.option(
    "-a",
    "--autogen-reverse-supplemental-mappings",
    type=click.Choice([x.value for x in DimensionType]),
    callback=lambda _, __, x: [DimensionType(y) for y in x],
    multiple=True,
    help="For any dimension listed here, if the dataset's dimension is a project's supplemental "
    "dimension and no mapping is provided, create a reverse mapping from that supplemental "
    "dimension.",
)
@click.option(
    "-p",
    "--project-id",
    required=True,
    type=str,
    help="project identifier",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.option(
    "-D",
    "--data-base-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for data files. If set and data file paths are relative, "
    "prepend them with this path.",
)
@click.option(
    "-M",
    "--missing-associations-base-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for missing associations files. If set and missing associations "
    "paths are relative, prepend them with this path.",
)
@click.pass_obj
@click.pass_context
def register_and_submit_dataset(
    ctx,
    registry_manager,
    dataset_config_file,
    dimension_mapping_file,
    dimension_mapping_references_file,
    autogen_reverse_supplemental_mappings,
    project_id,
    log_message,
    data_base_dir,
    missing_associations_base_dir,
):
    """Register a dataset and then submit it to a dsgrid project.

    The dataset config file must include a table_schema with data_file and optional
    lookup_data_file paths pointing to the dataset files.
    """
    submitter = getpass.getuser()
    manager = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        manager.register_and_submit_dataset,
        dataset_config_file,
        project_id,
        submitter,
        log_message,
        dimension_mapping_file=dimension_mapping_file,
        dimension_mapping_references_file=dimension_mapping_references_file,
        autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
        data_base_dir=data_base_dir,
        missing_associations_base_dir=missing_associations_base_dir,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_dump_project_epilog = """
Examples:\n
$ dsgrid registry projects dump my-project-id\n
"""


@click.command(name="dump", epilog=_dump_project_epilog)
@click.argument("project-id")
@click.option(
    "-v",
    "--version",
    callback=_version_info_callback,
    help="Version to dump; defaults to latest",
)
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True),
    help="Directory in which to create the config file",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_obj
@click.pass_context
def dump_project(
    ctx,
    registry_manager: RegistryManager,
    project_id: str,
    version: str,
    directory: Path,
    force: bool,
):
    """Dump a project config file from the registry."""
    manager = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx, manager.dump, project_id, directory, version=version, force=force
    )
    if res[1] != 0:
        ctx.exit(res[1])


_update_project_epilog = """
Examples: \n
$ dsgrid registry projects update \\ \n
    -p my-project-id \\ \n
    -u patch \\ \n
    -v 1.5.0 \\ \n
    -l "Update description for project my-project-id." \n
"""


@click.command(name="update", epilog=_update_project_epilog)
@click.argument("project-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-p",
    "--project-id",
    required=True,
    type=str,
    help="project ID",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.option(
    "-t",
    "--update-type",
    required=True,
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
@click.option(
    "-v",
    "--version",
    required=True,
    callback=_version_info_required_callback,
    help="Version to update; must be the current version.",
)
@click.pass_obj
@click.pass_context
def update_project(
    ctx,
    registry_manager: RegistryManager,
    project_config_file: Path,
    project_id: str,
    log_message: str,
    update_type: VersionUpdateType,
    version: str,
):
    """Update an existing project in the registry."""
    manager = registry_manager.project_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx,
        manager.update_from_file,
        project_config_file,
        project_id,
        submitter,
        update_type,
        log_message,
        version,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_register_subset_dimensions_epilog = """
Examples:\n
$ dsgrid registry projects register-subset-dimensions \\ \n
    -l "Register subset dimensions for end uses by fuel type for my-project-id." \\ \n
    my-project-id \\ \n
    subset_dimensions.json5 \n
"""


@click.command(epilog=_register_subset_dimensions_epilog)
@click.pass_obj
@click.pass_context
@click.argument("project_id")
@click.argument("filename", callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for this addition.",
)
@click.option(
    "-t",
    "--update-type",
    default="patch",
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
def register_subset_dimensions(
    ctx,
    registry_manager: RegistryManager,
    project_id: str,
    filename: Path,
    log_message: str,
    update_type: VersionUpdateType,
):
    """Register new subset dimensions with a project. The contents of the JSON/JSON5 file must
    match the data model defined by this documentation:

    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.project_config.SubsetDimensionGroupListModel
    """

    submitter = getpass.getuser()
    project_mgr = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        project_mgr.register_subset_dimensions,
        project_id,
        filename,
        submitter,
        log_message,
        update_type,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_register_supplemental_dimensions_epilog = """
Examples:\n
$ dsgrid registry projects register-supplemental-dimensions \\ \n
    -l "Register states supplemental dimension for my-project-id" \\ \n
    my-project-id \\ \n
    supplemental_dimensions.json5\n
"""


@click.command(epilog=_register_supplemental_dimensions_epilog)
@click.pass_obj
@click.pass_context
@click.argument("project_id")
@click.argument("filename", callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for this addition.",
)
@click.option(
    "-t",
    "--update-type",
    default="patch",
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
def register_supplemental_dimensions(
    ctx, registry_manager, project_id, filename: Path, log_message, update_type
):
    """Register new supplemental dimensions with a project. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.supplemental_dimension.SupplementalDimensionsListModel
    """

    submitter = getpass.getuser()
    project_mgr = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        project_mgr.register_supplemental_dimensions,
        project_id,
        filename,
        submitter,
        log_message,
        update_type,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_add_dataset_requirements_epilog = """
Examples:\n
$ dsgrid registry projects add-dataset-requirements \\ \n
    -l "Add requirements for dataset my-dataset-id to my-project-id." \\ \n
    my-project-id \\ \n
    dataset_requirements.json5\n
"""


@click.command(epilog=_add_dataset_requirements_epilog)
@click.pass_obj
@click.pass_context
@click.argument("project_id")
@click.argument("filename", callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for the new datasets.",
)
@click.option(
    "-t",
    "--update-type",
    default="patch",
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
def add_dataset_requirements(
    ctx, registry_manager, project_id, filename: Path, log_message, update_type
):
    """Add requirements for one or more datasets to a project. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.input_dataset_requirements.InputDatasetListModel
    """
    submitter = getpass.getuser()
    project_mgr = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        project_mgr.add_dataset_requirements,
        project_id,
        filename,
        submitter,
        log_message,
        update_type,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_replace_dataset_dimension_requirements_epilog = """
Examples:\n
$ dsgrid registry projects replace-dataset-dimension-requirements \\ \n
    -l "Replace dimension requirements for dataset my-dataset-id in my-project-id." \\ \n
    project_id \\ \n
    dataset_dimension_requirements.json5\n
"""


@click.command(epilog=_replace_dataset_dimension_requirements_epilog)
@click.pass_obj
@click.pass_context
@click.argument("project_id")
@click.argument("filename", callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for the new requirements.",
)
@click.option(
    "-t",
    "--update-type",
    default="major",
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
def replace_dataset_dimension_requirements(
    ctx, registry_manager, project_id, filename: Path, log_message, update_type
):
    """Replace dimension requirements for one or more datasets in a project. The contents of the
    JSON/JSON5 file must match the data model defined by this documentation:

    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.input_dataset_requirements.InputDatasetDimensionRequirementsListModel
    """
    submitter = getpass.getuser()
    project_mgr = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        project_mgr.replace_dataset_dimension_requirements,
        project_id,
        filename,
        submitter,
        log_message,
        update_type,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_list_project_dimension_names_epilog = """
Examples:\n
$ dsgrid registry projects list-dimension-names my_project_id\n
$ dsgrid registry projects list-dimension-names --exclude-subset my_project_id\n
$ dsgrid registry projects list-dimension-names --exclude-supplemental my_project_id\n
"""


@click.command(name="list-dimension-names", epilog=_list_project_dimension_names_epilog)
@click.argument("project-id")
@click.option(
    "-b",
    "--exclude-base",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude base dimension names.",
)
@click.option(
    "-S",
    "--exclude-subset",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude subset dimension names.",
)
@click.option(
    "-s",
    "--exclude-supplemental",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude supplemental dimension names.",
)
@click.pass_obj
@click.pass_context
def list_project_dimension_names(
    ctx,
    registry_manager: RegistryManager,
    project_id,
    exclude_base,
    exclude_subset,
    exclude_supplemental,
):
    """List the project's dimension names."""
    if exclude_base and exclude_subset and exclude_supplemental:
        print(
            "exclude_base, exclude_subset, and exclude_supplemental cannot all be set",
            file=sys.stderr,
        )
        ctx.exit(1)

    manager = registry_manager.project_manager
    res = handle_dsgrid_exception(ctx, manager.get_by_id, project_id)
    if res[1] != 0:
        ctx.exit(res[1])

    project_config = res[0]
    assert isinstance(project_config, ProjectConfig)
    base = None if exclude_base else project_config.get_dimension_type_to_base_name_mapping()
    sub = None if exclude_subset else project_config.get_subset_dimension_to_name_mapping()
    supp = (
        None
        if exclude_supplemental
        else project_config.get_supplemental_dimension_to_name_mapping()
    )

    dimensions = sorted(DimensionType, key=lambda x: x.value)
    print("Dimension names:")
    for dim_type in dimensions:
        print(f"  {dim_type.value}:")
        if base:
            base_str = " ".join(base[dim_type])
            print(f"    base: {base_str}")
        if sub:
            print("    subset: " + " ".join(sub[dim_type]))
        if supp:
            print("    supplemental: " + " ".join(supp[dim_type]))


_generate_project_config_epilog = """
Examples:\n
$ dsgrid registry projects generate-config \\ \n
    -o "./my-project-dir" \\ \n
    my-project-id dataset-id1 dataset-id2 dataset-id3\n
"""


@click.command(name="generate-config", epilog=_generate_project_config_epilog)
@click.argument("project_id")
@click.argument("dataset_ids", nargs=-1)
@click.option(
    "-m",
    "--metric-type",
    multiple=True,
    type=click.Choice(sorted(SUPPORTED_METRIC_TYPES)),
    help="Metric types available in the project",
)
@click.option(
    "-n",
    "--name",
    type=str,
    help="Project name, optional",
)
@click.option(
    "-d",
    "--description",
    type=str,
    help="Project description, optional",
)
@click.option(
    "-t",
    "--time-type",
    type=click.Choice([x.value for x in TimeDimensionType]),
    default=TimeDimensionType.DATETIME.value,
    show_default=True,
    help="Type of the time dimension",
    callback=lambda *x: TimeDimensionType(x[2]),
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    default=".",
    show_default=True,
    callback=path_callback,
    help="Path in which to generate project config files.",
)
@click.option(
    "-O",
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_context
def generate_project_config_from_ids(
    ctx: click.Context,
    project_id: str,
    dataset_ids: tuple[str],
    metric_type: tuple[str],
    name: str | None,
    description: str | None,
    time_type: TimeDimensionType,
    output: Path | None,
    overwrite: bool,
):
    """Generate project config files from a project ID and one or more dataset IDs."""
    res = handle_dsgrid_exception(
        ctx,
        generate_project_config,
        project_id,
        dataset_ids,
        metric_type,
        name=name,
        description=description,
        time_type=time_type,
        output_directory=output,
        overwrite=overwrite,
    )
    if res[1] != 0:
        ctx.exit(res[1])


@click.command(name="remove")
@click.argument("project-id")
@click.pass_obj
def remove_project(registry_manager: RegistryManager, project_id: str):
    """Remove a project from the dsgrid repository."""
    registry_manager.project_manager.remove(project_id)


"""
Dataset Commands
"""


_list_datasets_epilog = """
Examples:\n
$ dsgrid registry datasets list\n
$ dsgrid registry datasets list -f "ID contains com" -f "Submitter == username"\n
"""


@click.command(name="list", epilog=_list_datasets_epilog)
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    Filter table with a case-insensitive expression in the format 'column operation value',
    accepts multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    """,
)
@click.pass_obj
def list_datasets(registry_manager, filter):
    """List the registered dimensions."""
    registry_manager.dataset_manager.show(filters=filter)


_register_dataset_epilog = """
Examples:\n
$ dsgrid registry datasets register dataset.json5 -l "Register dataset my-dataset-id."\n
$ dsgrid registry datasets register dataset.json5 --data-base-dir /path/to/data -l "Register dataset my-dataset-id."\n
"""


@click.command(name="register", epilog=_register_dataset_epilog)
@click.argument("dataset-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.option(
    "-D",
    "--data-base-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for data files. If set and data file paths are relative, "
    "prepend them with this path.",
)
@click.option(
    "-M",
    "--missing-associations-base-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for missing associations files. If set and missing associations "
    "paths are relative, prepend them with this path.",
)
@click.pass_obj
@click.pass_context
def register_dataset(
    ctx: click.Context,
    registry_manager: RegistryManager,
    dataset_config_file: Path,
    log_message: str,
    data_base_dir: Path | None,
    missing_associations_base_dir: Path | None,
):
    """Register a new dataset with the registry. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dataset.html#dsgrid.config.dataset_config.DatasetConfigModel

    The config file must include a table_schema with data_file and optional
    lookup_data_file paths pointing to the dataset files.
    """
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx,
        manager.register,
        dataset_config_file,
        submitter,
        log_message,
        data_base_dir=data_base_dir,
        missing_associations_base_dir=missing_associations_base_dir,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_dump_dataset_epilog = """
Examples:\n
$ dsgrid registry datasets dump my-dataset-id\n
"""


@click.command(name="dump", epilog=_dump_dataset_epilog)
@click.argument("dataset-id")
@click.option(
    "-v",
    "--version",
    callback=_version_info_callback,
    help="Version to dump; defaults to latest",
)
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True),
    help="Directory in which to create the config file",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_obj
def dump_dataset(registry_manager, dataset_id, version, directory, force):
    """Dump a dataset config file from the registry."""
    manager = registry_manager.dataset_manager
    manager.dump(dataset_id, directory, version=version, force=force)


_update_dataset_epilog = """
Examples:\n
$ dsgrid registry datasets update \\ \n
    -l "Update the description for dataset my-dataset-id." \\ \n
    -u patch \\ \n
    -v 1.0.0 \\ \n
    dataset.json5\n
"""


@click.command(name="update", epilog=_update_dataset_epilog)
@click.argument("dataset-config-file", type=click.Path(exists=True), callback=path_callback)
@click.option(
    "-d",
    "--dataset-id",
    required=True,
    type=str,
    help="dataset ID",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.option(
    "-t",
    "--update-type",
    required=True,
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=_version_update_callback,
)
@click.option(
    "-v",
    "--version",
    required=True,
    callback=_version_info_required_callback,
    help="Version to update; must be the current version.",
)
@click.pass_obj
@click.pass_context
def update_dataset(
    ctx: click.Context,
    registry_manager: RegistryManager,
    dataset_config_file: Path,
    dataset_id: str,
    log_message: str,
    update_type: VersionUpdateType,
    version: str,
):
    """Update an existing dataset in the registry. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dataset.html#dsgrid.config.dataset_config.DatasetConfigModel

    If the config file includes a UserDatasetSchema with file paths, the data will be
    re-read from those paths. Otherwise, the existing data in the registry is used.
    """
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx,
        manager.update_from_file,
        dataset_config_file,
        dataset_id,
        submitter,
        update_type,
        log_message,
        version,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_generate_dataset_config_from_dataset_epilog = """
Examples:\n
$ dsgrid registry datasets generate-config-from-dataset \\ \n
    -o "./my-dataset-dir" \\ \n
    -P my-project-id \\ \n
    my-dataset-id \\ \n
    /path/to/table.parquet\n
"""


@click.command(name="generate-config", epilog=_generate_dataset_config_from_dataset_epilog)
@click.argument("dataset-id")
@click.argument("dataset-path")
@click.option(
    "-s",
    "--schema-type",
    type=click.Choice([x.value for x in DataSchemaType]),
    default=DataSchemaType.ONE_TABLE.value,
    show_default=True,
    callback=lambda *x: DataSchemaType(x[2]),
)
@click.option(
    "-m",
    "--metric-type",
    type=click.Choice(sorted(SUPPORTED_METRIC_TYPES)),
    default="EnergyEndUse",
    show_default=True,
)
@click.option(
    "-p",
    "--pivoted-dimension-type",
    type=click.Choice([x.value for x in DimensionType if x != DimensionType.TIME]),
    default=None,
    callback=lambda *x: None if x[2] is None else DimensionType(x[2]),
    help="Optional, if one dimension has its records pivoted as columns, its type.",
)
@click.option(
    "-t",
    "--time-type",
    type=click.Choice([x.value for x in TimeDimensionType]),
    default=TimeDimensionType.DATETIME.value,
    show_default=True,
    help="Type of the time dimension",
    callback=lambda *x: TimeDimensionType(x[2]),
)
@click.option(
    "-T",
    "--time-columns",
    multiple=True,
    help="Names of time columns in the table. Required if pivoted_dimension_type is set "
    "and the time column is not 'timestamp'.",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    default=".",
    show_default=True,
    callback=path_callback,
    help="Path in which to create dataset config files.",
)
@click.option(
    "-P",
    "--project-id",
    required=False,
    type=str,
    help="Project ID, optional. If provided, prioritize project base dimensions when "
    "searching for matching dimensions.",
)
@click.option(
    "-n",
    "--no-prompts",
    is_flag=True,
    default=False,
    show_default=True,
    help="Do not prompt for matches. Automatically accept the first one.",
)
@click.option(
    "-O",
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite files if they exist.",
)
@click.pass_obj
@click.pass_context
def generate_dataset_config_from_dataset(
    ctx: click.Context,
    registry_manager: RegistryManager,
    dataset_id: str,
    dataset_path: Path,
    schema_type: DataSchemaType,
    metric_type: str,
    pivoted_dimension_type: DimensionType | None,
    time_type: TimeDimensionType,
    time_columns: tuple[str],
    output: Path | None,
    project_id: str | None,
    no_prompts: bool,
    overwrite: bool,
):
    """Generate dataset config files from a dataset table.

    Fill out the dimension record files based on the unique values in the dataset.

    Look for matches for dimensions in the registry. Prompt the user for confirmation unless
    --no-prompts is set. If --no-prompts is set, the first match is automatically accepted.
    """
    res = handle_dsgrid_exception(
        ctx,
        generate_config_from_dataset,
        registry_manager,
        dataset_id,
        dataset_path,
        schema_type,
        metric_type,
        pivoted_dimension_type=pivoted_dimension_type,
        time_type=time_type,
        time_columns=set(time_columns),
        output_directory=output,
        project_id=project_id,
        no_prompts=no_prompts,
        overwrite=overwrite,
    )
    if res[1] != 0:
        ctx.exit(res[1])


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


_bulk_register_epilog = """
Examples:\n
$ dsgrid registry bulk-register registration.json5
$ dsgrid registry bulk-register registration.json5 -j journal__11f733f6-ac9b-4f70-ad4b-df75b291f150.json5
"""


@click.command(name="bulk-register", epilog=_bulk_register_epilog)
@click.argument("registration_file", type=click.Path(exists=True))
@click.option(
    "-d",
    "--base-data-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for input data. If set, and if the dataset paths are relative, prepend "
    "them with this path.",
)
@click.option(
    "-r",
    "--base-repo-dir",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Base directory for dsgrid project/dataset repository. If set, and if the config file "
    "paths are relative, prepend them with this path.",
)
@click.option(
    "-j",
    "--journal-file",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Journal file created by a previous bulk register operation. If passed, the code will "
    "read it and skip all projects and datasets that were successfully registered. "
    "The file will be updated with IDs that are successfully registered.",
)
@click.pass_obj
@click.pass_context
def bulk_register_cli(
    ctx,
    registry_manager: RegistryManager,
    registration_file: Path,
    base_data_dir: Path | None,
    base_repo_dir: Path | None,
    journal_file: Path | None,
):
    """Bulk register projects, datasets, and their dimensions. If any failure occurs, the code
    records successfully registered project and dataset IDs to a journal file and prints its
    filename to the console. Users can pass that filename with the --journal-file option to
    avoid re-registering those projects and datasets on subsequent attempts.

    The JSON/JSON5 filename must match the data model defined by this documentation:

    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.registration_models.RegistrationModel
    """
    res = handle_dsgrid_exception(
        ctx,
        bulk_register,
        registry_manager,
        registration_file,
        base_data_dir=base_data_dir,
        base_repo_dir=base_repo_dir,
        journal_file=journal_file,
    )
    if res[1] != 0:
        ctx.exit(res[1])


@click.command()
@click.pass_obj
@click.pass_context
@click.option(
    "--project-id",
    "-P",
    type=str,
    help="Sync latest dataset(s) version based on Project ID",
)
@click.option(
    "--dataset-id",
    "-D",
    type=str,
    help="Sync latest dataset version based on Dataset ID",
)
def data_sync(ctx, registry_manager, project_id, dataset_id):
    """Sync the official dsgrid registry data to the local system."""
    no_prompts = ctx.parents[1].params["no_prompts"]
    registry_manager.data_sync(project_id, dataset_id, no_prompts)


dimensions.add_command(list_dimensions)
dimensions.add_command(register_dimensions)
dimensions.add_command(dump_dimension)
dimensions.add_command(show_dimension)
dimensions.add_command(update_dimension)
dimensions.add_command(remove_dimension)

dimension_mappings.add_command(list_dimension_mappings)
dimension_mappings.add_command(register_dimension_mappings)
dimension_mappings.add_command(dump_dimension_mapping)
dimension_mappings.add_command(show_dimension_mapping)
dimension_mappings.add_command(update_dimension_mapping)
dimension_mappings.add_command(remove_dimension_mapping)

projects.add_command(list_projects)
projects.add_command(register_project)
projects.add_command(submit_dataset)
projects.add_command(register_and_submit_dataset)
projects.add_command(dump_project)
projects.add_command(update_project)
projects.add_command(register_subset_dimensions)
projects.add_command(register_supplemental_dimensions)
projects.add_command(add_dataset_requirements)
projects.add_command(replace_dataset_dimension_requirements)
projects.add_command(list_project_dimension_names)
projects.add_command(generate_project_config_from_ids)
projects.add_command(remove_project)

datasets.add_command(list_datasets)
datasets.add_command(register_dataset)
datasets.add_command(dump_dataset)
datasets.add_command(update_dataset)
datasets.add_command(generate_dataset_config_from_dataset)
datasets.add_command(remove_datasets)

registry.add_command(list_)
registry.add_command(create_registry)
registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(projects)
registry.add_command(datasets)
registry.add_command(bulk_register_cli)
# registry.add_command(data_sync)
