"""Manages a dsgrid registry."""

import getpass
import logging
import sys
from pathlib import Path

import rich_click as click
from semver import VersionInfo

from dsgrid.cli.common import get_value_from_context, handle_dsgrid_exception
from dsgrid.common import REMOTE_REGISTRY
from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.common import VersionUpdateType
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.filters import ACCEPTED_OPS


logger = logging.getLogger(__name__)


def _version_info_callback(*args):
    val = args[2]
    if val is None:
        return val
    return VersionInfo.parse(val)


def _version_info_required_callback(*args):
    val = args[2]
    return VersionInfo.parse(val)


def _version_update_callback(*args):
    val = args[2]
    return VersionUpdateType(val)


def _path_callback(*args):
    val = args[2]
    if val is None:
        return val
    return Path(val)


"""
Click Group Definitions
"""


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
    conn = DatabaseConnection.from_url(
        get_value_from_context(ctx, "url"),
        database=get_value_from_context(ctx, "database_name"),
        username=get_value_from_context(ctx, "username"),
        password=get_value_from_context(ctx, "password"),
    )
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    no_prompts = ctx.parent.params["no_prompts"]
    offline = get_value_from_context(ctx, "offline")
    if "--help" in sys.argv:
        ctx.obj = None
    else:
        ctx.obj = RegistryManager.load(
            conn, remote_path, offline_mode=offline, no_prompts=no_prompts, scratch_dir=scratch_dir
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


# TODO: Support registry file reads without syncing using something like sfs3
@click.command(name="list")
@click.pass_obj
def list_(registry_manager):
    """List the contents of a registry."""
    print(f"Registry: {registry_manager.path}")
    registry_manager.show()


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
@click.argument("dimension-config-file", type=click.Path(exists=True), callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
@click.pass_context
def register_dimensions(
    ctx, registry_manager: RegistryManager, dimension_config_file: Path, log_message: str
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
        return 1


_dump_dimension_epilog = """
Examples:\n
$ dsgrid registry dimensions dump 17565829\n
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
        return 1


_update_dimension_epilog = """
Examples:\n
$ dsgrid registry dimensions update -d 17565829 -l "Update county dimension" -u major -v 1.0.0 dimension.json5\n
"""


@click.command(name="update", epilog=_update_dimension_epilog)
@click.argument("dimension-config-file", type=click.Path(exists=True), callback=_path_callback)
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
        return 1


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
    "dimension-mapping-config-file", type=click.Path(exists=True), callback=_path_callback
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
    ctx, registry_manager: RegistryManager, dimension_mapping_config_file: Path, log_message: str
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
        return 1


_dump_dimension_mapping_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings dump 17565575\n
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
        return 1


_update_dimension_mapping_epilog = """
Examples:\n
$ dsgrid registry dimension-mappings update \\ \n
    -d 17565575 \\ \n
    -l "Swap out the state to county mapping for my-dataset to that-project" \\ \n
    -u major \\ \n
    -v 1.0.0 dimension_mappings.json5"
"""


@click.command(name="update", epilog=_update_dimension_mapping_epilog)
@click.argument(
    "dimension-mapping-config-file", type=click.Path(exists=True), callback=_path_callback
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
        return 1


_register_project_epilog = """
Examples:\n
$ dsgrid registry projects register -l "Register project my-project" project.json5\n
"""


@click.command(name="register", epilog=_register_project_epilog)
@click.argument("project-config-file", type=click.Path(exists=True), callback=_path_callback)
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
        return 1


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
    callback=_path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered. Must mach the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mapping_base.DimensionMappingReferenceListModel",
    callback=_path_callback,
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
    registry_manager,
    dataset_id,
    project_id,
    dimension_mapping_file,
    dimension_mapping_references_file,
    autogen_reverse_supplemental_mappings,
    log_message,
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
    -d path/to/my/dataset \\ \n
    -p my-project-id \\ \n
    -d my-dataset-id \\ \n
    -m dimension_mappings.json5 \\ \n
    -l "Register and submit dataset my-dataset to project my-project." \n
"""


@click.command(epilog=_register_and_submit_dataset_epilog)
@click.option(
    "-c",
    "--dataset-config-file",
    required=True,
    type=click.Path(exists=True),
    callback=_path_callback,
    help="Dataset config file",
)
@click.option(
    "-d",
    "--dataset-path",
    required=True,
    help="Path to directory containing load data (Parquet) files.",
    type=click.Path(exists=True),
    callback=_path_callback,
)
@click.option(
    "-m",
    "--dimension-mapping-file",
    type=click.Path(exists=True),
    help="Dimension mapping file. Must match the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mappings_config.DimensionMappingsConfigModel",
    callback=_path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered. Must mach the data model defined by "
    "https://dsgrid.github.io/dsgrid/reference/data_models/dimension_mapping.html#dsgrid.config.dimension_mapping_base.DimensionMappingReferenceListModel",
    callback=_path_callback,
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
@click.pass_obj
@click.pass_context
def register_and_submit_dataset(
    ctx,
    registry_manager,
    dataset_config_file,
    dataset_path,
    dimension_mapping_file,
    dimension_mapping_references_file,
    autogen_reverse_supplemental_mappings,
    project_id,
    log_message,
):
    """Register a dataset and then submit it to a dsgrid project."""
    submitter = getpass.getuser()
    manager = registry_manager.project_manager
    res = handle_dsgrid_exception(
        ctx,
        manager.register_and_submit_dataset,
        dataset_config_file,
        dataset_path,
        project_id,
        submitter,
        log_message,
        dimension_mapping_file=dimension_mapping_file,
        dimension_mapping_references_file=dimension_mapping_references_file,
        autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
    )
    if res[1] != 0:
        return 1


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
        return 1


_update_project_epilog = """
Examples: \n
$ dsgrid registry projects update \\ \n
    -p my-project-id \\ \n
    -u patch \\ \n
    -v 1.5.0 \\ \n
    -l "Update description for project my-project-id." \n
"""


@click.command(name="update", epilog=_update_project_epilog)
@click.argument("project-config-file", type=click.Path(exists=True), callback=_path_callback)
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
        return 1


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
@click.argument("filename", callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for this addition.",
)
def register_subset_dimensions(
    ctx, registry_manager: RegistryManager, project_id: str, filename: Path, log_message: str
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
    )
    if res[1] != 0:
        return 1


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
@click.argument("filename", callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for this addition.",
)
def register_supplemental_dimensions(
    ctx, registry_manager, project_id, filename: Path, log_message
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
    )
    if res[1] != 0:
        return 1


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
@click.argument("filename", callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for the new datasets.",
)
def add_dataset_requirements(ctx, registry_manager, project_id, filename: Path, log_message):
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
    )
    if res[1] != 0:
        return 1


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
@click.argument("filename", callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="Please specify the reason for the new requirements.",
)
def replace_dataset_dimension_requirements(
    ctx, registry_manager, project_id, filename: Path, log_message
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
    )
    if res[1] != 0:
        return 1


_list_project_dimension_query_names_epilog = """
Examples:\n
$ dsgrid registry projects list-dimension-query-names my_project_id\n
$ dsgrid registry projects list-dimension-query-names --exclude-subset my_project_id\n
$ dsgrid registry projects list-dimension-query-names --exclude-supplemental my_project_id\n
"""


@click.command(
    name="list-dimension-query-names", epilog=_list_project_dimension_query_names_epilog
)
@click.argument("project-id")
@click.option(
    "-b",
    "--exclude-base",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude base dimension query names.",
)
@click.option(
    "-S",
    "--exclude-subset",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude subset dimension query names.",
)
@click.option(
    "-s",
    "--exclude-supplemental",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude supplemental dimension query names.",
)
@click.pass_obj
@click.pass_context
def list_project_dimension_query_names(
    ctx,
    registry_manager: RegistryManager,
    project_id,
    exclude_base,
    exclude_subset,
    exclude_supplemental,
):
    """List the project's dimension query names."""
    if exclude_base and exclude_subset and exclude_supplemental:
        print(
            "exclude_base, exclude_subset, and exclude_supplemental cannot all be set",
            file=sys.stderr,
        )
        return 1

    manager = registry_manager.project_manager
    res = handle_dsgrid_exception(ctx, manager.get_by_id, project_id)
    if res[1] != 0:
        return 1

    project_config = res[0]
    base = None if exclude_base else project_config.get_base_dimension_to_query_name_mapping()
    sub = None if exclude_subset else project_config.get_subset_dimension_to_query_name_mapping()
    supp = (
        None
        if exclude_supplemental
        else project_config.get_supplemental_dimension_to_query_name_mapping()
    )

    dimensions = sorted(DimensionType, key=lambda x: x.value)
    lines = []
    for dim_type in dimensions:
        lines.append(f"  {dim_type.value}:")
        if base:
            lines.append(f"    base: {base[dim_type]}")
        if sub:
            lines.append("    subset: " + " ".join(sub[dim_type]))
        if supp:
            lines.append("    supplemental: " + " ".join(supp[dim_type]))
    print("Dimension query names:")
    for line in lines:
        print(line)


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
"""


@click.command(name="register", epilog=_register_dataset_epilog)
@click.argument("dataset-config-file", type=click.Path(exists=True), callback=_path_callback)
@click.argument("dataset-path", type=click.Path(exists=True), callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
@click.pass_context
def register_dataset(ctx, registry_manager, dataset_config_file, dataset_path, log_message):
    """Register a new dataset with the registry. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dataset.html#dsgrid.config.dataset_config.DatasetConfigModel
    """
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    res = handle_dsgrid_exception(
        ctx, manager.register, dataset_config_file, dataset_path, submitter, log_message
    )
    if res[1] != 0:
        return 1


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
@click.argument("dataset-config-file", type=click.Path(exists=True), callback=_path_callback)
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
    ctx, registry_manager, dataset_config_file, dataset_id, log_message, update_type, version
):
    """Update an existing dataset in the registry. The contents of the JSON/JSON5 file
    must match the data model defined by this documentation:
    https://dsgrid.github.io/dsgrid/reference/data_models/dataset.html#dsgrid.config.dataset_config.DatasetConfigModel
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
        return 1


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
dimensions.add_command(update_dimension)

dimension_mappings.add_command(list_dimension_mappings)
dimension_mappings.add_command(register_dimension_mappings)
dimension_mappings.add_command(dump_dimension_mapping)
dimension_mappings.add_command(update_dimension_mapping)

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
projects.add_command(list_project_dimension_query_names)

datasets.add_command(list_datasets)
datasets.add_command(register_dataset)
datasets.add_command(dump_dataset)
datasets.add_command(update_dataset)

registry.add_command(list_)
registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(projects)
registry.add_command(datasets)
registry.add_command(data_sync)
