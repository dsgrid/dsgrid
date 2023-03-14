"""Manages a dsgrid registry."""

import getpass
import logging
import sys
from pathlib import Path

import click
from semver import VersionInfo

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.common import VersionUpdateType
from dsgrid.registry.registry_database import DatabaseConnection

from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.filters import ACCEPTED_OPS


logger = logging.getLogger(__name__)


def _version_info_callback(ctx, param, val):
    if val is None:
        return val
    return VersionInfo.parse(val)


def _version_info_required_callback(ctx, param, val):
    return VersionInfo.parse(val)


def _version_update_callback(ctx, param, val):
    return VersionUpdateType(val)


def _path_callback(ctx, param, val):
    if val is None:
        return val
    return Path(val)


"""
Click Group Definitions
"""


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
    help="run in registry commands in offline mode. WARNING: any commands you perform in offline "
    "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
    "commands will not be officially synced with the remote registry",
)
@click.pass_context
def registry(ctx, url, db_name, remote_path, offline):
    """Manage a registry."""
    conn = DatabaseConnection.from_url(url, database=db_name)
    no_prompts = ctx.parent.params["no_prompts"]
    if "--help" in sys.argv:
        ctx.obj = None
    else:
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


@click.command(name="list")
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    filter table with a case-insensitive expression in the format 'field operation value',
    accept multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    example:\n
       -f 'Submitter == username' -f 'Description contains sector'
    """,
)
@click.pass_obj
def list_dimensions(registry_manager, filter):
    """List the registered dimensions."""
    registry_manager.dimension_manager.show(filters=filter)


@click.command(name="register")
@click.argument("dimension-config-file", type=click.Path(exists=True), callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_dimensions(registry_manager, dimension_config_file, log_message):
    """Register new dimensions with the dsgrid repository."""
    manager = registry_manager.dimension_manager
    submitter = getpass.getuser()
    manager.register(dimension_config_file, submitter, log_message)


@click.command(name="dump")
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
def dump_dimension(registry_manager, dimension_id, version, directory, force):
    """Dump a dimension config file (and any related data) from the registry."""
    manager = registry_manager.dimension_manager
    manager.dump(dimension_id, Path(directory), version=version, force=force)


@click.command(name="update")
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
def update_dimension(
    registry_manager, dimension_config_file, dimension_id, log_message, update_type, version
):
    """Update an existing dimension registry."""
    manager = registry_manager.dimension_manager
    submitter = getpass.getuser()
    manager.update_from_file(
        dimension_config_file, dimension_id, submitter, update_type, log_message, version
    )


"""
Dimension Mapping Commands
"""


@click.command(name="list")
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    filter table with a case-insensitive expression in the format 'field operation value',
    accept multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    example:\n
       -f 'Submitter == username' -f 'Description contains sector'
    """,
)
@click.pass_obj
def list_dimension_mappings(registry_manager, filter):
    """List the registered dimension mappings."""
    registry_manager.dimension_mapping_manager.show(filters=filter)


@click.command(name="register")
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
def register_dimension_mappings(registry_manager, dimension_mapping_config_file, log_message):
    """Register new dimension mappings with the dsgrid repository."""
    submitter = getpass.getuser()
    manager = registry_manager.dimension_mapping_manager
    manager.register(dimension_mapping_config_file, submitter, log_message)


@click.command(name="dump")
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
def dump_dimension_mapping(registry_manager, dimension_mapping_id, version, directory, force):
    """Dump a dimension mapping config file (and any related data) from the registry."""
    manager = registry_manager.dimension_mapping_manager
    manager.dump(dimension_mapping_id, Path(directory), version=version, force=force)


@click.command(name="update")
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
    registry_manager,
    dimension_mapping_config_file,
    dimension_mapping_id,
    log_message,
    update_type,
    version,
):
    """Update an existing dimension mapping registry."""
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


@click.command(name="list")
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    filter table with a case-insensitive expression in the format 'field operation value',
    accept multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    example:\n
       -f 'Submitter == username' -f 'Description contains sector'
    """,
)
@click.pass_obj
def list_projects(registry_manager, filter):
    """List the registered projects."""
    try:
        registry_manager.project_manager.show(filters=filter)
    except DSGInvalidParameter as exc:
        print(exc)
        sys.exit(1)


@click.command(name="register")
@click.argument("project-config-file", type=click.Path(exists=True), callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_project(
    registry_manager,
    project_config_file,
    log_message,
):
    """Register a new project with the dsgrid repository."""
    submitter = getpass.getuser()
    registry_manager.project_manager.register(
        project_config_file,
        submitter,
        log_message,
    )


@click.command()
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
    help="dimension mapping file",
    callback=_path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered.",
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


@click.command()
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
    type=click.Path(exists=True),
    callback=_path_callback,
)
@click.option(
    "-m",
    "--dimension-mapping-file",
    type=click.Path(exists=True),
    help="File containing dimension mappings to register with the dataset",
    callback=_path_callback,
)
@click.option(
    "-r",
    "--dimension-mapping-references-file",
    type=click.Path(exists=True),
    show_default=True,
    help="dimension mapping references file. Mutually exclusive with dimension_mapping_file. "
    "Use it when the mappings are already registered.",
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
def register_and_submit_dataset(
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
    manager.register_and_submit_dataset(
        dataset_config_file,
        dataset_path,
        project_id,
        submitter,
        log_message,
        dimension_mapping_file=dimension_mapping_file,
        dimension_mapping_references_file=dimension_mapping_references_file,
        autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
    )


@click.command(name="dump")
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
def dump_project(registry_manager, project_id, version, directory, force):
    """Dump a project config file from the registry."""
    manager = registry_manager.project_manager
    manager.dump(project_id, directory, version=version, force=force)


@click.command(name="update")
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
def update_project(
    registry_manager, project_config_file, project_id, log_message, update_type, version
):
    """Update an existing project registry."""
    manager = registry_manager.project_manager
    submitter = getpass.getuser()
    manager.update_from_file(
        project_config_file, project_id, submitter, update_type, log_message, version
    )


@click.command(name="list-dimension-query-names")
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
    "-s",
    "--exclude-supplemental",
    is_flag=True,
    default=False,
    show_default=True,
    help="Exclude supplemental dimension query names.",
)
@click.pass_obj
def list_project_dimension_query_names(
    registry_manager: RegistryManager, project_id, exclude_base, exclude_supplemental
):
    """List the project's dimension query names."""
    if exclude_base and exclude_supplemental:
        print("exclude_base and exclude_supplemental cannot both be set", file=sys.stderr)
        sys.exit(1)

    manager = registry_manager.project_manager
    project_config = manager.get_by_id(project_id)
    base_query_names_by_type = project_config.get_base_dimension_to_query_name_mapping()
    supp_query_names_by_type = project_config.get_supplemental_dimension_to_query_name_mapping()

    dimensions = sorted(DimensionType, key=lambda x: x.value)
    if not exclude_base and not exclude_supplemental:
        print("Dimension query names for base and supplemental dimensions:")
        for dimension_type in dimensions:
            base_dim = base_query_names_by_type[dimension_type]
            supp_dims = " ".join(supp_query_names_by_type[dimension_type])
            print(f"  {dimension_type.value}: base={base_dim} supplemental=[{supp_dims}]")
    elif exclude_supplemental:
        print("Dimension query names for base dimensions:")
        for dimension_type in dimensions:
            base_dim = base_query_names_by_type[dimension_type]
            print(f"  {dimension_type.value}: {base_dim}")
    elif exclude_base:
        print("Dimension query names for supplemental dimensions:")
        for dimension_type in dimensions:
            supp_dims = " ".join(supp_query_names_by_type[dimension_type])
            print(f"  {dimension_type.value}: [{supp_dims}]")
    else:
        assert False


"""
Dataset Commands
"""


@click.command(name="list")
@click.option(
    "-f",
    "--filter",
    multiple=True,
    type=str,
    help=f"""
    filter table with a case-insensitive expression in the format 'field operation value',
    accept multiple flags\b\n
    valid operations: {ACCEPTED_OPS}\n
    example:\n
       -f 'Submitter == username' -f 'Description contains sector'
    """,
)
@click.pass_obj
def list_datasets(registry_manager, filter):
    """List the registered dimensions."""
    registry_manager.dataset_manager.show(filters=filter)


@click.command(name="register")
@click.argument("dataset-config-file", type=click.Path(exists=True), callback=_path_callback)
@click.argument("dataset-path", type=click.Path(exists=True), callback=_path_callback)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_dataset(registry_manager, dataset_config_file, dataset_path, log_message):
    """Register a new dataset with the dsgrid repository."""
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    manager.register(dataset_config_file, dataset_path, submitter, log_message)


@click.command(name="dump")
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


@click.command(name="update")
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
def update_dataset(
    registry_manager, dataset_config_file, dataset_id, log_message, update_type, version
):
    """Update an existing dataset registry."""
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    manager.update_from_file(
        dataset_config_file, dataset_id, submitter, update_type, log_message, version
    )


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
    no_prompts = ctx.parent.parent.params["no_prompts"]
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
