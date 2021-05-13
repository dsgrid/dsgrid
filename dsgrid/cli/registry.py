"""Manages a dsgrid registry."""

# TODO: need to support a dataset registry CLI seperate from submit-dataset
# TODO: Do we want to support dry-run mode for write only? and offline mode for read-only?

import getpass
import logging
from pathlib import Path

import click
from semver import VersionInfo

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.registry.common import VersionUpdateType

# from dsgrid.filesystem import aws
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


def _version_info_callback(ctx, param, val):
    if val is None:
        return val
    return VersionInfo.parse(val)


def _version_info_required_callback(ctx, param, val):
    return VersionInfo.parse(val)


def _version_update_callback(ctx, param, val):
    return VersionUpdateType(val)


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
    ctx.obj = RegistryManager.load(
        path, remote_path, offline_mode=offline, dry_run_mode=dry_run, no_prompts=no_prompts
    )


# TODO: Support registry file reads without syncing using something like sfs3
@click.command(name="list")
@click.pass_obj
def list_(registry_manager):
    """List the contents of a registry."""
    print(f"Registry: {registry_manager.path}")
    registry_manager.project_manager.show()
    registry_manager.dataset_manager.show()
    registry_manager.dimension_manager.show()
    registry_manager.dimension_mapping_manager.show()


@click.group()
@click.pass_obj
def projects(registry_manager):
    """Project subcommands"""


@click.group()
@click.pass_obj
def datasets(registry_manager):
    """Dataset subcommands"""


@click.group()
@click.pass_obj
def dimensions(registry_manager):
    """Dimension subcommands"""


@click.group()
@click.pass_obj
def dimension_mappings(registry_manager):
    """Dimension mapping subcommands"""


@click.command(name="list")
@click.pass_obj
def list_datasets(registry_manager):
    """List the registered dimensions."""
    registry_manager.dataset_manager.show()


@click.command(name="list")
@click.pass_obj
def list_dimensions(registry_manager):
    """List the registered dimensions."""
    registry_manager.dimension_manager.show()


@click.command(name="list")
@click.pass_obj
def list_dimension_mappings(registry_manager):
    """List the registered dimension mappings."""
    registry_manager.dimension_mapping_manager.show()


@click.command(name="list")
@click.pass_obj
def list_projects(registry_manager):
    """List the registered dimensions."""
    registry_manager.project_manager.show()


@click.command(name="remove")
@click.argument("project-id")
@click.pass_obj
def remove_project(registry_manager, project_id):
    """Remove a project from the dsgrid repository."""
    registry_manager.project_manager.remove(project_id)


@click.command(name="register")
@click.argument("project-config-file")
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_project(registry_manager, project_config_file, log_message):
    """Register a new project with the dsgrid repository."""
    submitter = getpass.getuser()
    registry_manager.project_manager.register(project_config_file, submitter, log_message)


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
    "--dimension-mapping-files",
    type=click.Path(exists=True),
    multiple=True,
    show_default=True,
    help="dimension mapping file(s)",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    type=str,
    help="reason for submission",
)
@click.pass_obj
def submit_dataset(registry_manager, dataset_id, project_id, dimension_mapping_files, log_message):
    """Submit a dataset to a dsgrid project."""
    submitter = getpass.getuser()
    manager = registry_manager.project_manager
    manager.submit_dataset(project_id, dataset_id, dimension_mapping_files, submitter, log_message)


# TODO: When resubmitting an existing dataset to a project, is that a new command or an extension
# of submit_dataset?
# TODO: update_dataset
@click.command(name="register")
@click.argument("dimension-config-file")
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
    help="Directory in which to create file",
)
@click.pass_obj
def dump_dimension(registry_manager, dimension_id, version, directory):
    """Dump a dimension config file from the registry."""
    manager = registry_manager.dimension_manager
    manager.dump(dimension_id, Path(directory), version=version)


@click.command(name="register")
@click.argument("dimension-mapping-config-file")
@click.option(
    # TODO: Why do we want this?
    "--force",
    default=False,
    is_flag=True,
    show_default=True,
    help="Register the dimension mappings even if there are duplicates",
)
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_dimension_mappings(
    registry_manager, dimension_mapping_config_file, log_message, force
):
    """Register new dimension mappings with the dsgrid repository."""
    submitter = getpass.getuser()
    manager = registry_manager.dimension_mapping_manager
    manager.register(dimension_mapping_config_file, submitter, log_message, force=force)


@click.command(name="update")
@click.argument("project-config-file")
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
    manager.update(project_config_file, project_id, submitter, update_type, log_message, version)


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
    help="Directory in which to create file",
)
@click.pass_obj
def dump_project(registry_manager, project_id, version, directory):
    """Dump a project config file from the registry."""
    manager = registry_manager.project_manager
    manager.dump(project_id, directory, version=version)


@click.command(name="register")
@click.argument("dataset-config-file")
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_obj
def register_dataset(registry_manager, dataset_config_file, log_message):
    """Register a new dataset with the dsgrid repository."""
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    manager.register(dataset_config_file, submitter, log_message)


@click.command(name="remove")
@click.argument("dataset-id")
@click.pass_obj
def remove_dataset(registry_manager, dataset_id, offline, dry_run):
    """Remove a dataset from the dsgrid repository."""
    registry_manager.dataset_manager.remove_dataset(dataset_id)


@click.command(name="update")
@click.argument("dataset-config-file")
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
@click.pass_obj
def update_dataset(registry_manager, dataset_config_file, log_message, update_type):
    """Update an existing dataset registry."""
    manager = registry_manager.dataset_manager
    submitter = getpass.getuser()
    manager.update(dataset_config_file, submitter, update_type, log_message)


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
    help="Directory in which to create file",
)
@click.pass_obj
def dump_dataset(registry_manager, dataset_id, version, directory):
    """Dump a dataset config file from the registry."""
    manager = registry_manager.dataset_manager
    manager.dump(dataset_id, directory, version=version)


@click.command()
@click.pass_obj
def sync(registry_manager):
    """Sync the official dsgrid registry to the local system."""
    # aws.sync(REMOTE_REGISTRY, registry_manager.path)


projects.add_command(list_projects)
projects.add_command(register_project)
projects.add_command(remove_project)
projects.add_command(update_project)
projects.add_command(submit_dataset)
projects.add_command(dump_project)

datasets.add_command(dump_dataset)
datasets.add_command(list_datasets)
datasets.add_command(remove_dataset)
datasets.add_command(register_dataset)
datasets.add_command(update_dataset)

dimensions.add_command(register_dimensions)
dimensions.add_command(list_dimensions)
dimensions.add_command(dump_dimension)

dimension_mappings.add_command(register_dimension_mappings)
dimension_mappings.add_command(list_dimension_mappings)

registry.add_command(list_)
registry.add_command(projects)
registry.add_command(datasets)
registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(sync)
