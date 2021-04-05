"""Manages a dsgrid registry."""

import getpass
import logging

import click

from dsgrid.common import S3_REGISTRY, LOCAL_REGISTRY
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


@click.group()
# This exists for test & dev. May go away.
@click.option(
    "--path",
    default=LOCAL_REGISTRY,  # TEMPORARY: S3_REGISTRY is not yet supported
    show_default=True,
    envvar="DSGRID_REGISTRY_PATH",
    help="INTERNAL-ONLY: path to dsgrid registry. Override with the environment variable DSGRID_REGISTRY_PATH",
)
@click.pass_context
def registry(ctx, path):
    """Manage a registry."""


@click.command()
@click.argument("registry_path")
def create(registry_path):
    """Create a new registry."""
    RegistryManager.create(registry_path)


@click.command(name="list")
@click.pass_context
# TODO: options for only projects or datasets
def list_(ctx):
    """List the contents of a registry."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    print(f"Registry: {registry_path}")
    print("Projects:")
    for project in manager.list_projects():
        print(f"  - {project}")
    print("\nDatasets:")
    for dataset in manager.list_datasets():
        print(f"  - {dataset}")
    print("\nDimensions:")
    for dimension in manager.list_dimensions():
        print(f"  - {dimension}")


@click.command()
@click.argument("project-id")
@click.pass_context
def remove_project(ctx, project_id):
    """Remove a project from the dsgrid repository."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    manager.remove_project(project_id)


@click.command()
@click.argument("project-config-file")
@click.option(
    "-l",
    "--log-message",
    default="Initial submission",
    show_default=True,
    help="reason for submission",
)
@click.pass_context
def register_project(ctx, project_config_file, log_message):
    """Register a new project with the dsgrid repository."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    submitter = getpass.getuser()
    manager.register_project(project_config_file, submitter, log_message)


@click.command()
@click.argument("dimension-config-file")
@click.option(
    "-l",
    "--log-message",
    default="Initial submission",
    show_default=True,
    help="reason for submission",
)
@click.pass_context
def register_dimension(ctx, dimension_config_file, log_message):
    """Register a new project with the dsgrid repository."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    submitter = getpass.getuser()
    manager.register_dimension(dimension_config_file, submitter, log_message)


@click.command()
@click.argument("project-config-file")
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
    type=str,
    help="type of update, options: major | minor | patch",
)
@click.pass_context
def update_project(ctx, project_config_file, log_message, update_type):
    """Update an existing project registry."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    submitter = getpass.getuser()
    manager.update_project(project_config_file, submitter, update_type, log_message)
    # TODO


@click.command()
@click.argument("dataset-config-file")
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
@click.pass_context
def submit_dataset(ctx, dataset_config_file, project_id, log_message):
    """Submit a new dataset to a dsgrid project."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    submitter = getpass.getuser()
    manager.submit_dataset(dataset_config_file, project_id, submitter, log_message)


# TODO: When resubmitting an existing dataset to a project, is that a new command or an extension
# of submit_dataset?
# TODO: update_dataset


@click.command()
@click.argument("dataset-id")
@click.pass_context
def remove_dataset(ctx, dataset_id):
    """Remove a dataset from the dsgrid repository."""
    registry_path = ctx.parent.params["path"]
    manager = RegistryManager.load(registry_path)
    manager.remove_dataset(dataset_id)


registry.add_command(create)
registry.add_command(list_)
registry.add_command(remove_dataset)
registry.add_command(remove_project)
registry.add_command(register_project)
registry.add_command(register_dimension)
registry.add_command(submit_dataset)
registry.add_command(update_project)