"""Manages a dsgrid registry."""

# TODO: need to support a dataset registry CLI seperate from submit-dataset
# TODO: Do we want to support dry-run mode for write only? and offline mode for read-only?

import getpass
import logging

import click

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.loggers import setup_logging
from dsgrid.registry.common import VersionUpdateType

# from dsgrid.filesytem import aws
from dsgrid.registry.common import REGISTRY_LOG_FILE
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--path",
    default=LOCAL_REGISTRY,
    show_default=True,
    envvar="DSGRID_REGISTRY_PATH",
    help="path to dsgrid registry. Override with the environment variable DSGRID_REGISTRY_PATH",
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
def registry(ctx, path, offline, dry_run):
    """Manage a registry."""
    # We want to keep a log of items that have been registered on the
    # current system. But we probably don't want this to grow forever.
    # Consider truncating or rotating.
    # TODO: pass in offline and dry_run arguments into logs
    setup_logging("dsgrid", REGISTRY_LOG_FILE, mode="a")


@click.command()
@click.argument("registry_path")
def create(registry_path):
    """Create a new registry."""
    RegistryManager.create(registry_path)


# TODO: Support registry file reads without syncing using something like sfs3
@click.command(name="list")
@click.pass_context
# TODO: options for only projects or datasets
# TODO: can we run this in offline mode?
def list_(ctx):
    """List the contents of a registry."""
    registry_path = ctx.parent.params["path"]
    offline_mode = ctx.parent.params["offline"]
    dry_run_mode = ctx.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode)
    print(f"Registry: {registry_path}")
    manager.project_manager.show()
    manager.dataset_manager.show()
    manager.dimension_manager.show()
    manager.dimension_mapping_manager.show()


@click.group()
@click.pass_context
def projects(ctx):
    """Project subcommands"""


@click.group()
@click.pass_context
def datasets(ctx):
    """Dataset subcommands"""


@click.group()
@click.pass_context
def dimensions(ctx):
    """Dimension subcommands"""


@click.group()
@click.pass_context
def dimension_mappings(ctx):
    """Dimension mapping subcommands"""


@click.command(name="list")
@click.pass_context
def list_dimensions(ctx):
    """List the registered dimensions."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode).dimension_manager
    manager.show()


@click.command(name="list")
@click.pass_context
def list_dimension_mappings(ctx):
    """List the registered dimension mappings."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(
        registry_path, offline_mode, dry_run_mode
    ).dimension_mapping_manager
    manager.show()


@click.command(name="remove")
@click.argument("project-id")
@click.pass_context
def remove_project(ctx, project_id):
    """Remove a project from the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode).project_manager
    manager.remove(project_id)


@click.command(name="register")
@click.argument("project-config-file")
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_context
def register_project(ctx, project_config_file, log_message):
    """Register a new project with the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode)
    submitter = getpass.getuser()
    manager.register(project_config_file, submitter, log_message)


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
@click.pass_context
def submit_dataset(ctx, dataset_id, project_id, dimension_mapping_files, log_message):
    """Submit a dataset to a dsgrid project."""
    registry_path = ctx.parent.parent.params["path"]
    manager = RegistryManager.load(registry_path).project_manager
    submitter = getpass.getuser()
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
@click.pass_context
def register_dimensions(ctx, dimension_config_file, log_message):
    """Register new dimensions with the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode).dimension_manager
    submitter = getpass.getuser()
    manager.register(dimension_config_file, submitter, log_message)


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
@click.pass_context
def register_dimension_mappings(ctx, dimension_mapping_config_file, log_message, force):
    """Register new dimension mappings with the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    submitter = getpass.getuser()
    manager = RegistryManager.load(
        registry_path, offline_mode, dry_run_mode
    ).dimension_mapping_manager
    manager.register(dimension_mapping_config_file, submitter, log_message, force=force)


@click.command(name="update")
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
    type=click.Choice([x.value for x in VersionUpdateType]),
    callback=lambda ctx, x: VersionUpdateType(x),
)
@click.pass_context
def update_project(ctx, project_config_file, log_message, update_type):
    """Update an existing project registry."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode).project_manager
    submitter = getpass.getuser()
    manager.update(project_config_file, submitter, update_type, log_message)


@click.command(name="register")
@click.argument("dataset-config-file")
@click.option(
    "-l",
    "--log-message",
    required=True,
    help="reason for submission",
)
@click.pass_context
def register_dataset(ctx, dataset_config_file, log_message):
    """Register a new dataset with the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode).dataset_manager
    submitter = getpass.getuser()
    manager.register(dataset_config_file, submitter, log_message)


@click.command(name="remove")
@click.argument("dataset-id")
@click.pass_context
def remove_dataset(ctx, dataset_id, offline, dry_run):
    """Remove a dataset from the dsgrid repository."""
    registry_path = ctx.parent.parent.params["path"]
    offline_mode = ctx.parent.parent.params["offline"]
    dry_run_mode = ctx.parent.parent.params["dry_run"]
    manager = RegistryManager.load(registry_path, offline_mode, dry_run_mode)
    manager.remove_dataset(dataset_id)


@click.command()
@click.pass_context
# TODO is this a sync pull command?
def sync(ctx):
    """Sync the official dsgrid registry to the local system."""
    registry_path = ctx.parent.params["path"]
    # aws.sync(REMOTE_REGISTRY, registry_path)


projects.add_command(register_project)
projects.add_command(remove_project)
projects.add_command(update_project)
projects.add_command(submit_dataset)

datasets.add_command(remove_dataset)
datasets.add_command(register_dataset)

dimensions.add_command(register_dimensions)
dimensions.add_command(list_dimensions)

dimension_mappings.add_command(register_dimension_mappings)
dimension_mappings.add_command(list_dimension_mappings)

registry.add_command(create)
registry.add_command(list_)
registry.add_command(projects)
registry.add_command(datasets)
registry.add_command(dimensions)
registry.add_command(dimension_mappings)
registry.add_command(sync)
