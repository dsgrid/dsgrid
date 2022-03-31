"""Interactive registry management tool"""

import getpass
import logging
import os
import sys

import click
from devtools import debug
from semver import VersionInfo

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.loggers import setup_logging
from dsgrid.registry.common import VersionUpdateType
from dsgrid.registry.registry_manager import RegistryManager


@click.command()
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
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
def load(path, remote_path, offline, verbose):
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", "dsgrid.log", console_level=level, file_level=level, mode="a")
    return RegistryManager.load(path, remote_path, offline_mode=offline)


if __name__ == "__main__":
    manager = load(standalone_mode=False)
    if isinstance(manager, int):
        # The user likely invoked --help
        sys.exit(manager)
    project_manager = manager.project_manager
    dataset_manager = manager.dataset_manager
    dimension_manager = manager.dimension_manager
    dimension_mapping_manager = manager.dimension_mapping_manager
    submitter = getpass.getuser()
