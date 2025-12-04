"""CLI commands to manage the dsgrid runtime configuration"""

import logging
import sys

import rich_click as click

from dsgrid.common import BackendEngine
from dsgrid.cli.common import handle_scratch_dir
from dsgrid.dsgrid_rc import (
    DsgridRuntimeConfig,
    DEFAULT_THRIFT_SERVER_URL,
    DEFAULT_BACKEND,
)
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.common import DatabaseConnection


logger = logging.getLogger(__name__)


@click.group()
def config():
    """Config commands"""


_config_epilog = """
Create a dsgrid configuration file to store registry connection settings and
other dsgrid parameters.

Examples:\n
$ dsgrid config create sqlite:///./registry.db\n
$ dsgrid config create sqlite:////projects/dsgrid/registries/standard-scenarios/registry.db\n
"""


@click.command(epilog=_config_epilog)
@click.argument("url")
@click.option(
    "-b",
    "--backend-engine",
    type=click.Choice([x.value for x in BackendEngine]),
    default=DEFAULT_BACKEND,
    help="Backend engine for SQL processing",
)
@click.option(
    "-t",
    "--thrift-server-url",
    type=str,
    default=DEFAULT_THRIFT_SERVER_URL,
    help="URL for the Apache Thrift Server to be used by chronify. "
    "Only applies if Spark is the backend engine.",
)
@click.option(
    "-m",
    "--use-hive-metastore",
    is_flag=True,
    default=False,
    help="Set this flag to use a Hive metastore when sharing data with chronify. "
    "Only applies if Spark is the backend engine.",
)
@click.option(
    "--timings/--no-timings",
    default=False,
    is_flag=True,
    show_default=True,
    help="Enable tracking of function timings.",
)
@click.option(
    "--use-absolute-db-path/--no-use-absolute-db-path",
    default=True,
    is_flag=True,
    show_default=True,
    help="Convert the SQLite database file path to an absolute path.",
)
# @click.option(
#    "-U",
#    "--username",
#    type=str,
#    default=getpass.getuser(),
#    help="Database username",
# )
# @click.option(
#    "-P",
#    "--password",
#    prompt=True,
#    hide_input=True,
#    type=str,
#    default=DEFAULT_DB_PASSWORD,
#    help="Database username",
# )
# @click.option(
#    "-o",
#    is_flag=True,
#    default=False,
#    show_default=True,
#    help="Run registry commands in offline mode. WARNING: any commands you perform in offline "
#    "mode run the risk of being out-of-sync with the latest dsgrid registry, and any write "
#    "commands will not be officially synced with the remote registry",
# )
@click.option(
    "--console-level",
    default="info",
    show_default=True,
    help="Console log level.",
)
@click.option(
    "--file-level",
    default="info",
    show_default=True,
    help="File log level.",
)
@click.option(
    "-r",
    "--reraise-exceptions",
    is_flag=True,
    default=False,
    show_default=True,
    help="Re-raise any dsgrid exception. Default is to log the exception and exit.",
)
@click.option(
    "-s",
    "--scratch-dir",
    default=None,
    callback=handle_scratch_dir,
    help="Base directory for dsgrid temporary directories. Must be accessible on all compute "
    "nodes. Defaults to the current directory.",
)
def create(
    url,
    backend_engine,
    thrift_server_url,
    use_hive_metastore,
    timings,
    use_absolute_db_path,
    # username,
    # password,
    # offline,
    console_level,
    file_level,
    reraise_exceptions,
    scratch_dir,
):
    """Create a local dsgrid runtime configuration file."""
    conn = DatabaseConnection(url=url)
    try:
        db_filename = conn.get_filename()
        if use_absolute_db_path and not db_filename.is_absolute():
            conn.url = f"sqlite:///{db_filename.resolve()}"

    except DSGInvalidParameter as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    if not db_filename.exists():
        print(f"The registry database file {db_filename} does not exist.", file=sys.stderr)
        sys.exit(1)

    dsgrid_config = DsgridRuntimeConfig(
        backend_engine=backend_engine,
        thrift_server_url=thrift_server_url,
        use_hive_metastore=use_hive_metastore,
        timings=timings,
        database_url=conn.url,
        # database_user=username,
        # database_password=password,
        offline=True,
        console_level=console_level,
        file_level=file_level,
        reraise_exceptions=reraise_exceptions,
        scratch_dir=scratch_dir,
    )
    dsgrid_config.dump()


config.add_command(create)
