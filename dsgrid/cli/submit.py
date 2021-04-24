"""Submit a dataset for upload to the repository."""

import sys

import click


@click.command()
@click.argument("submit")
@click.option(
    "--offline",
    "-o",
    is_flag=True,
    help="run in registry commands in offline mode. WARNING: any commands you perform in offline mode run the risk of being out-of-sync with the latest dsgrid registry, and any write commands will not be officially synced with the remote registry",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    help="run registry commands in dry-run mode without writing to the local or remote registry",
)
def submit(dataset, offline, dry_run):
    """Submit a dataset for upload to the repository."""
    print("not currently functional")
    sys.exit(1)
