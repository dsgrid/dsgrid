"""Submit a dataset for upload to the repository."""

import sys

import click


@click.command()
@click.argument("submit")
def submit(dataset):
    """Submit a dataset for upload to the repository."""
    print("not currently functional")
    sys.exit(1)
