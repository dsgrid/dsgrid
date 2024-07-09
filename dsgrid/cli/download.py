"""Download a dataset."""

import sys

import rich_click as click


@click.command()
@click.argument("dataset")
def download(dataset):
    """Download a dataset."""
    print("not currently functional")
    sys.exit(1)
