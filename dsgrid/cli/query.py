"""Run a query on a dataset."""

import sys

import click


@click.command()
@click.argument("dataset")
@click.argument("query_string")
@click.option(
    "-o",
    "--output",
    default="output",
    show_default=True,
    help="output directory for query results",
)
def query(dataset, query_string, output):
    """Run a query on a dataset."""
    print("not currently functional")
    sys.exit(1)
