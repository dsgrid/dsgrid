"""Main CLI command for dsgrid."""

import click

from dsgrid.cli.download import download
from dsgrid.cli.query import query
from dsgrid.cli.submit import submit


@click.group()
def cli():
    """dsgrid commands"""


cli.add_command(download)
cli.add_command(query)
cli.add_command(submit)
