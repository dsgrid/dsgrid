"""Runs dsgrid queries."""

import sys
from pathlib import Path

import click

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.project import Project
from dsgrid.query.models import (
    ProjectQueryModel,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter  # , CompositeDatasetQuerySubmitter


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


_COMMON_RUN_OPTIONS = (
    click.option(
        "--registry-path",
        default=LOCAL_REGISTRY,
        show_default=True,
        envvar="DSGRID_REGISTRY_PATH",
        help="Path to dsgrid registry. Override with the environment variable DSGRID_REGISTRY_PATH",
    ),
    click.option(
        "--remote-path",
        default=REMOTE_REGISTRY,
        show_default=True,
        help="Path to dsgrid remote registry",
    ),
    click.option(
        "--offline",
        "-o",
        is_flag=True,
        default=False,
        show_default=True,
        help="If offline is true, sync with the remote registry before running the query.",
    ),
    click.option(
        "-o",
        "--output",
        default="query_output",
        show_default=True,
        help="Output directory for query results",
        callback=lambda _, __, x: Path(x),
    ),
    click.option(
        "-i",
        "--load-cached-table",
        is_flag=True,
        default=True,
        show_default=True,
        help="Try to load a cached table if one exists.",
    ),
)


@click.command()
@click.argument("query_definition_file", type=click.Path(exists=True))
@click.option(
    "-p",
    "--persist-intermediate-table",
    is_flag=True,
    default=True,
    show_default=True,
    help="Persist the intermediate table to the filesystem to allow for reuse.",
)
@add_options(_COMMON_RUN_OPTIONS)
def project(
    query_definition_file,
    persist_intermediate_table,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
):
    """Run a query on a dsgrid project."""
    query = ProjectQueryModel.from_file(query_definition_file)
    project = Project.load(
        query.project.project_id,
        registry_path=registry_path,
        remote_path=remote_path,
        offline_mode=offline,
    )
    ProjectQuerySubmitter(project, output).submit(
        query,
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
    )


@click.command()
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
def create_composite_dataset(
    query_definition_file,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
):
    """Run a query to create a composite dataset."""
    CreateCompositeDatasetQueryModel.from_file(query_definition_file)
    # TODO
    print("not implemented yet")
    sys.exit(1)
    # project = Project.load(
    #     query.project.project_id,
    #     registry_path=registry_path,
    #     remote_path=remote_path,
    #     offline_mode=offline,
    # )
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query)


@click.command()
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
def composite_dataset(
    query_definition_file,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
):
    """Run a query on a composite dataset."""
    CompositeDatasetQueryModel.from_file(query_definition_file)
    # TODO
    print("not implemented yet")
    sys.exit(1)
    # project = Project.load(
    #     query.project.project_id,
    #     registry_path=registry_path,
    #     remote_path=remote_path,
    #     offline_mode=offline,
    # )
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query)


@click.group()
def query():
    """Query group commands"""


@click.group()
def run():
    """Query run group commands"""


query.add_command(run)
run.add_command(project)
run.add_command(create_composite_dataset)
run.add_command(composite_dataset)
