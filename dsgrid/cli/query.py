"""Runs dsgrid queries."""

import sys
from pathlib import Path

import click
from pydantic import ValidationError

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterExpressionModel,
    DimensionFilterExpressionRawModel,
    DimensionFilterBetweenColumnOperatorModel,
    DimensionFilterColumnOperatorModel,
    SupplementalDimensionFilterColumnOperatorModel,
)
from dsgrid.query.models import (
    AggregationModel,
    DimensionQueryNamesModel,
    ProjectQueryModel,
    ProjectQueryParamsModel,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    StandaloneDatasetModel,
    DatasetType,
    DatasetModel,
)
from dsgrid.query.query_submitter import ProjectQuerySubmitter  # , CompositeDatasetQuerySubmitter
from dsgrid.registry.registry_manager import RegistryManager


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


_COMMON_REGISTRY_OPTIONS = (
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
)


_COMMON_RUN_OPTIONS = (
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
    click.option(
        "--force",
        is_flag=True,
        default=False,
        show_default=True,
        help="Overwrite results directory if it exists.",
    ),
)


@click.command("create")
@click.argument("query_name")
@click.argument("project_id")
@click.argument("dataset_id")
@click.option(
    "-F",
    "--filters",
    type=click.Choice(
        [
            "expression",
            "between_column_operator",
            "column_operator",
            "supplemental_column_operator",
            "raw",
        ]
    ),
    multiple=True,
    help="Add a dimension filter. Requires user customization.",
)
@click.option(
    "-a",
    "--aggregation-function",
    default="sum",
    show_default=True,
    help="Aggregation function for any included default aggregations.",
)
@click.option(
    "-d",
    "--default-per-dataset-aggregation",
    is_flag=True,
    default=False,
    show_default=True,
    help="Add default per-dataset aggregration.",
)
@click.option(
    "-f",
    "--query-file",
    default="query.json5",
    show_default=True,
    help="Query file to create.",
    callback=lambda _, __, x: Path(x),
)
@click.option(
    "-r",
    "--default-result-aggregation",
    is_flag=True,
    default=False,
    show_default=True,
    help="Add default result aggregration.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite query file if it exists.",
)
@add_options(_COMMON_REGISTRY_OPTIONS)
def create_project(
    query_name,
    project_id,
    dataset_id,
    filters,
    aggregation_function,
    default_per_dataset_aggregation,
    query_file,
    default_result_aggregation,
    force,
    registry_path,
    remote_path,
    offline,
):
    """Create a default query file for a dsgrid project."""
    if query_file.exists():
        if force:
            query_file.unlink()
        else:
            print(
                f"{query_file} already exists. Choose a different name or pass --force to overwrite it.",
                file=sys.stderr,
            )
            sys.exit(1)

    registry_manager = RegistryManager.load(
        registry_path,
        remote_path=remote_path,
        offline_mode=offline,
    )
    project = registry_manager.project_manager.load_project(project_id)
    query = ProjectQueryModel(
        name=query_name,
        project=ProjectQueryParamsModel(
            project_id=project_id,
            dataset=DatasetModel(
                dataset_id=dataset_id,
                source_datasets=[
                    StandaloneDatasetModel(dataset_type=DatasetType.STANDALONE, dataset_id=x)
                    for x in project.config.list_registered_dataset_ids()
                ],
            ),
        ),
    )

    for dim_filter in filters:
        if dim_filter == "expression":
            flt = DimensionFilterExpressionModel(
                dimension_type=DimensionType.GEOGRAPHY,
                dimension_query_name="county",
                operator="==",
                value="",
            )
        elif dim_filter == "between_column_operator":
            flt = DimensionFilterBetweenColumnOperatorModel(
                dimension_type=DimensionType.TIME,
                dimension_query_name="time_est",
                lower_bound="",
                upper_bound="",
            )
        elif dim_filter == "column_operator":
            flt = DimensionFilterColumnOperatorModel(
                dimension_type=DimensionType.GEOGRAPHY,
                dimension_query_name="county",
                value="",
                operator="contains",
            )
        elif dim_filter == "supplemental_column_operator":
            flt = SupplementalDimensionFilterColumnOperatorModel(
                dimension_type=DimensionType.GEOGRAPHY,
                dimension_query_name="state",
            )
        elif dim_filter == "raw":
            flt = DimensionFilterExpressionRawModel(
                dimension_type=DimensionType.GEOGRAPHY,
                dimension_query_name="county",
                value="== '06037'",
            )
        else:
            assert False
        query.project.dataset.params.dimension_filters.append(flt)

    if default_result_aggregation:
        default_aggs = {}
        for dim_type, name in project.config.get_base_dimension_to_query_name_mapping().items():
            default_aggs[dim_type.value] = [name]
        if default_result_aggregation:
            query.result.aggregations = [
                AggregationModel(
                    dimensions=DimensionQueryNamesModel(**default_aggs),
                    aggregation_function=aggregation_function,
                ),
            ]

    query_file.write_text(query.json(indent=2))
    print(f"Wrote query to {query_file}")


@click.command("validate")
@click.argument("query_file", type=click.Path(exists=True), callback=lambda _, __, x: Path(x))
def validate_project(query_file):
    try:
        ProjectQueryModel.from_file(query_file)
        print(f"Validated {query_file}", file=sys.stderr)
    except ValidationError:
        print(f"Failed to validate query file {query_file}", file=sys.stderr)
        raise


@click.command("run")
@click.argument("query_definition_file", type=click.Path(exists=True))
@click.option(
    "-p",
    "--persist-intermediate-table",
    is_flag=True,
    default=True,
    show_default=True,
    help="Persist the intermediate table to the filesystem to allow for reuse.",
)
@click.option(
    "-z",
    "--zip",
    is_flag=True,
    default=False,
    show_default=True,
    help="Create a zip file containing all output files.",
)
@add_options(_COMMON_REGISTRY_OPTIONS)
@add_options(_COMMON_RUN_OPTIONS)
def run_project(
    query_definition_file,
    persist_intermediate_table,
    zip,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
    force,
):
    """Run a query on a dsgrid project."""
    query = ProjectQueryModel.from_file(query_definition_file)
    registry_manager = RegistryManager.load(
        registry_path,
        remote_path=remote_path,
        offline_mode=offline,
    )
    project = registry_manager.project_manager.load_project(query.project.project_id)
    ProjectQuerySubmitter(project, output).submit(
        query,
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
        zip=zip,
        force=force,
    )


@click.command("create_dataset")
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
def create_composite_dataset(
    query_definition_file,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
    force,
):
    """Run a query to create a composite dataset."""
    CreateCompositeDatasetQueryModel.from_file(query_definition_file)
    # TODO
    print("not implemented yet")
    sys.exit(1)
    # registry_manager = RegistryManager.load(
    #     registry_path,
    #     remote_path=remote_path,
    #     offline_mode=offline,
    # )
    # project = registry_manager.project_manager.load_project(query.project.project_id)
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query, force=force)


@click.command("run")
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
def query_composite_dataset(
    query_definition_file,
    registry_path,
    remote_path,
    offline,
    output,
    load_cached_table,
    force,
):
    """Run a query on a composite dataset."""
    CompositeDatasetQueryModel.from_file(query_definition_file)
    # TODO
    print("not implemented yet")
    sys.exit(1)
    # registry_manager = RegistryManager.load(
    #     registry_path,
    #     remote_path=remote_path,
    #     offline_mode=offline,
    # )
    # project = registry_manager.project_manager.load_project(query.project.project_id)
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query, force=force)


@click.group()
def query():
    """Query group commands"""


@click.group()
def project():
    """Project group commands"""


@click.group()
def composite_dataset():
    """Composite dataset group commands"""


query.add_command(composite_dataset)
query.add_command(project)
project.add_command(create_project)
project.add_command(validate_project)
project.add_command(run_project)
composite_dataset.add_command(create_composite_dataset)
composite_dataset.add_command(query_composite_dataset)
