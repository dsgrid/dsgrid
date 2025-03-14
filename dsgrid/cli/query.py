"""Runs dsgrid queries."""

import logging
import sys
from pathlib import Path

import rich_click as click
from pydantic import ValidationError

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.cli.common import (
    check_output_directory,
    get_value_from_context,
    handle_dsgrid_exception,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.dimension_filters import (
    DimensionFilterType,
    DimensionFilterExpressionModel,
    DimensionFilterExpressionRawModel,
    DimensionFilterBetweenColumnOperatorModel,
    DimensionFilterColumnOperatorModel,
    SubsetDimensionFilterModel,
    SupplementalDimensionFilterColumnOperatorModel,
)
from dsgrid.filesystem.factory import make_filesystem_interface
from dsgrid.query.derived_dataset import create_derived_dataset_config_from_query
from dsgrid.query.models import (
    AggregationModel,
    DimensionNamesModel,
    ProjectQueryModel,
    ProjectQueryParamsModel,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    StandaloneDatasetModel,
    DatasetModel,
)
from dsgrid.query.query_submitter import (
    DatasetMapper,
    ProjectQuerySubmitter,
)  # , CompositeDatasetQuerySubmitter
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager


QUERY_OUTPUT_DIR = "query_output"

logger = logging.getLogger(__name__)


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


_COMMON_REGISTRY_OPTIONS = (
    click.option(
        "--remote-path",
        default=REMOTE_REGISTRY,
        show_default=True,
        help="Path to dsgrid remote registry",
    ),
)


_COMMON_RUN_OPTIONS = (
    click.option(
        "-o",
        "--output",
        default=QUERY_OUTPUT_DIR,
        show_default=True,
        type=str,
        help="Output directory for query results",
    ),
    click.option(
        "--load-cached-table/--no-load-cached-table",
        is_flag=True,
        default=True,
        show_default=True,
        help="Try to load a cached table if one exists.",
    ),
    click.option(
        "--overwrite",
        is_flag=True,
        default=False,
        show_default=True,
        help="Overwrite results directory if it exists.",
    ),
)


_create_project_query_epilog = """
Examples:\n
$ dsgrid query project create my_query_result_name my_project_id my_dataset_id\n
$ dsgrid query project create --default-result-aggregation my_query_result_name my_project_id my_dataset_id\n
"""


@click.command("create", epilog=_create_project_query_epilog)
@click.argument("query_name")
@click.argument("project_id")
@click.argument("dataset_id")
@click.option(
    "-F",
    "--filters",
    type=click.Choice([x.value for x in DimensionFilterType]),
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
    "-f",
    "--query-file",
    default="query.json5",
    show_default=True,
    help="Query file to create.",
    callback=lambda *x: Path(x[2]),
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
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite query file if it exists.",
)
@add_options(_COMMON_REGISTRY_OPTIONS)
@click.pass_context
def create_project_query(
    ctx,
    query_name,
    project_id,
    dataset_id,
    filters,
    aggregation_function,
    query_file,
    default_result_aggregation,
    overwrite,
    remote_path,
):
    """Create a default query file for a dsgrid project."""
    if query_file.exists():
        if overwrite:
            query_file.unlink()
        else:
            print(
                f"{query_file} already exists. Choose a different name or pass --overwrite to overwrite it.",
                file=sys.stderr,
            )
            return 1

    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    registry_manager = RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    )
    project = registry_manager.project_manager.load_project(project_id)
    query = ProjectQueryModel(
        name=query_name,
        project=ProjectQueryParamsModel(
            project_id=project_id,
            dataset=DatasetModel(
                dataset_id=dataset_id,
                source_datasets=[
                    StandaloneDatasetModel(dataset_id=x)
                    for x in project.config.list_registered_dataset_ids()
                ],
            ),
        ),
    )

    for dim_filter in filters:
        filter_type = DimensionFilterType(dim_filter)
        match filter_type:
            case DimensionFilterType.EXPRESSION:
                flt = DimensionFilterExpressionModel(
                    dimension_type=DimensionType.GEOGRAPHY,
                    dimension_name="county",
                    operator="==",
                    value="",
                )
            case DimensionFilterType.BETWEEN_COLUMN_OPERATOR:
                flt = DimensionFilterBetweenColumnOperatorModel(
                    dimension_type=DimensionType.TIME,
                    dimension_name="time_est",
                    lower_bound="",
                    upper_bound="",
                )
            case DimensionFilterType.COLUMN_OPERATOR:
                flt = DimensionFilterColumnOperatorModel(
                    dimension_type=DimensionType.GEOGRAPHY,
                    dimension_name="county",
                    value="",
                    operator="contains",
                )
            case DimensionFilterType.SUPPLEMENTAL_COLUMN_OPERATOR:
                flt = SupplementalDimensionFilterColumnOperatorModel(
                    dimension_type=DimensionType.GEOGRAPHY,
                    dimension_name="state",
                )
            case DimensionFilterType.EXPRESSION_RAW:
                flt = DimensionFilterExpressionRawModel(
                    dimension_type=DimensionType.GEOGRAPHY,
                    dimension_name="county",
                    value="== '06037'",
                )
            case DimensionFilterType.SUBSET:
                flt = SubsetDimensionFilterModel(
                    dimension_type=DimensionType.SUBSECTOR,
                    dimension_names=["commercial_subsectors", "residential_subsectors"],
                )
            case _:
                raise NotImplementedError(f"Bug: {filter_type}")
        query.project.dataset.params.dimension_filters.append(flt)

    if default_result_aggregation:
        default_aggs = {
            k.value: v for k, v in project.config.get_dimension_type_to_base_name_mapping().items()
        }
        if default_result_aggregation:
            query.result.aggregations = [
                AggregationModel(
                    dimensions=DimensionNamesModel(**default_aggs),
                    aggregation_function=aggregation_function,
                ),
            ]

    query_file.write_text(query.model_dump_json(indent=2))
    print(f"Wrote query to {query_file}")


@click.command("validate")
@click.argument("query_file", type=click.Path(exists=True), callback=lambda *x: Path(x[2]))
def validate_project_query(query_file):
    try:
        ProjectQueryModel.from_file(query_file)
        print(f"Validated {query_file}", file=sys.stderr)
    except ValidationError:
        print(f"Failed to validate query file {query_file}", file=sys.stderr)
        raise


_run_project_query_epilog = """
Examples:\n
$ dsgrid query project run query.json5
"""


@click.command("run", epilog=_run_project_query_epilog)
@click.argument("query_definition_file", type=click.Path(exists=True))
@click.option(
    "--persist-intermediate-table/--no-persist-intermediate-table",
    is_flag=True,
    default=True,
    show_default=True,
    help="Persist the intermediate table to the filesystem to allow for reuse.",
)
@click.option(
    "-z",
    "--zip-file",
    is_flag=True,
    default=False,
    show_default=True,
    help="Create a zip file containing all output files.",
)
@add_options(_COMMON_REGISTRY_OPTIONS)
@add_options(_COMMON_RUN_OPTIONS)
@click.pass_context
def run_project_query(
    ctx,
    query_definition_file,
    persist_intermediate_table,
    zip_file,
    remote_path,
    output,
    load_cached_table,
    overwrite,
):
    """Run a query on a dsgrid project."""
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    query = ProjectQueryModel.from_file(query_definition_file)
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    registry_manager = RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    )
    project = registry_manager.project_manager.load_project(query.project.project_id)
    fs_interface = make_filesystem_interface(output)
    submitter = ProjectQuerySubmitter(project, fs_interface.path(output))
    res = handle_dsgrid_exception(
        ctx,
        submitter.submit,
        query,
        scratch_dir,
        persist_intermediate_table=persist_intermediate_table,
        load_cached_table=load_cached_table,
        zip_file=zip_file,
        force=overwrite,
    )
    if res[1] != 0:
        ctx.exit(res[1])


_map_dataset_epilog = """
Examples:\n
$ dsgrid query project map_dataset project_id dataset_id
"""


@click.command("map-dataset", epilog=_map_dataset_epilog)
@click.argument("project-id")
@click.argument("dataset-id")
@click.option(
    "-o",
    "--output",
    default=QUERY_OUTPUT_DIR,
    show_default=True,
    type=str,
    help="Output directory for query results",
    callback=lambda *x: Path(x[2]),
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite results directory if it exists.",
)
@add_options(_COMMON_REGISTRY_OPTIONS)
@click.pass_context
def map_dataset(
    ctx: click.Context,
    project_id: str,
    dataset_id: str,
    remote_path,
    output: Path,
    overwrite: bool,
):
    """Map a dataset to the project's base dimensions."""
    # TODO: Support supplemental dimensions: issue #343.
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    registry_manager = RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    )
    project = registry_manager.project_manager.load_project(project_id)
    fs_interface = make_filesystem_interface(output)
    mapper = DatasetMapper(project, dataset_id, fs_interface.path(output))
    res = handle_dsgrid_exception(
        ctx,
        mapper.submit,
        scratch_dir,
        overwrite=overwrite,
    )
    if res[1] != 0:
        ctx.exit(res[1])


@click.command("create_dataset")
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
@click.pass_context
def create_composite_dataset(
    ctx,
    query_definition_file,
    remote_path,
    output,
    load_cached_table,
    overwrite,
):
    """Run a query to create a composite dataset."""
    CreateCompositeDatasetQueryModel.from_file(query_definition_file)
    # conn = DatabaseConnection.from_url(
    #     get_value_from_context(ctx, "url"),
    #     database=get_value_from_context(ctx, "database_name"),
    #     username=get_value_from_context(ctx, "username"),
    #     password=get_value_from_context(ctx, "password"),
    # )
    # TODO
    print("not implemented yet")
    return 1
    # registry_manager = RegistryManager.load(
    #     conn,
    #     remote_path=remote_path,
    #     offline_mode=get_value_from_context(ctx, "offline"),
    # )
    # project = registry_manager.project_manager.load_project(query.project.project_id)
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query, force=overwrite)


@click.command("run")
@click.argument("query_definition_file", type=click.Path(exists=True))
@add_options(_COMMON_RUN_OPTIONS)
@click.pass_context
def query_composite_dataset(
    ctx,
    query_definition_file,
    remote_path,
    output,
    load_cached_table,
    overwrite,
):
    """Run a query on a composite dataset."""
    CompositeDatasetQueryModel.from_file(query_definition_file)
    # conn = DatabaseConnection.from_url(
    #     get_value_from_context(ctx, "url"),
    #     database=get_value_from_context(ctx, "database_name"),
    #     username=get_value_from_context(ctx, "username"),
    #     password=get_value_from_context(ctx, "password"),
    # )
    # TODO
    print("not implemented yet")
    return 1
    # registry_manager = RegistryManager.load(
    #     registry_path,
    #     remote_path=remote_path,
    #     offline_mode=get_value_from_context(ctx, "offline"),
    # )
    # project = registry_manager.project_manager.load_project(query.project.project_id)
    # CompositeDatasetQuerySubmitter.submit(project, output).submit(query, overwrite=overwrite)


_create_derived_dataset_config_epilog = f"""
Examples:\n
$ dsgrid query project create-derived-dataset-config {QUERY_OUTPUT_DIR}/my_query_result_name my_dataset_config\n
"""


@click.command(epilog=_create_derived_dataset_config_epilog)
@click.argument("src")
@click.argument("dst")
@add_options(_COMMON_REGISTRY_OPTIONS)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite results directory if it exists.",
)
@click.pass_context
def create_derived_dataset_config(ctx, src, dst, remote_path, overwrite):
    """Create a derived dataset configuration and dimensions from a query result."""
    fs_interface = make_filesystem_interface(src)
    src_path = fs_interface.path(src)
    if not src_path.exists():
        print(f"{src} does not exist", file=sys.stderr)
        return 1
    dst_path = fs_interface.path(dst)
    check_output_directory(dst_path, fs_interface, overwrite)

    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    registry_manager = RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    )
    result = create_derived_dataset_config_from_query(src_path, dst_path, registry_manager)
    if not result:
        logger.error("The query defined in %s does not support a derived dataset.", src)
        return 1


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
project.add_command(create_project_query)
project.add_command(validate_project_query)
project.add_command(run_project_query)
project.add_command(create_derived_dataset_config)
project.add_command(map_dataset)
composite_dataset.add_command(create_composite_dataset)
composite_dataset.add_command(query_composite_dataset)
