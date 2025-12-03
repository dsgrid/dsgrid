"""Runs dsgrid queries."""

import logging
import sys
from pathlib import Path

import rich_click as click
from chronify.utils.path_utils import check_overwrite
from pydantic import ValidationError

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.cli.common import (
    check_output_directory,
    get_value_from_context,
    handle_dsgrid_exception,
    path_callback,
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
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan
from dsgrid.query.derived_dataset import create_derived_dataset_config_from_query
from dsgrid.query.models import (
    AggregationModel,
    DatasetQueryModel,
    DimensionNamesModel,
    ProjectQueryModel,
    ProjectQueryParamsModel,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    StandaloneDatasetModel,
    ColumnType,
    DatasetModel,
    make_query_for_standalone_dataset,
)
from dsgrid.query.query_submitter import (
    DatasetQuerySubmitter,
    ProjectQuerySubmitter,
)  # , CompositeDatasetQuerySubmitter
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import dump_json_file


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
        callback=path_callback,
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
    callback=path_callback,
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
    check_overwrite(query_file, overwrite)
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    with RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    ) as registry_manager:
        project = registry_manager.project_manager.load_project(project_id)
        _create_project_query_impl(
            ctx,
            project,
            query_name,
            project_id,
            dataset_id,
            filters,
            aggregation_function,
            query_file,
            default_result_aggregation,
        )


def _create_project_query_impl(
    ctx,
    project,
    query_name,
    project_id,
    dataset_id,
    filters,
    aggregation_function,
    query_file,
    default_result_aggregation,
):
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
                msg = f"Bug: {filter_type}"
                raise NotImplementedError(msg)
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
    print(f"Wrote query to {query_file}", file=sys.stderr)


@click.command("validate")
@click.argument("query_file", type=click.Path(exists=True), callback=path_callback)
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
    "-c",
    "--checkpoint-file",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Checkpoint file created by a previous map operation. If passed, the code will "
    "read it and resume from the last persisted file.",
)
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
    ctx: click.Context,
    query_definition_file: Path,
    checkpoint_file: Path | None,
    persist_intermediate_table: bool,
    zip_file: bool,
    remote_path: str,
    output: Path,
    load_cached_table: bool,
    overwrite: bool,
):
    """Run a query on a dsgrid project."""
    query = ProjectQueryModel.from_file(query_definition_file)
    _run_project_query(
        ctx,
        query,
        checkpoint_file,
        persist_intermediate_table,
        zip_file,
        remote_path,
        output,
        load_cached_table,
        overwrite,
    )


def _run_project_query(
    ctx: click.Context,
    query: ProjectQueryModel,
    checkpoint_file: Path | None,
    persist_intermediate_table: bool,
    zip_file: bool,
    remote_path,
    output: Path,
    load_cached_table: bool,
    overwrite: bool,
) -> None:
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    with RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    ) as registry_manager:
        project = registry_manager.project_manager.load_project(query.project.project_id)
        fs_interface = make_filesystem_interface(output)
        submitter = ProjectQuerySubmitter(project, fs_interface.path(output))
        res = handle_dsgrid_exception(
            ctx,
            submitter.submit,
            query,
            scratch_dir,
            checkpoint_file=checkpoint_file,
            persist_intermediate_table=persist_intermediate_table,
            load_cached_table=load_cached_table,
            zip_file=zip_file,
            overwrite=overwrite,
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
    "-c",
    "--checkpoint-file",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Checkpoint file created by a previous map operation. If passed, the code will "
    "read it and resume from the last persisted file.",
)
@click.option(
    "-p",
    "--mapping-plan",
    type=click.Path(exists=True),
    help="Path to a mapping plan file. If not provided, the default mapping plan will be used.",
    callback=path_callback,
)
@click.option(
    "--persist-intermediate-table/--no-persist-intermediate-table",
    is_flag=True,
    default=True,
    show_default=True,
    help="Persist the intermediate table to the filesystem to allow for reuse.",
)
@click.option(
    "-t",
    "--column-type",
    type=click.Choice([x.value for x in ColumnType]),
    default=ColumnType.DIMENSION_NAMES.value,
    callback=lambda *x: ColumnType(x[2]),
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
def map_dataset(
    ctx: click.Context,
    project_id: str,
    dataset_id: str,
    checkpoint_file: Path | None,
    mapping_plan: Path | None,
    persist_intermediate_table: bool,
    remote_path,
    output: Path,
    load_cached_table: bool,
    overwrite: bool,
    column_type: ColumnType,
    zip_file: bool,
):
    """Map a dataset to the project's base dimensions."""
    plan = DatasetMappingPlan.from_file(mapping_plan) if mapping_plan else None
    query = make_query_for_standalone_dataset(
        project_id, dataset_id, plan, column_type=column_type
    )
    _run_project_query(
        ctx=ctx,
        query=query,
        checkpoint_file=checkpoint_file,
        persist_intermediate_table=persist_intermediate_table,
        zip_file=zip_file,
        remote_path=remote_path,
        output=output,
        load_cached_table=load_cached_table,
        overwrite=overwrite,
    )


@click.command("create-query")
@click.argument("name", type=str)
@click.argument("dataset_id", type=str)
@click.option(
    "-f",
    "--query-file",
    default="dataset_query.json5",
    show_default=True,
    help="Query file to create.",
    callback=path_callback,
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    show_default=True,
    help="Overwrite query file if it exists.",
)
@click.pass_context
def create_dataset_query(
    ctx,
    name: str,
    dataset_id: str,
    query_file: Path,
    overwrite: bool,
):
    """Create a query file to be used for mapping a dataset to an arbitrary list of dimensions."""
    query = DatasetQueryModel(name=name, dataset_id=dataset_id, to_dimension_references=[])
    check_overwrite(query_file, overwrite)
    data = query.model_dump(mode="json")
    unsupported_result_fields = (
        "column_type",
        "replace_ids_with_names",
        "aggregations",
        "aggregate_each_dataset",
        "reports",
        "dimension_filters",
        "time_zone",
    )
    data.pop("version")
    for field in unsupported_result_fields:
        data["result"].pop(field)

    dump_json_file(data, query_file, indent=2)
    print(f"Wrote query to {query_file}", file=sys.stderr)


_run_dataset_query_epilog = """
Examples:\n
$ dsgrid query dataset run query.json5
"""


@click.command("run", epilog=_run_dataset_query_epilog)
@click.argument("query_definition_file", type=click.Path(exists=True))
@click.option(
    "-c",
    "--checkpoint-file",
    type=click.Path(exists=True),
    callback=path_callback,
    help="Checkpoint file created by a previous map operation. If passed, the code will "
    "read it and resume from the last persisted file.",
)
@click.option(
    "-o",
    "--output",
    default=QUERY_OUTPUT_DIR,
    show_default=True,
    type=str,
    help="Output directory for query results",
    callback=path_callback,
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
def run_dataset_query(
    ctx: click.Context,
    query_definition_file: Path,
    checkpoint_file: Path | None,
    output: Path,
    overwrite: bool,
    remote_path: str,
):
    """Run a query on a dsgrid dataset."""
    query = DatasetQueryModel.from_file(query_definition_file)
    _run_dataset_query(
        ctx,
        query,
        checkpoint_file,
        remote_path,
        output,
        overwrite,
    )


def _run_dataset_query(
    ctx: click.Context,
    query: DatasetQueryModel,
    checkpoint_file: Path | None,
    remote_path,
    output: Path,
    overwrite: bool,
) -> None:
    conn = DatabaseConnection(
        url=get_value_from_context(ctx, "url"),
    )
    scratch_dir = get_value_from_context(ctx, "scratch_dir")
    with RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    ) as registry_manager:
        fs_interface = make_filesystem_interface(output)
        submitter = DatasetQuerySubmitter(fs_interface.path(output))
        res = handle_dsgrid_exception(
            ctx,
            submitter.submit,
            query,
            registry_manager,
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
    with RegistryManager.load(
        conn,
        remote_path=remote_path,
        offline_mode=get_value_from_context(ctx, "offline"),
    ) as registry_manager:
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
def dataset():
    """Dataset group commands"""


@click.group()
def composite_dataset():
    """Composite dataset group commands"""


query.add_command(composite_dataset)
query.add_command(project)
query.add_command(dataset)
project.add_command(create_project_query)
project.add_command(validate_project_query)
project.add_command(run_project_query)
project.add_command(create_derived_dataset_config)
project.add_command(map_dataset)
dataset.add_command(create_dataset_query)
dataset.add_command(run_dataset_query)
composite_dataset.add_command(create_composite_dataset)
composite_dataset.add_command(query_composite_dataset)
