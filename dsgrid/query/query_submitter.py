import abc
import json
import logging
import shutil
from pathlib import Path
import copy
from zipfile import ZipFile

from chronify.utils.path_utils import check_overwrite
from semver import VersionInfo
from sqlalchemy import Connection

import dsgrid
from dsgrid.common import VALUE_COLUMN, BackendEngine
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_config import DimensionBaseConfig
from dsgrid.config.project_config import DatasetBaseDimensionNamesModel
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.dataset_expression_handler import (
    DatasetExpressionHandler,
    evaluate_expression,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.dataset.models import TableFormatType, PivotedTableFormatModel
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dataset.table_format_handler_factory import make_table_format_handler
from dsgrid.dimension.base_models import DimensionCategory, DimensionType
from dsgrid.dimension.dimension_filters import SubsetDimensionFilterModel
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidParameter, DSGInvalidQuery
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.query.dataset_mapping_plan import MapOperationCheckpoint
from dsgrid.query.query_context import QueryContext
from dsgrid.query.report_factory import make_report
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.functions import pivot
from dsgrid.spark.types import DataFrame
from dsgrid.project import Project
from dsgrid.utils.spark import (
    custom_time_zone,
    read_dataframe,
    try_read_dataframe,
    write_dataframe,
    write_dataframe_and_auto_partition,
    persist_table,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.files import delete_if_exists, compute_hash, load_data
from dsgrid.query.models import (
    DatasetQueryModel,
    ProjectQueryModel,
    ColumnType,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    DatasetMetadataModel,
    ProjectionDatasetModel,
    StandaloneDatasetModel,
)
from dsgrid.utils.dataset import (
    add_time_zone,
    convert_time_zone_with_chronify_spark_hive,
    convert_time_zone_with_chronify_spark_path,
    convert_time_zone_with_chronify_duckdb,
    convert_time_zone_by_column_with_chronify_spark_hive,
    convert_time_zone_by_column_with_chronify_spark_path,
    convert_time_zone_by_column_with_chronify_duckdb,
)
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.dimension.time import TimeZone
from dsgrid.exceptions import DSGInvalidOperation

logger = logging.getLogger(__name__)


class QuerySubmitterBase:
    """Handles query submission"""

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._cached_tables_dir().mkdir(exist_ok=True, parents=True)
        self._composite_datasets_dir().mkdir(exist_ok=True, parents=True)

        # TODO #186: This location will need more consideration.
        # We might want to store cached datasets in the spark-warehouse and let Spark manage it
        # for us. However, would we share them on the HPC? What happens on HPC walltime timeouts
        # where the tables are left in intermediate states?
        # This is even more of a problem on AWS.
        self._cached_project_mapped_datasets_dir().mkdir(exist_ok=True, parents=True)

    @abc.abstractmethod
    def submit(self, *args, **kwargs) -> DataFrame:
        """Submit a query for execution"""

    def _composite_datasets_dir(self):
        return self._output_dir / "composite_datasets"

    def _cached_tables_dir(self):
        """Directory for intermediate tables made up of multiple project-mapped datasets."""
        return self._output_dir / "cached_tables"

    def _cached_project_mapped_datasets_dir(self):
        """Directory for intermediate project-mapped datasets.
        Data could be filtered.
        """
        return self._output_dir / "cached_project_mapped_datasets"

    @staticmethod
    def metadata_filename(path: Path):
        return path / "metadata.json"

    @staticmethod
    def query_filename(path: Path):
        return path / "query.json5"

    @staticmethod
    def table_filename(path: Path):
        return path / "table.parquet"

    @staticmethod
    def _cached_table_filename(path: Path):
        return path / "table.parquet"


class ProjectBasedQuerySubmitter(QuerySubmitterBase):
    def __init__(self, project: Project, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._project = project

    @property
    def project(self):
        return self._project

    def _create_table_hash(self, context: QueryContext) -> tuple[str, str]:
        """Create a hash that can be used to identify whether the following sequence
        can be skipped based on a previous query:
          - Apply expression across all datasets in the query.
          - Apply filters.
          - Apply aggregations.

        Examples of changes that will invalidate the query:
          - Change to the project section of the query
          - Bump to project major version number
          - Change to a dataset version
          - Change to a project's dimension requirements for a dataset
          - Change to a dataset dimension mapping
        """
        assert isinstance(context.model, ProjectQueryModel) or isinstance(
            context.model, CreateCompositeDatasetQueryModel
        )
        data = {
            "project_major_version": VersionInfo.parse(self._project.config.model.version).major,
            "project_query": context.model.serialize_cached_content(),
            "datasets": [
                self._project.config.get_dataset(x.dataset_id).model_dump(mode="json")
                for x in context.model.project.dataset.source_datasets
            ],
        }
        text = json.dumps(data, indent=2)
        hash_value = compute_hash(text.encode())
        return text, hash_value

    def _try_read_cache(self, context: QueryContext):
        _, hash_value = self._create_table_hash(context)
        cached_dir = self._cached_tables_dir() / hash_value
        filename = self._cached_table_filename(cached_dir)
        df = try_read_dataframe(filename)
        if df is not None:
            logger.info("Load intermediate table from cache: %s", filename)
            metadata_file = self.metadata_filename(cached_dir)
            return df, DatasetMetadataModel.from_file(metadata_file)
        return None, None

    def _run_checks(self, model: ProjectQueryModel) -> DatasetBaseDimensionNamesModel:
        subsets = set(self.project.config.list_dimension_names(DimensionCategory.SUBSET))
        for agg in model.result.aggregations:
            for _, column in agg.iter_dimensions_to_keep():
                dimension_name = column.dimension_name
                if dimension_name in subsets:
                    subset_dim = self._project.config.get_dimension(dimension_name)
                    dim_type = subset_dim.model.dimension_type
                    supp_names = " ".join(
                        self._project.config.get_supplemental_dimension_to_name_mapping()[dim_type]
                    )
                    base_names = [
                        x.model.name
                        for x in self._project.config.list_base_dimensions(dimension_type=dim_type)
                    ]
                    msg = (
                        f"Subset dimensions cannot be used in aggregations: "
                        f"{dimension_name=}. Only base and supplemental dimensions are "
                        f"allowed. base={base_names} supplemental={supp_names}"
                    )
                    raise DSGInvalidQuery(msg)

        for report_inputs in model.result.reports:
            report = make_report(report_inputs.report_type)
            report.check_query(model)

        with self._project.dimension_mapping_manager.db.engine.connect() as conn:
            return self._check_datasets(model, conn)

    def _check_datasets(
        self, query_model: ProjectQueryModel, conn: Connection
    ) -> DatasetBaseDimensionNamesModel:
        base_dimension_names: DatasetBaseDimensionNamesModel | None = None
        dataset_ids: list[str] = []
        query_names: list[DatasetBaseDimensionNamesModel] = []
        for dataset in query_model.project.dataset.source_datasets:
            src_dataset_ids = dataset.list_source_dataset_ids()
            dataset_ids += src_dataset_ids
            if isinstance(dataset, StandaloneDatasetModel):
                query_names.append(
                    self._project.config.get_dataset_base_dimension_names(dataset.dataset_id)
                )
            elif isinstance(dataset, ProjectionDatasetModel):
                query_names += [
                    self._project.config.get_dataset_base_dimension_names(
                        dataset.initial_value_dataset_id
                    ),
                    self._project.config.get_dataset_base_dimension_names(
                        dataset.growth_rate_dataset_id
                    ),
                ]
            else:
                msg = f"Unhandled dataset type: {dataset=}"
                raise NotImplementedError(msg)

            for dataset_id in src_dataset_ids:
                dataset = self._project.load_dataset(dataset_id, conn=conn)
                plan = query_model.project.get_dataset_mapping_plan(dataset_id)
                if plan is None:
                    plan = dataset.handler.build_default_dataset_mapping_plan()
                    query_model.project.set_dataset_mapper(plan)
                else:
                    dataset.handler.check_dataset_mapping_plan(plan, self._project.config)

            for dataset_id, names in zip(dataset_ids, query_names):
                self._fix_legacy_base_dimension_names(names, dataset_id)
                if base_dimension_names is None:
                    base_dimension_names = names
                elif base_dimension_names != names:
                    msg = (
                        "Datasets in a query must have the same base dimension query names: "
                        f"{dataset=} {base_dimension_names} {names}"
                    )
                    raise DSGInvalidQuery(msg)

        assert base_dimension_names is not None
        return base_dimension_names

    def _fix_legacy_base_dimension_names(
        self, names: DatasetBaseDimensionNamesModel, dataset_id: str
    ) -> None:
        for dim_type in DimensionType:
            val = getattr(names, dim_type.value)
            if val is None:
                # This is a workaround for dsgrid projects created before the field
                # base_dimension_names was added to InputDatasetModel.
                dims = self._project.config.list_base_dimensions(dimension_type=dim_type)
                if len(dims) > 1:
                    msg = (
                        "The dataset's base_dimension_names value is not set and "
                        f"there are multiple base dimensions of type {dim_type} in the project. "
                        f"Please re-register the dataset with {dataset_id=}."
                    )
                    raise DSGInvalidDataset(msg)
                setattr(names, dim_type.value, dims[0].model.name)

    def _run_query(
        self,
        scratch_dir_context: ScratchDirContext,
        model: ProjectQueryModel,
        load_cached_table: bool,
        checkpoint_file: Path | None,
        persist_intermediate_table: bool,
        zip_file: bool = False,
        overwrite: bool = False,
    ):
        base_dimension_names = self._run_checks(model)
        checkpoint = self._check_checkpoint_file(checkpoint_file, model)
        context = QueryContext(
            model,
            base_dimension_names,
            scratch_dir_context=scratch_dir_context,
            checkpoint=checkpoint,
        )
        assert isinstance(context.model, ProjectQueryModel) or isinstance(
            context.model, CreateCompositeDatasetQueryModel
        )
        context.model.project.version = str(self._project.version)
        output_dir = self._output_dir / context.model.name
        if output_dir.exists() and not overwrite:
            msg = (
                f"output directory {self._output_dir} and query name={context.model.name} will "
                "overwrite an existing query results directory. "
                "Choose a different path or pass force=True."
            )
            raise DSGInvalidParameter(msg)

        df = None
        if load_cached_table:
            df, metadata = self._try_read_cache(context)
        if df is None:
            df_filenames = self._project.process_query(
                context, self._cached_project_mapped_datasets_dir()
            )
            df = self._postprocess_datasets(context, scratch_dir_context, df_filenames)
            is_cached = False
        else:
            context.metadata = metadata
            is_cached = True

        if context.model.result.aggregate_each_dataset:
            # This wouldn't save any time.
            persist_intermediate_table = False

        if persist_intermediate_table and not is_cached:
            df = self._persist_intermediate_result(context, df)

        if not context.model.result.aggregate_each_dataset:
            if context.model.result.dimension_filters:
                df = self._apply_filters(df, context)
            df = self._process_aggregations(df, context)

        repartition = not persist_intermediate_table
        table_filename = self._save_query_results(context, df, repartition, zip_file=zip_file)

        for report_inputs in context.model.result.reports:
            report = make_report(report_inputs.report_type)
            output_dir = self._output_dir / context.model.name
            report.generate(table_filename, output_dir, context, report_inputs.inputs)

        return df, context

    def _convert_time_zone(
        self,
        scratch_dir_context: ScratchDirContext,
        model: ProjectQueryModel,
        df,
        context,
        persist_intermediate_table: bool,
        zip_file: bool = False,
    ):
        time_dim = copy.deepcopy(self._project.config.get_base_time_dimension())
        if not isinstance(time_dim, DateTimeDimensionConfig):
            msg = f"Only DateTimeDimensionConfig allowed for time zone conversion. {time_dim.__class__.__name__}"
            raise DSGInvalidOperation(msg)
        time_cols = list(context.get_dimension_column_names(DimensionType.TIME))
        assert len(time_cols) == 1
        time_col = next(iter(time_cols))
        time_dim.model.time_column = time_col

        config = dsgrid.runtime_config
        if isinstance(model.result.time_zone, TimeZone):
            if time_dim.supports_chronify():
                match (config.backend_engine, config.use_hive_metastore):
                    case (BackendEngine.SPARK, True):
                        df = convert_time_zone_with_chronify_spark_hive(
                            df=df,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone=model.result.time_zone,
                            scratch_dir_context=scratch_dir_context,
                        )

                    case (BackendEngine.SPARK, False):
                        filename = persist_table(
                            df,
                            scratch_dir_context,
                            tag="project query before time mapping",
                        )
                        df = convert_time_zone_with_chronify_spark_path(
                            df=df,
                            filename=filename,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone=model.result.time_zone,
                            scratch_dir_context=scratch_dir_context,
                        )
                    case (BackendEngine.DUCKDB, _):
                        df = convert_time_zone_with_chronify_duckdb(
                            df=df,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone=model.result.time_zone,
                            scratch_dir_context=scratch_dir_context,
                        )

            else:
                msg = "time_dim must support Chronify"
                raise DSGInvalidParameter(msg)

        elif model.result.time_zone == "geography":
            if "time_zone" not in df.columns:
                geo_cols = list(context.get_dimension_column_names(DimensionType.GEOGRAPHY))
                assert len(geo_cols) == 1
                geo_col = next(iter(geo_cols))
                geo_dim = self._project.config.get_base_dimension(DimensionType.GEOGRAPHY)
                if model.result.replace_ids_with_names:
                    dim_key = "name"
                else:
                    dim_key = "id"
                df = add_time_zone(df, geo_dim, df_key=geo_col, dim_key=dim_key)

            if time_dim.supports_chronify():
                # use chronify
                match (config.backend_engine, config.use_hive_metastore):
                    case (BackendEngine.SPARK, True):
                        df = convert_time_zone_by_column_with_chronify_spark_hive(
                            df=df,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone_column="time_zone",
                            scratch_dir_context=scratch_dir_context,
                            wrap_time_allowed=False,
                        )
                    case (BackendEngine.SPARK, False):
                        filename = persist_table(
                            df,
                            scratch_dir_context,
                            tag="project query before time mapping",
                        )
                        df = convert_time_zone_by_column_with_chronify_spark_path(
                            df=df,
                            filename=filename,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone_column="time_zone",
                            scratch_dir_context=scratch_dir_context,
                            wrap_time_allowed=False,
                        )
                    case (BackendEngine.DUCKDB, _):
                        df = convert_time_zone_by_column_with_chronify_duckdb(
                            df=df,
                            value_column=VALUE_COLUMN,
                            from_time_dim=time_dim,
                            time_zone_column="time_zone",
                            scratch_dir_context=scratch_dir_context,
                            wrap_time_allowed=False,
                        )

            else:
                msg = "time_dim must support Chronify"
                raise DSGInvalidParameter(msg)
        else:
            msg = f"Unknown input {model.result.time_zone=}"
            raise DSGInvalidParameter(msg)

        repartition = not persist_intermediate_table
        table_filename = self._save_query_results(context, df, repartition, zip_file=zip_file)

        for report_inputs in context.model.result.reports:
            report = make_report(report_inputs.report_type)
            output_dir = self._output_dir / context.model.name
            report.generate(table_filename, output_dir, context, report_inputs.inputs)

        return df, context

    def _check_checkpoint_file(
        self, checkpoint_file: Path | None, model: ProjectQueryModel
    ) -> MapOperationCheckpoint | None:
        if checkpoint_file is None:
            return None

        checkpoint = MapOperationCheckpoint.from_file(checkpoint_file)
        confirmed_checkpoint = False
        for dataset in model.project.dataset.source_datasets:
            if dataset.get_dataset_id() == checkpoint.dataset_id:
                for plan in model.project.mapping_plans:
                    if plan.dataset_id == checkpoint.dataset_id:
                        if plan.compute_hash() == checkpoint.mapping_plan_hash:
                            confirmed_checkpoint = True
                        else:
                            msg = (
                                f"The hash of the mapping plan for dataset {checkpoint.dataset_id} "
                                "does not match the checkpoint file. Cannot use the checkpoint."
                            )
                            raise DSGInvalidParameter(msg)
        if not confirmed_checkpoint:
            msg = f"Checkpoint {checkpoint_file} does not match any dataset in the query."
            raise DSGInvalidParameter(msg)

        return checkpoint

    @track_timing(timer_stats_collector)
    def _persist_intermediate_result(self, context: QueryContext, df):
        text, hash_value = self._create_table_hash(context)
        cached_dir = self._cached_tables_dir() / hash_value
        if cached_dir.exists():
            shutil.rmtree(cached_dir)
        cached_dir.mkdir()
        filename = self._cached_table_filename(cached_dir)
        df = write_dataframe_and_auto_partition(df, filename)

        self.metadata_filename(cached_dir).write_text(
            context.metadata.model_dump_json(indent=2), encoding="utf-8"
        )
        self.query_filename(cached_dir).write_text(text, encoding="utf-8")
        logger.debug("Persisted intermediate table to %s", filename)
        return df

    def _postprocess_datasets(
        self,
        context: QueryContext,
        scratch_dir_context: ScratchDirContext,
        df_filenames: dict[str, Path],
    ) -> DataFrame:
        if context.model.result.aggregate_each_dataset:
            for dataset_id, path in df_filenames.items():
                df = read_dataframe(path)
                if context.model.result.dimension_filters:
                    df = self._apply_filters(df, context)
                df = self._process_aggregations(df, context, dataset_id=dataset_id)
                path = scratch_dir_context.get_temp_filename(suffix=".parquet")
                write_dataframe(df, path)
                df_filenames[dataset_id] = path

        # All dataset columns need to be in the same order.
        context.consolidate_dataset_metadata()
        datasets = self._convert_datasets(context, df_filenames)
        assert isinstance(context.model, ProjectQueryModel) or isinstance(
            context.model, CreateCompositeDatasetQueryModel
        )
        assert context.model.project.dataset.expression is not None
        return evaluate_expression(context.model.project.dataset.expression, datasets).df

    def _convert_datasets(self, context: QueryContext, filenames: dict[str, Path]):
        dim_columns, time_columns = self._get_dimension_columns(context)
        expected_columns = time_columns + dim_columns
        expected_columns.append(VALUE_COLUMN)

        datasets = {}
        for dataset_id, path in filenames.items():
            df = read_dataframe(path)
            unexpected = sorted(set(df.columns).difference(expected_columns))
            if unexpected:
                msg = f"Unexpected columns are present in {dataset_id=} {unexpected=}"
                raise Exception(msg)
            datasets[dataset_id] = DatasetExpressionHandler(
                df.select(*expected_columns), time_columns + dim_columns, [VALUE_COLUMN]
            )
        return datasets

    def _get_dimension_columns(self, context: QueryContext) -> tuple[list[str], list[str]]:
        match context.model.result.column_type:
            case ColumnType.DIMENSION_NAMES:
                dim_columns = context.get_all_dimension_column_names(exclude={DimensionType.TIME})
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case ColumnType.DIMENSION_TYPES:
                dim_columns = {x.value for x in DimensionType if x != DimensionType.TIME}
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case _:
                msg = f"BUG: unhandled {context.model.result.column_type=}"
                raise NotImplementedError(msg)

        return sorted(dim_columns), sorted(time_columns)

    def _process_aggregations(
        self, df: DataFrame, context: QueryContext, dataset_id: str | None = None
    ) -> DataFrame:
        handler = make_table_format_handler(
            TableFormatType.UNPIVOTED, self._project.config, dataset_id=dataset_id
        )
        df = handler.process_aggregations(df, context.model.result.aggregations, context)

        if context.model.result.replace_ids_with_names:
            df = handler.replace_ids_with_names(df)

        if context.model.result.sort_columns:
            df = df.sort(*context.model.result.sort_columns)

        if isinstance(context.model.result.table_format, PivotedTableFormatModel):
            df = _pivot_table(df, context)

        return df

    def _process_aggregations_and_save(
        self,
        df: DataFrame,
        context: QueryContext,
        repartition: bool,
        zip_file: bool = False,
    ) -> DataFrame:
        df = self._process_aggregations(df, context)

        self._save_query_results(context, df, repartition, zip_file=zip_file)
        return df

    def _apply_filters(self, df, context: QueryContext):
        for dim_filter in context.model.result.dimension_filters:
            column_names = context.get_dimension_column_names(dim_filter.dimension_type)
            if len(column_names) > 1:
                msg = f"Cannot filter {dim_filter} when there are multiple {column_names=}"
                raise NotImplementedError(msg)
            if isinstance(dim_filter, SubsetDimensionFilterModel):
                records = dim_filter.get_filtered_records_dataframe(
                    self.project.config.get_dimension
                )
                column = next(iter(column_names))
                df = df.join(
                    records.select("id"),
                    on=getattr(df, column) == getattr(records, "id"),
                ).drop("id")
            else:
                query_name = dim_filter.dimension_name
                if query_name not in df.columns:
                    # Consider catching this exception and still write to a file.
                    # It could mean writing a lot of data the user doesn't want.
                    msg = f"filter column {query_name} is not in the dataframe: {df.columns}"
                    raise DSGInvalidParameter(msg)
                df = dim_filter.apply_filter(df, column=query_name)
        return df

    @track_timing(timer_stats_collector)
    def _save_query_results(
        self,
        context: QueryContext,
        df,
        repartition,
        aggregation_name=None,
        zip_file=False,
    ):
        output_dir = self._output_dir / context.model.name
        output_dir.mkdir(exist_ok=True)
        if aggregation_name is not None:
            output_dir /= aggregation_name
            output_dir.mkdir(exist_ok=True)
        filename = output_dir / f"table.{context.model.result.output_format}"
        self._save_result(context, df, filename, repartition)
        if zip_file:
            zip_name = Path(str(output_dir) + ".zip")
            with ZipFile(zip_name, "w") as zipf:
                for path in output_dir.rglob("*"):
                    zipf.write(path)
        return filename

    def _save_result(self, context: QueryContext, df, filename, repartition):
        output_dir = filename.parent
        suffix = filename.suffix
        if suffix == ".csv":
            df.toPandas().to_csv(filename, header=True, index=False)
        elif suffix == ".parquet":
            if repartition:
                df = write_dataframe_and_auto_partition(df, filename)
            else:
                delete_if_exists(filename)
                write_dataframe(df, filename, overwrite=True)
        else:
            msg = f"Unsupported output_format={suffix}"
            raise NotImplementedError(msg)
        self.query_filename(output_dir).write_text(context.model.serialize_with_hash()[1])
        self.metadata_filename(output_dir).write_text(context.metadata.model_dump_json(indent=2))
        logger.info("Wrote query=%s output table to %s", context.model.name, filename)


class ProjectQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        model: ProjectQueryModel,
        scratch_dir: Path | None = None,
        checkpoint_file: Path | None = None,
        persist_intermediate_table: bool = True,
        load_cached_table: bool = True,
        zip_file: bool = False,
        overwrite: bool = False,
    ) -> DataFrame:
        """Submits a project query to consolidate datasets and produce result tables.

        Parameters
        ----------
        model : ProjectQueryResultModel
        checkpoint_file : bool, optional
            Optional checkpoint file from which to resume the operation.
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.
        zip_file : bool, optional
            Create a zip file with all output files.
        overwrite : bool
            If True, overwrite any existing output directory.

        Returns
        -------
        pyspark.sql.DataFrame

        Raises
        ------
        DSGInvalidParameter
            Raised if the model defines a project version
        DSGInvalidQuery
            Raised if the query is invalid
        """
        tz = self._project.config.get_base_time_dimension().get_time_zone()
        assert tz is not None, "Project base time dimension must have a time zone"

        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            # Ensure that queries that aggregate time reflect the project's time zone instead
            # of the local computer.
            # If any other settings get customized here, handle them in restart_spark()
            # as well. This change won't persist Spark session restarts.
            with custom_time_zone(tz.tz_name):
                df, context = self._run_query(
                    scratch_dir_context,
                    model,
                    load_cached_table,
                    checkpoint_file=checkpoint_file,
                    persist_intermediate_table=persist_intermediate_table,
                    zip_file=zip_file,
                    overwrite=overwrite,
                )
            if model.result.time_zone:
                df, context = self._convert_time_zone(
                    scratch_dir_context,
                    model,
                    df,
                    context,
                    persist_intermediate_table=persist_intermediate_table,
                    zip_file=zip_file,
                )
            context.finalize()

            return df


class CompositeDatasetQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a composite dataset."""

    @track_timing(timer_stats_collector)
    def create_dataset(
        self,
        model: CreateCompositeDatasetQueryModel,
        scratch_dir: Path | None = None,
        persist_intermediate_table=False,
        load_cached_table=True,
        force=False,
    ):
        """Create a composite dataset from a project.

        Parameters
        ----------
        model : CreateCompositeDatasetQueryModel
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.
        force : bool
            If True, overwrite any existing output directory.

        """
        tz = self._project.config.get_base_time_dimension().get_time_zone()
        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            # Ensure that queries that aggregate time reflect the project's time zone instead
            # of the local computer.
            # If any other settings get customized here, handle them in restart_spark()
            # as well. This change won't persist Spark session restarts.
            with custom_time_zone(tz.tz_name):  # type: ignore
                df, context = self._run_query(
                    scratch_dir_context,
                    model,
                    load_cached_table,
                    None,
                    persist_intermediate_table,
                    overwrite=force,
                )
                self._save_composite_dataset(context, df, not persist_intermediate_table)
                context.finalize()

    @track_timing(timer_stats_collector)
    def submit(
        self,
        query: CompositeDatasetQueryModel,
        scratch_dir: Path | None = None,
    ) -> DataFrame:
        """Submit a query to an composite dataset and produce result tables.

        Parameters
        ----------
        query : CompositeDatasetQueryModel
        scratch_dir : Path | None
        """
        tz = self._project.config.get_base_time_dimension().get_time_zone()
        assert tz is not None
        scratch_dir = DsgridRuntimeConfig.load().get_scratch_dir()
        # orig_query = self._load_composite_dataset_query(query.dataset_id)
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            df, metadata = self._read_dataset(query.dataset_id)
            base_dimension_names = DatasetBaseDimensionNamesModel()
            for dim_type in DimensionType:
                field = dim_type.value
                query_names = getattr(metadata.dimensions, field)
                if len(query_names) > 1:
                    msg = (
                        "Composite datasets must have a single query name for each dimension: "
                        f"{dim_type} {query_names}"
                    )
                    raise DSGInvalidQuery(msg)
                setattr(base_dimension_names, field, query_names[0].dimension_name)
            context = QueryContext(query, base_dimension_names, scratch_dir_context)
            context.metadata = metadata
            # Refer to the comment in ProjectQuerySubmitter.submit for an explanation or if
            # you add a new customization.
            with custom_time_zone(tz.tz_name):  # type: ignore
                df = self._process_aggregations_and_save(df, context, repartition=False)
            context.finalize()
            return df

    def _load_composite_dataset_query(self, dataset_id):
        filename = self._composite_datasets_dir() / dataset_id / "query.json5"
        return CreateCompositeDatasetQueryModel.from_file(filename)

    def _read_dataset(self, dataset_id) -> tuple[DataFrame, DatasetMetadataModel]:
        filename = self._composite_datasets_dir() / dataset_id / "table.parquet"
        if not filename.exists():
            msg = f"There is no composite dataset with dataset_id={dataset_id}"
            raise DSGInvalidParameter(msg)
        metadata_file = self.metadata_filename(self._composite_datasets_dir() / dataset_id)
        return (
            read_dataframe(filename),
            DatasetMetadataModel(**load_data(metadata_file)),
        )

    @track_timing(timer_stats_collector)
    def _save_composite_dataset(self, context: QueryContext, df, repartition):
        output_dir = self._composite_datasets_dir() / context.model.dataset_id
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "table.parquet"
        self._save_result(context, df, filename, repartition)
        self.metadata_filename(output_dir).write_text(context.metadata.model_dump_json(indent=2))


class DatasetQuerySubmitter(QuerySubmitterBase):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        query: DatasetQueryModel,
        mgr: RegistryManager,
        scratch_dir: Path | None = None,
        checkpoint_file: Path | None = None,
        overwrite: bool = False,
    ) -> DataFrame:
        """Submits a dataset query to produce a result table."""
        if not query.to_dimension_references:
            msg = "A dataset query must specify at least one dimension to map."
            raise DSGInvalidQuery(msg)

        dataset_config = mgr.dataset_manager.get_by_id(query.dataset_id)
        to_dimension_mapping_refs, dims = self._build_mappings(query, dataset_config, mgr)
        handler = make_dataset_schema_handler(
            conn=None,
            config=dataset_config,
            dimension_mgr=mgr.dimension_manager,
            dimension_mapping_mgr=mgr.dimension_mapping_manager,
            store=mgr.dataset_manager.store,
            mapping_references=to_dimension_mapping_refs,
        )

        base_dim_names = DatasetBaseDimensionNamesModel()
        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        time_dim = dims.get(DimensionType.TIME) or dataset_config.get_time_dimension()
        time_zone = None if time_dim is None else time_dim.get_time_zone()
        checkpoint = self._check_checkpoint_file(checkpoint_file, query)
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            context = QueryContext(
                query, base_dim_names, scratch_dir_context, checkpoint=checkpoint
            )
            output_dir = self._query_output_dir(context)
            check_overwrite(output_dir, overwrite)
            args = (context, handler)
            kwargs = {"time_dimension": dims.get(DimensionType.TIME)}
            if time_dim is not None and time_zone is not None:
                with custom_time_zone(time_zone.tz_name):
                    df = self._run_query(*args, **kwargs)
            else:
                df = self._run_query(*args, **kwargs)
        return df

    def _build_mappings(
        self, query: DatasetQueryModel, config: DatasetConfig, mgr: RegistryManager
    ) -> tuple[list[DimensionMappingReferenceModel], dict[DimensionType, DimensionBaseConfig]]:
        config = mgr.dataset_manager.get_by_id(query.dataset_id)
        to_dimension_mapping_refs: list[DimensionMappingReferenceModel] = []
        mapped_dimension_types: set[DimensionType] = set()
        dimensions: dict[DimensionType, DimensionBaseConfig] = {}
        with mgr.dimension_mapping_manager.db.engine.connect() as conn:
            graph = mgr.dimension_mapping_manager.build_graph(conn=conn)
            for to_dim_ref in query.to_dimension_references:
                to_dim = mgr.dimension_manager.get_by_id(
                    to_dim_ref.dimension_id, version=to_dim_ref.version
                )
                if to_dim.model.dimension_type in mapped_dimension_types:
                    msg = f"A dataset query cannot map multiple dimensions of the same type: {to_dim.model.dimension_type}"
                    raise DSGInvalidQuery(msg)
                dataset_dim = config.get_dimension(to_dim.model.dimension_type)
                assert dataset_dim is not None
                if to_dim.model.dimension_id == dataset_dim.model.dimension_id:
                    if to_dim.model.version != dataset_dim.model.version:
                        msg = (
                            f"A to_dimension_reference cannot point to a different version of a "
                            f"dataset's dimension dimension: dataset version = {dataset_dim.model.version}, "
                            f"dimension version = {to_dim.model.version}"
                        )
                        raise DSGInvalidQuery(msg)
                    # No mapping is required.
                    continue
                if to_dim.model.dimension_type != DimensionType.TIME:
                    refs = mgr.dimension_mapping_manager.list_mappings_between_dimensions(
                        graph,
                        dataset_dim.model.dimension_id,
                        to_dim.model.dimension_id,
                    )
                    to_dimension_mapping_refs += refs
                mapped_dimension_types.add(to_dim.model.dimension_type)
                dimensions[to_dim.model.dimension_type] = to_dim
        return to_dimension_mapping_refs, dimensions

    def _check_checkpoint_file(
        self, checkpoint_file: Path | None, query: DatasetQueryModel
    ) -> MapOperationCheckpoint | None:
        if checkpoint_file is None:
            return None

        if query.mapping_plan is None:
            msg = f"Query {query.name} does not have a mapping plan. A checkpoint file cannot be used."
            raise DSGInvalidQuery(msg)

        checkpoint = MapOperationCheckpoint.from_file(checkpoint_file)
        if query.dataset_id != checkpoint.dataset_id:
            msg = (
                f"The dataset_id in the checkpoint file {checkpoint.dataset_id} does not match "
                f"the query dataset_id {query.dataset_id}."
            )
            raise DSGInvalidQuery(msg)

        if query.mapping_plan.compute_hash() != checkpoint.mapping_plan_hash:
            msg = (
                f"The hash of the mapping plan for dataset {checkpoint.dataset_id} "
                "does not match the checkpoint file. Cannot use the checkpoint."
            )
            raise DSGInvalidParameter(msg)

        return checkpoint

    def _run_query(
        self,
        context: QueryContext,
        handler: DatasetSchemaHandlerBase,
        time_dimension: TimeDimensionBaseConfig | None,
    ) -> DataFrame:
        df = handler.make_mapped_dataframe(context, time_dimension=time_dimension)
        df = self._postprocess(context, df)
        self._save_results(context, df)
        return df

    def _postprocess(self, context: QueryContext, df: DataFrame) -> DataFrame:
        if context.model.result.sort_columns:
            df = df.sort(*context.model.result.sort_columns)

        if isinstance(context.model.result.table_format, PivotedTableFormatModel):
            df = _pivot_table(df, context)

        return df

    def _query_output_dir(self, context: QueryContext) -> Path:
        return self._output_dir / context.model.name

    @track_timing(timer_stats_collector)
    def _save_results(self, context: QueryContext, df) -> Path:
        output_dir = self._query_output_dir(context)
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / f"table.{context.model.result.output_format}"
        suffix = filename.suffix
        if suffix == ".csv":
            df.toPandas().to_csv(filename, header=True, index=False)
        elif suffix == ".parquet":
            df = write_dataframe_and_auto_partition(df, filename)
        else:
            msg = f"Unsupported output_format={suffix}"
            raise NotImplementedError(msg)

        logger.info("Wrote query=%s output table to %s", context.model.name, filename)
        return filename


def _pivot_table(df: DataFrame, context: QueryContext):
    pivoted_column = context.convert_to_pivoted()
    return pivot(df, pivoted_column, VALUE_COLUMN)
