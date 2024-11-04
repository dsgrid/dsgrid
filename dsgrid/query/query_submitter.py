import abc
import json
import logging
import shutil
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

from pyspark.sql import DataFrame
from semver import VersionInfo

from dsgrid.common import VALUE_COLUMN
from dsgrid.dataset.dataset_expression_handler import DatasetExpressionHandler, evaluate_expression
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.dataset.models import TableFormatType, PivotedTableFormatModel
from dsgrid.dataset.table_format_handler_factory import make_table_format_handler
from dsgrid.dimension.base_models import DimensionCategory, DimensionType
from dsgrid.dimension.dimension_filters import SubsetDimensionFilterModel
from dsgrid.exceptions import DSGInvalidParameter, DSGInvalidQuery
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.query.query_context import QueryContext
from dsgrid.query.report_factory import make_report
from dsgrid.project import Project
from dsgrid.utils.spark import (
    custom_spark_conf,
    read_dataframe,
    try_read_dataframe,
    write_dataframe,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.files import compute_hash, load_data
from dsgrid.query.models import (
    ProjectQueryModel,
    ColumnType,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    DatasetMetadataModel,
)


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
    def submit(self, *args, **kwargs):
        """Submit a query for execution"""

    def _composite_datasets_dir(self):
        return self._output_dir / "composite_datasets"

    def _cached_tables_dir(self):
        """Directory for intermediate tables made up of multiple project-mapped datasets."""
        return self._output_dir / "cached_tables"

    def _cached_project_mapped_datasets_dir(self):
        """Directory for intermediate project-mapped datasets."""
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

    def _run_checks(self, model):
        subsets = set(self.project.config.list_dimension_query_names(DimensionCategory.SUBSET))
        for agg in model.result.aggregations:
            for _, column in agg.iter_dimensions_to_keep():
                dimension_query_name = column.dimension_query_name
                if dimension_query_name in subsets:
                    dim_type = self._project.config.get_dimension(
                        dimension_query_name
                    ).model.dimension_type
                    base_name = self._project.config.get_base_dimension(
                        dim_type
                    ).model.dimension_query_name
                    supp_names = " ".join(
                        self._project.config.get_supplemental_dimension_to_query_name_mapping()[
                            dim_type
                        ]
                    )
                    raise DSGInvalidQuery(
                        f"Subset dimensions cannot be used in aggregations: "
                        f"{dimension_query_name=}. Only base and supplemental dimensions are "
                        f"allowed. base={base_name} supplemental={supp_names}"
                    )

        for report_inputs in model.result.reports:
            report = make_report(report_inputs.report_type)
            report.check_query(model)

    def _run_query(
        self,
        scratch_dir_context: ScratchDirContext,
        model,
        load_cached_table,
        persist_intermediate_table,
        zip_file=False,
        force=False,
    ):
        self._run_checks(model)
        context = QueryContext(model, scratch_dir_context=scratch_dir_context)
        context.model.project.version = str(self._project.version)
        output_dir = self._output_dir / context.model.name
        if output_dir.exists() and not force:
            raise DSGInvalidParameter(
                f"output directory {self._output_dir} and query name={context.model.name} will "
                "overwrite an existing query results directory. "
                "Choose a different path or pass force=True."
            )

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
        logger.info("Persisted intermediate table to %s", filename)
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
                raise Exception(f"Unexpected columns are present in {dataset_id=} {unexpected=}")
            datasets[dataset_id] = DatasetExpressionHandler(
                df.select(*expected_columns), time_columns + dim_columns, [VALUE_COLUMN]
            )
        return datasets

    def _get_dimension_columns(self, context: QueryContext) -> tuple[list[str], list[str]]:
        match context.model.result.column_type:
            case ColumnType.DIMENSION_QUERY_NAMES:
                dim_columns = context.get_all_dimension_column_names(exclude={DimensionType.TIME})
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case ColumnType.DIMENSION_TYPES:
                dim_columns = {x.value for x in DimensionType if x != DimensionType.TIME}
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case _:
                raise NotImplementedError(f"BUG: unhandled {context.model.result.column_type=}")

        return sorted(dim_columns), sorted(time_columns)

    def _process_aggregations(
        self, df: DataFrame, context: QueryContext, dataset_id: Optional[str] = None
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
        self, df: DataFrame, context: QueryContext, repartition: bool, zip_file: bool = False
    ) -> Path:
        df = self._process_aggregations(df, context)

        table_filename = self._save_query_results(context, df, repartition, zip_file=zip_file)
        return table_filename

    def _apply_filters(self, df, context: QueryContext):
        for dim_filter in context.model.result.dimension_filters:
            column_names = context.get_dimension_column_names(dim_filter.dimension_type)
            if len(column_names) > 1:
                raise NotImplementedError(
                    f"Cannot filter {dim_filter} when there are multiple {column_names=}"
                )
            if isinstance(dim_filter, SubsetDimensionFilterModel):
                records = dim_filter.get_filtered_records_dataframe(
                    self.project.config.get_dimension
                )
                column = next(iter(column_names))
                df = df.join(records.select("id"), on=df[column] == records["id"]).drop("id")
            else:
                query_name = dim_filter.dimension_query_name
                if query_name not in df.columns:
                    # Consider catching this exception and still write to a file.
                    # It could mean writing a lot of data the user doesn't want.
                    raise DSGInvalidParameter(
                        f"filter column {query_name} is not in the dataframe: {df.columns}"
                    )
                df = dim_filter.apply_filter(df, column=query_name)
        return df

    @track_timing(timer_stats_collector)
    def _save_query_results(
        self, context: QueryContext, df, repartition, aggregation_name=None, zip_file=False
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
                df.write.mode("overwrite").parquet(str(filename))
        else:
            raise NotImplementedError(f"Unsupported output_format={suffix}")
        self.query_filename(output_dir).write_text(context.model.serialize()[1])
        self.metadata_filename(output_dir).write_text(context.metadata.model_dump_json(indent=2))
        logger.info("Wrote query=%s output table to %s", context.model.name, filename)


class ProjectQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        model: ProjectQueryModel,
        scratch_dir: Optional[Path] = None,
        persist_intermediate_table=True,
        load_cached_table=True,
        zip_file=False,
        force=False,
    ):
        """Submits a project query to consolidate datasets and produce result tables.

        Parameters
        ----------
        model : ProjectQueryResultModel
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.
        zip_file : bool, optional
            Create a zip file with all output files.
        force : bool
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
        tz = self._project.config.get_base_dimension(DimensionType.TIME).get_time_zone()
        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            # Ensure that queries that aggregate time reflect the project's time zone instead
            # of the local computer.
            # If any other settings get customized here, handle them in restart_spark()
            # as well. This change won't persist Spark session restarts.
            conf = {} if tz is None else {"spark.sql.session.timeZone": tz.tz_name}
            with custom_spark_conf(conf):
                return self._run_query(
                    scratch_dir_context,
                    model,
                    load_cached_table,
                    persist_intermediate_table=persist_intermediate_table,
                    zip_file=zip_file,
                    force=force,
                )[0]


class CompositeDatasetQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a composite dataset."""

    @track_timing(timer_stats_collector)
    def create_dataset(
        self,
        model: CreateCompositeDatasetQueryModel,
        scratch_dir: Optional[Path] = None,
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
        tz = self._project.config.get_base_dimension(DimensionType.TIME).get_time_zone()
        scratch_dir = scratch_dir or DsgridRuntimeConfig.load().get_scratch_dir()
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            # Refer to the comment in ProjectQuerySubmitter.submit for an explanation or if
            # you add a new customization.
            conf = {} if tz is None else {"spark.sql.session.timeZone": tz.tz_name}
            with custom_spark_conf(conf):
                df, context = self._run_query(
                    scratch_dir_context,
                    model,
                    load_cached_table,
                    persist_intermediate_table,
                    force=force,
                )
                self._save_composite_dataset(context, df, not persist_intermediate_table)

    @track_timing(timer_stats_collector)
    def submit(
        self,
        query: CompositeDatasetQueryModel,
        scratch_dir: Optional[Path] = None,
    ):
        """Submit a query to an composite dataset and produce result tables.

        Parameters
        ----------
        query : CompositeDatasetQueryModel
        scratch_dir : Optional[Path]
        """
        tz = self._project.config.get_base_dimension(DimensionType.TIME).get_time_zone()
        scratch_dir = DsgridRuntimeConfig.load().get_scratch_dir()
        # orig_query = self._load_composite_dataset_query(query.dataset_id)
        with ScratchDirContext(scratch_dir) as scratch_dir_context:
            context = QueryContext(query, scratch_dir_context)
            df, context.metadata = self._read_dataset(query.dataset_id)
            # Refer to the comment in ProjectQuerySubmitter.submit for an explanation or if
            # you add a new customization.
            conf = {} if tz is None else {"spark.sql.session.timeZone": tz.tz_name}
            with custom_spark_conf(conf):
                self._process_aggregations_and_save(df, context, repartition=False)

    def _load_composite_dataset_query(self, dataset_id):
        filename = self._composite_datasets_dir() / dataset_id / "query.json5"
        return CreateCompositeDatasetQueryModel.from_file(filename)

    def _read_dataset(self, dataset_id):
        filename = self._composite_datasets_dir() / dataset_id / "table.parquet"
        if not filename.exists():
            raise DSGInvalidParameter(
                f"There is no composite dataset with dataset_id={dataset_id}"
            )
        metadata_file = self.metadata_filename(self._composite_datasets_dir() / dataset_id)
        return (read_dataframe(filename), DatasetMetadataModel(**load_data(metadata_file)))

    @track_timing(timer_stats_collector)
    def _save_composite_dataset(self, context: QueryContext, df, repartition):
        output_dir = self._composite_datasets_dir() / context.model.dataset_id
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "table.parquet"
        self._save_result(context, df, filename, repartition)
        self.metadata_filename(output_dir).write_text(context.metadata.model_dump_json(indent=2))


def _pivot_table(df: DataFrame, context: QueryContext):
    pivoted_column = context.convert_to_pivoted()
    ids = [x for x in df.columns if x not in {pivoted_column, VALUE_COLUMN}]
    return df.groupBy(*ids).pivot(pivoted_column).sum(VALUE_COLUMN)
