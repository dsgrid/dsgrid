import abc
import logging
import shutil
from pathlib import Path

from dsgrid.dataset.pivoted_table import PivotedTableHandler
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.spark import read_dataframe, write_dataframe_and_auto_partition
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.files import load_data
from .models import (
    ProjectQueryModel,
    CreateCompositeDatasetQueryModel,
    CompositeDatasetQueryModel,
    ReportType,
    DatasetMetadataModel,
    TableFormatType,
)
from .query_context import QueryContext
from .peak_load_report import PeakLoadReport


logger = logging.getLogger(__name__)


class QuerySubmitterBase:
    """Handles query submission"""

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._cached_tables_dir().mkdir(exist_ok=True, parents=True)
        self._composite_datasets_dir().mkdir(exist_ok=True, parents=True)

    @abc.abstractmethod
    def submit(self, *args, **kwargs):
        """Submit a query for execution"""

    def _composite_datasets_dir(self):
        return self._output_dir / "composite_datasets"

    def _cached_tables_dir(self):
        return self._output_dir / "cached_tables"

    @staticmethod
    def _metadata_filename(path: Path):
        return path / "metadata.json"

    @staticmethod
    def _query_filename(path: Path):
        return path / "query.json"

    @staticmethod
    def _cached_table_filename(path: Path):
        return path / "table.parquet"

    @track_timing(timer_stats_collector)
    def _persist_intermediate_result(self, context: QueryContext, df):
        hash, text = context.model.serialize_cached_content()
        cached_dir = self._cached_tables_dir() / hash
        if cached_dir.exists():
            shutil.rmtree(cached_dir)
        cached_dir.mkdir()
        filename = self._cached_table_filename(cached_dir)
        df = write_dataframe_and_auto_partition(df, filename)

        self._metadata_filename(cached_dir).write_text(context.metadata.json(indent=2))
        self._query_filename(cached_dir).write_text(text)
        logger.info("Persisted intermediate table to %s", filename)
        return df

    def _try_read_cache(self, context: QueryContext):
        hash, _ = context.model.serialize_cached_content()
        cached_dir = self._cached_tables_dir() / hash
        if cached_dir.exists():
            filename = self._cached_table_filename(cached_dir)
            logger.info("Load intermediate table from cache: %s", filename)
            metadata_file = self._metadata_filename(cached_dir)
            return read_dataframe(filename), DatasetMetadataModel(**load_data(metadata_file))
        return None, None


class ProjectBasedQuerySubmitter(QuerySubmitterBase):
    def __init__(self, project, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._project = project

    @property
    def project(self):
        return self._project

    def _make_table_handler(self, context: QueryContext):
        if context.get_table_format_type() == TableFormatType.PIVOTED:
            handler = PivotedTableHandler(self._project.config)
        else:
            raise Exception(f"Unsupported table format {context.get_table_format_type()}")
        return handler

    def _run_query(
        self,
        model: CreateCompositeDatasetQueryModel,
        load_cached_table,
        persist_intermediate_table,
    ):
        context = QueryContext(model)
        context.model.project.version = str(self._project.version)

        df = None
        if load_cached_table:
            df, metadata = self._try_read_cache(context)
        if df is None:
            df = self._project.process_query(context)
            is_cached = False
        else:
            context.metadata = metadata
            is_cached = True

        if persist_intermediate_table and not is_cached:
            df = self._persist_intermediate_result(context, df)

        handler = self._make_table_handler(context)
        if context.model.result.supplemental_columns:
            df = handler.add_columns(
                df,
                context.model.result.supplemental_columns,
                context,
                False,
            )
        df = handler.process_aggregations(df, context.model.result.aggregations, context)

        if context.model.result.replace_ids_with_names:
            df = handler.replace_ids_with_names(df)

        repartition = not persist_intermediate_table
        table_filename = self._save_query_results(context, df, repartition)

        for report in context.model.result.reports:
            output_dir = self._output_dir / context.model.name
            if report.report_type == ReportType.PEAK_LOAD:
                peak_load = PeakLoadReport()
                peak_load.generate(table_filename, output_dir, context, report.inputs)

        return df, context

    @track_timing(timer_stats_collector)
    def _save_query_results(self, context: QueryContext, df, repartition, aggregation_name=None):
        output_dir = self._output_dir / context.model.name
        output_dir.mkdir(exist_ok=True)
        if aggregation_name is not None:
            output_dir /= aggregation_name
            output_dir.mkdir(exist_ok=True)
        filename = output_dir / f"table.{context.model.result.output_format}"
        self._save_result(context, df, filename, repartition)
        return filename

    def _save_result(self, context, df, filename, repartition):
        output_dir = filename.parent
        suffix = filename.suffix
        if suffix == ".csv":
            # TODO minor: Some users may want us to use pandas because Spark makes a csv directory.
            df.write.mode("overwrite").csv(str(filename), header=True)
        elif suffix == ".parquet":
            if repartition:
                df = write_dataframe_and_auto_partition(df, filename)
            else:
                df.write.mode("overwrite").parquet(str(filename))
        else:
            raise Exception(f"Unsupported output_format={suffix}")
        self._query_filename(output_dir).write_text(context.model.serialize()[1])
        self._metadata_filename(output_dir).write_text(context.metadata.json(indent=2))
        logger.info("Wrote query=%s output table to %s", context.model.name, filename)


class ProjectQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        model: ProjectQueryModel,
        persist_intermediate_table=True,
        load_cached_table=True,
    ):
        """Submits a project query to consolidate datasets and produce result tables.

        Parameters
        ----------
        model : ProjectQueryResultModel
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.

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
        return self._run_query(model, load_cached_table, persist_intermediate_table)[0]


class CompositeDatasetQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a composite dataset."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @track_timing(timer_stats_collector)
    def create_dataset(
        self,
        model: CreateCompositeDatasetQueryModel,
        persist_intermediate_table=False,
        load_cached_table=True,
    ):
        """Create a composite dataset from a project.

        Parameters
        ----------
        model : CreateCompositeDatasetQueryModel
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.
        """
        df, context = self._run_query(model, load_cached_table, persist_intermediate_table)
        self._save_composite_dataset(context, df, not persist_intermediate_table)

    @track_timing(timer_stats_collector)
    def submit(
        self,
        query: CompositeDatasetQueryModel,
    ):
        """Submit a query to an composite dataset and produce result tables.

        Parameters
        ----------
        query : CompositeDatasetQueryModel
        """
        # orig_query = self._load_composite_dataset_query(query.dataset_id)
        context = QueryContext(query)
        df, context.metadata = self._read_dataset(query.dataset_id)
        handler = self._make_table_handler(context)

        repartition = False
        if context.model.result.aggregations:
            df = handler.process_aggregations(df, context.model.result.aggregations, context)

        if context.model.result.replace_ids_with_names:
            df = handler.replace_ids_with_names(df)
        self._save_query_results(context, df, repartition)

    def _load_composite_dataset_query(self, dataset_id):
        filename = self._composite_datasets_dir() / dataset_id / "query.json"
        return CreateCompositeDatasetQueryModel.from_file(filename)

    def _read_dataset(self, dataset_id):
        filename = self._composite_datasets_dir() / dataset_id / "table.parquet"
        if not filename.exists():
            raise DSGInvalidParameter(
                f"There is no composite dataset with dataset_id={dataset_id}"
            )
        metadata_file = self._metadata_filename(self._composite_datasets_dir() / dataset_id)
        return (read_dataframe(filename), DatasetMetadataModel(**load_data(metadata_file)))

    @track_timing(timer_stats_collector)
    def _save_composite_dataset(self, context: QueryContext, df, repartition):
        output_dir = self._composite_datasets_dir() / context.model.dataset_id
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "table.parquet"
        self._save_result(context, df, filename, repartition)
        self._metadata_filename(output_dir).write_text(context.metadata.json(indent=2))
