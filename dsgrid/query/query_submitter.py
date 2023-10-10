import abc
import logging
import shutil
from pathlib import Path
from zipfile import ZipFile

from dsgrid.dataset.pivoted_table import PivotedTableHandler
from dsgrid.dimension.base_models import DimensionCategory
from dsgrid.dimension.dimension_filters import SubsetDimensionFilterModel
from dsgrid.exceptions import DSGInvalidParameter, DSGInvalidQuery
from dsgrid.utils.spark import (
    get_unique_values,
    read_dataframe,
    try_read_dataframe,
    write_dataframe_and_auto_partition,
)
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
from .report_peak_load import PeakLoadReport


logger = logging.getLogger(__name__)


class QuerySubmitterBase:
    """Handles query submission"""

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._cached_tables_dir().mkdir(exist_ok=True, parents=True)
        self._composite_datasets_dir().mkdir(exist_ok=True, parents=True)

        # TODO #186: This location will need more consideration.
        # We might want to store cached datasets in the spark-warehouse and let Spark manage it
        # for us. However, would we share them on Eagle? What happens on Eagle walltime timeouts
        # where the tables are left in intermediate states?
        # This is even more of a problem on AWS.
        self._cached_project_mapped_datasets_dir().mkdir(exist_ok=True, parents=True)

    @abc.abstractmethod
    def submit(self, *args, **kwargs):
        """Submit a query for execution"""

    def _composite_datasets_dir(self):
        return self._output_dir / "composite_datasets"

    def _cached_tables_dir(self):
        return self._output_dir / "cached_tables"

    def _cached_project_mapped_datasets_dir(self):
        return self._output_dir / "cached_project_mapped_datasets"

    @staticmethod
    def metadata_filename(path: Path):
        return path / "metadata.json"

    @staticmethod
    def query_filename(path: Path):
        return path / "query.json"

    @staticmethod
    def table_filename(path: Path):
        return path / "table.parquet"

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

        self.metadata_filename(cached_dir).write_text(context.metadata.json(indent=2))
        self.query_filename(cached_dir).write_text(text)
        logger.info("Persisted intermediate table to %s", filename)
        return df

    def _try_read_cache(self, context: QueryContext):
        hash, _ = context.model.serialize_cached_content()
        cached_dir = self._cached_tables_dir() / hash
        filename = self._cached_table_filename(cached_dir)
        df = try_read_dataframe(filename)
        if df is not None:
            logger.info("Load intermediate table from cache: %s", filename)
            metadata_file = self.metadata_filename(cached_dir)
            return df, DatasetMetadataModel.from_file(metadata_file)
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
            raise NotImplementedError(f"Unsupported {context.get_table_format_type()=}")
        return handler

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

    def _run_query(
        self,
        model,
        load_cached_table,
        persist_intermediate_table,
        zip_file=False,
        force=False,
    ):
        self._run_checks(model)
        context = QueryContext(model)
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
            df = self._project.process_query(context, self._cached_project_mapped_datasets_dir())
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
        if context.model.result.dimension_filters:
            df = self._apply_filters(df, context)

        df = handler.process_aggregations(df, context.model.result.aggregations, context)

        if context.model.result.replace_ids_with_names:
            df = handler.replace_ids_with_names(df)

        if context.model.result.sort_columns:
            df = df.sort(*context.model.result.sort_columns)

        repartition = not persist_intermediate_table
        table_filename = self._save_query_results(context, df, repartition, zip_file=zip_file)

        for report in context.model.result.reports:
            output_dir = self._output_dir / context.model.name
            if report.report_type == ReportType.PEAK_LOAD:
                peak_load = PeakLoadReport()
                peak_load.generate(table_filename, output_dir, context, report.inputs)

        return df, context

    def _apply_filters(self, df, context: QueryContext):
        for dim_filter in context.model.result.dimension_filters:
            if isinstance(dim_filter, SubsetDimensionFilterModel):
                records = dim_filter.get_filtered_records_dataframe(
                    self.project.config.get_dimension
                )
                if dim_filter.dimension_type == context.get_pivoted_dimension_type():
                    df = self._drop_filtered_pivoted_columns(df, context, records)
                else:
                    column = dim_filter.dimension_type.value
                    df = df.join(records.select("id"), on=df[column] == records["id"]).drop("id")
            else:
                if dim_filter.dimension_type == context.get_pivoted_dimension_type():
                    dim_records = self.project.config.get_dimension_records(
                        dim_filter.dimension_query_name
                    )
                    records = dim_filter.apply_filter(dim_records, column="id")
                    df = self._drop_filtered_pivoted_columns(df, context, records)
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

    @staticmethod
    def _drop_filtered_pivoted_columns(df, context: QueryContext, records):
        record_ids = get_unique_values(records, "id")
        drop_columns = context.get_pivoted_columns() - record_ids
        df = df.drop(*drop_columns)
        pivoted_columns = (record_ids - drop_columns).intersection(df.columns)
        context.set_pivoted_columns(pivoted_columns)
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

    def _save_result(self, context, df, filename, repartition):
        output_dir = filename.parent
        suffix = filename.suffix
        if suffix == ".csv":
            # TODO #207: Some users may want us to use pandas because Spark makes a csv directory.
            df.write.mode("overwrite").csv(str(filename), header=True)
        elif suffix == ".parquet":
            if repartition:
                df = write_dataframe_and_auto_partition(df, filename)
            else:
                df.write.mode("overwrite").parquet(str(filename))
        else:
            raise NotImplementedError(f"Unsupported output_format={suffix}")
        self.query_filename(output_dir).write_text(context.model.serialize()[1])
        self.metadata_filename(output_dir).write_text(context.metadata.json(indent=2))
        logger.info("Wrote query=%s output table to %s", context.model.name, filename)


class ProjectQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        model: ProjectQueryModel,
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
        return self._run_query(
            model, load_cached_table, persist_intermediate_table, zip_file=zip_file, force=force
        )[0]


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
        df, context = self._run_query(
            model, load_cached_table, persist_intermediate_table, force=force
        )
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
        metadata_file = self.metadata_filename(self._composite_datasets_dir() / dataset_id)
        return (read_dataframe(filename), DatasetMetadataModel(**load_data(metadata_file)))

    @track_timing(timer_stats_collector)
    def _save_composite_dataset(self, context: QueryContext, df, repartition):
        output_dir = self._composite_datasets_dir() / context.model.dataset_id
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "table.parquet"
        self._save_result(context, df, filename, repartition)
        self.metadata_filename(output_dir).write_text(context.metadata.json(indent=2))
