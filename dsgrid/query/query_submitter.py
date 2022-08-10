import abc
import logging
import shutil
from pathlib import Path

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.dataset import map_and_reduce_pivot_dimension
from dsgrid.utils.spark import read_dataframe, write_dataframe_and_auto_partition
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.files import dump_data, load_data
from .models import (
    ChainedAggregationModel,
    ProjectQueryResultModel,
    DerivedDatasetQueryModel,
    DerivedDatasetQueryResultModel,
    QueryResultModel,
)
from .query_context import QueryContext


logger = logging.getLogger(__name__)


class QuerySubmitterBase:
    """Handles query submission"""

    def __init__(self, output_dir: Path):
        self._output_dir = output_dir
        self._cached_tables_dir().mkdir(exist_ok=True, parents=True)
        self._derived_datasets_dir().mkdir(exist_ok=True, parents=True)

    @abc.abstractmethod
    def submit(self, *args, **kwargs):
        """Submit a query for execution"""

    @track_timing(timer_stats_collector)
    def _persist_intermediate_result(self, context: QueryContext, df):
        hash, text = context.model.serialize_cached_content()
        cached_dir = self._cached_tables_dir() / hash
        if cached_dir.exists():
            shutil.rmtree(cached_dir)
        cached_dir.mkdir()
        filename = self._cached_table_filename(cached_dir)
        df = write_dataframe_and_auto_partition(df, filename)
        # TODO: These columns should probably be embedded inside the Parquet or have the
        # dimension types encoded, such as 'metric__electricity.'
        metadata = {
            "metric_columns": context.metric_columns,
            "required_dimension_mappings": context.required_dimension_mappings,
        }
        dump_data(metadata, self._metadata_filename(cached_dir), indent=2)
        self._query_filename(cached_dir).write_text(text)
        logger.info("Persisted intermediate table to %s", filename)
        return df

    def _try_read_cache(self, context: QueryContext):
        hash, _ = context.model.serialize_cached_content()
        cached_dir = self._cached_tables_dir() / hash
        if cached_dir.exists():
            filename = self._cached_table_filename(cached_dir)
            logger.info("Load intermediate table from cache: %s", filename)
            metadata = load_data(self._metadata_filename(cached_dir))
            return (
                read_dataframe(filename),
                metadata["metric_columns"],
                metadata["required_dimension_mappings"],
            )
        return None, None, None


class ProjectBasedQuerySubmitter(QuerySubmitterBase):
    def __init__(self, project, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._project = project

    def _process_metric_reductions(self, context, df):
        assert (
            len(context.model.result.metric_reductions) <= 1
        ), "More than one metric aggregation is not supported yet"
        cols = set(context.metric_columns)  # TODO DT: same as pivot columns?
        context.metric_columns = None
        # TODO: need to merge dataframes on each loop
        processed_first = False
        for aggregation in context.model.result.metric_reductions:
            dimension_query_name = aggregation.dimension_query_name
            records = self._project.config.get_base_to_supplemental_mapping_records(
                dimension_query_name
            )
            df, columns = map_and_reduce_pivot_dimension(
                df,
                records,
                cols,
                aggregation.operation,
                rename=True,
            )
            if processed_first:
                assert (
                    columns == context.metric_columns
                ), f"columns={columns} metric_columns={sorted(context.metric_columns)}"
            else:
                processed_first = True
                context.metric_columns = columns

        return df

    def _replace_ids_with_names(self, context, df):
        if not context.model.result.replace_ids_with_names:
            return df

        orig = df
        all_query_names = set(self._project.config.get_dimension_query_names())
        for dimension_query_name in set(df.columns).intersection(all_query_names):
            assert not {"id", "name"}.intersection(df.columns), df.columns
            records = self._project.config.get_dimension_records(dimension_query_name).select(
                "id", "name"
            )
            df = (
                df.join(records, on=df[dimension_query_name] == records.id)
                .drop("id", dimension_query_name)
                .withColumnRenamed("name", dimension_query_name)
            )
        assert df.count() == orig.count(), f"counts changed {df.count()} {orig.count()}"
        return df

    def _run_query(self, model: QueryResultModel, load_cached_table, persist_intermediate_table):
        context = QueryContext(model)
        if context.model.project.version is not None:
            raise DSGInvalidParameter("Query version cannot be set by the user")
        context.model.project.version = str(self._project.version)

        df = None
        if load_cached_table:
            df, context.metric_columns, required_dimension_mappings = self._try_read_cache(context)
            if required_dimension_mappings is not None:
                context.required_dimension_mappings = required_dimension_mappings
        if df is None:
            df = self._project.process_query(context)
            is_cached = False
        else:
            is_cached = True
        if persist_intermediate_table and not is_cached:
            df = self._persist_intermediate_result(context, df)

        if context.model.result.supplemental_columns:
            df = self._add_columns(df, context.model.result.supplemental_columns)

        return df, context

    def _perform_aggregation(self, context: QueryContext, df, model):
        df = self._add_columns(df, context.get_required_dimension_mappings(model.name))
        agg_columns = [model.aggregation_function(x) for x in context.metric_columns]
        assert agg_columns
        logger.info("groupBy=%s agg=%s", model.group_by_columns, agg_columns)
        return df.groupBy(model.group_by_columns).agg(*agg_columns)

    @track_timing(timer_stats_collector)
    def _save_query_results(self, context: QueryContext, df, repartition, aggregation_name=None):
        output_dir = self._output_dir / context.model.name
        output_dir.mkdir(exist_ok=True)
        if aggregation_name is not None:
            output_dir /= aggregation_name
            output_dir.mkdir(exist_ok=True)
        filename = output_dir / f"table.{context.model.result.output_format}"
        self._save_result(context, df, filename, repartition)

    def _save_result(self, context, df, filename, repartition):
        output_dir = filename.parent
        suffix = filename.suffix
        if suffix == ".csv":
            # Some users may want us to use pandas because Spark makes a csv directory.
            df.write.mode("overwrite").csv(str(filename), header=True)
        elif suffix == ".parquet":
            if repartition:
                df = write_dataframe_and_auto_partition(df, filename)
            else:
                df.write.mode("overwrite").parquet(str(filename))
        else:
            raise Exception(f"Unsupported output_format={suffix}")
        self._query_filename(output_dir).write_text(context.model.serialize()[1])
        logger.info("Wrote query=%s output table to %s", context.model.name, filename)

    def _add_columns(self, df, query_names):
        for dimension_query_name in query_names:
            dim = self._project.config.get_dimension(dimension_query_name)
            base_dim = self._project.config.get_base_dimension(dim.model.dimension_type)
            base_query_name = base_dim.model.dimension_query_name
            records = self._project.config.get_base_to_supplemental_mapping_records(
                dimension_query_name
            ).drop("from_fraction")
            df = (
                df.join(records, on=df[base_query_name] == records.from_id)
                .withColumnRenamed("to_id", dimension_query_name)
                .drop("from_id")
            )

        # TODO: Probably need to order base_dim and supp_dim columns sequentially, followed by
        # metric.
        return df

    def _cached_tables_dir(self):
        return self._output_dir / "cached_tables"

    def _derived_datasets_dir(self):
        return self._output_dir / "derived_datasets"

    @staticmethod
    def _metadata_filename(path: Path):
        return path / "metadata.json"

    @staticmethod
    def _query_filename(path: Path):
        return path / "query.json"

    @staticmethod
    def _cached_table_filename(path: Path):
        return path / "table.parquet"


class ProjectQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a project."""

    @track_timing(timer_stats_collector)
    def submit(
        self,
        model: ProjectQueryResultModel,
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

        Raises
        ------
        DSGInvalidParameter
            Raised if the model defines a project version
        """
        repartition = not persist_intermediate_table
        df, context = self._run_query(model, load_cached_table, persist_intermediate_table)
        if context.model.result.aggregations:
            for aggregation in context.model.result.aggregations:
                if isinstance(aggregation, ChainedAggregationModel):
                    # TODO: column name evolution is not handled.
                    assert False, "ChainedAggregationModel are currently unsupported"
                    for agg in aggregation.aggregations:
                        df = self._perform_aggregation(context, df, agg)
                        if agg.name is not None:
                            self._save_query_results(
                                context, df, repartition, aggregation_name=agg.name
                            )
                else:
                    df2 = self._perform_aggregation(context, df, aggregation)
                    df2 = self._replace_ids_with_names(context, df2)
                    self._save_query_results(
                        context, df2, repartition, aggregation_name=aggregation.name
                    )
        else:
            df = self._replace_ids_with_names(context, df)
            self._save_query_results(context, df, repartition)


class DerivedDatasetQuerySubmitter(ProjectBasedQuerySubmitter):
    """Submits queries for a derived dataset."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @track_timing(timer_stats_collector)
    def create_dataset(
        self,
        model: DerivedDatasetQueryModel,
        persist_intermediate_table=False,
        load_cached_table=True,
        register_dataset=False,
    ):
        """Create a derived dataset from a project.

        Parameters
        ----------
        model : DerivedDatasetQueryModel
        persist_intermediate_table : bool, optional
            Persist the intermediate consolidated table.
        load_cached_table : bool, optional
            Load a cached consolidated table if the query matches an existing query.
        """
        assert not register_dataset
        df, context = self._run_query(model, load_cached_table, persist_intermediate_table)
        self._save_derived_dataset(context, df, not persist_intermediate_table)

    @track_timing(timer_stats_collector)
    def submit(
        self,
        query: DerivedDatasetQueryResultModel,
    ):
        """Submit a query to a derived dataset and produce result tables.

        Parameters
        ----------
        query : DerivedDatasetQueryModel
        project : Project
        """
        # orig_query = self._load_derived_dataset_query(query.dataset_id)
        context = QueryContext(query)
        df, context.metric_columns, context.required_dimension_mappings = self._read_dataset(
            query.dataset_id
        )

        if query.result.metric_reductions:
            df = self._process_metric_reductions(context, df)

        repartition = False
        if context.model.result.aggregations:
            for aggregation in context.model.result.aggregations:
                if isinstance(aggregation, ChainedAggregationModel):
                    # TODO: column name evolution is not handled.
                    assert False, "ChainedAggregationModel are currently unsupported"
                    for agg in aggregation.aggregations:
                        df = self._perform_aggregation(context, df, agg)
                        if agg.name is not None:
                            self._save_query_results(
                                context, df, repartition, aggregation_name=agg.name
                            )
                else:
                    for dimension_query_name in set(aggregation.group_by_columns).difference(
                        df.columns
                    ):
                        context.add_required_dimension_mapping(
                            aggregation.name, dimension_query_name
                        )
                    df2 = self._perform_aggregation(context, df, aggregation)
                    df2 = self._replace_ids_with_names(context, df2)
                    self._save_query_results(
                        context, df2, repartition, aggregation_name=aggregation.name
                    )
        else:
            df = self._replace_ids_with_names(context, df)
            self._save_query_results(context, df, repartition)

    def _load_derived_dataset_query(self, dataset_id):
        filename = self._derived_datasets_dir() / dataset_id / "query.json"
        return DerivedDatasetQueryModel.from_file(filename)

    def _read_dataset(self, dataset_id):
        filename = self._derived_datasets_dir() / dataset_id / "dataset.parquet"
        if not filename.exists():
            raise DSGInvalidParameter(f"There is no derived dataset with dataset_id={dataset_id}")
        metadata = self._read_metadata(dataset_id)
        return (
            read_dataframe(filename),
            metadata["metric_columns"],
            metadata["required_dimension_mappings"],
        )

    def _read_metadata(self, dataset_id):
        return load_data(self._derived_datasets_dir() / dataset_id / "metadata.json")

    @track_timing(timer_stats_collector)
    def _save_derived_dataset(self, context: QueryContext, df, repartition):
        output_dir = self._derived_datasets_dir() / context.model.dataset_id
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "dataset.parquet"
        self._save_result(context, df, filename, repartition)
        metadata = {
            "metric_columns": context.metric_columns,
            "required_dimension_mappings": context.required_dimension_mappings,
        }
        dump_data(metadata, self._metadata_filename(output_dir))
