"""Interface to a dsgrid project."""

import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from dsgrid.dataset.dataset import Dataset
from dsgrid.dataset.dataset_expression_handler import DatasetExpressionHandler, evaluate_expression
from dsgrid.dataset.growth_rates import apply_growth_rate_123
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery, DSGValueNotRegistered
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import (
    StandaloneDatasetModel,
    ExponentialGrowthDatasetModel,
    TableFormatType,
    ColumnType,
)

from dsgrid.utils.spark import (
    read_dataframe,
    try_read_dataframe,
    restart_spark_with_custom_conf,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing, Timer


logger = logging.getLogger(__name__)


class Project:
    """Interface to a dsgrid project."""

    def __init__(self, config, version, dataset_configs, dimension_mgr, dimension_mapping_mgr):
        self._spark = SparkSession.getActiveSession()
        self._config = config
        self._version = version
        self._dataset_configs = dataset_configs
        self._datasets = {}
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr

    @property
    def config(self):
        """Returns the ProjectConfig."""
        return self._config

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_mgr

    @property
    def version(self):
        """Return the version of the project.

        Returns
        -------
        str

        """
        return self._version

    def get_dataset(self, dataset_id):
        """Returns a Dataset. Calls load_dataset if it hasn't already been loaded.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id in self._datasets:
            dataset = self._datasets[dataset_id]
        else:
            dataset = self.load_dataset(dataset_id)
        return dataset

    def load_dataset(self, dataset_id):
        """Loads a dataset.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id not in self._dataset_configs:
            raise DSGValueNotRegistered(
                f"dataset_id={dataset_id} is not registered in the project"
            )
        config = self._dataset_configs[dataset_id]
        input_dataset = self._config.get_dataset(dataset_id)
        dataset = Dataset.load(
            config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            mapping_references=input_dataset.mapping_references,
            project_time_dim=self._config.get_base_dimension(DimensionType.TIME),
        )
        self._datasets[dataset_id] = dataset
        return dataset

    def unload_dataset(self, dataset_id):
        """Unloads a dataset.

        Parameters
        ----------
        dataset_id : str

        """
        self._datasets.pop(dataset_id, None)

    def _iter_datasets(self):
        for dataset in self.config.model.datasets:
            yield dataset

    def list_datasets(self):
        return [x.dataset_id for x in self._iter_datasets()]

    @track_timing(timer_stats_collector)
    def process_query(self, context: QueryContext, cached_datasets_dir: Path):
        self._build_filtered_record_ids_by_dimension_type(context)

        # Note: Store DataFrame filenames instead of objects because the SparkSession will get
        # restarted for each dataset. The Spark DataFrame keeps a reference to the session that
        # created it, and so that reference will be invalid.
        df_filenames = {}
        for dataset in context.model.project.dataset.source_datasets:
            if isinstance(dataset, StandaloneDatasetModel):
                path = self._process_dataset(context, cached_datasets_dir, dataset.dataset_id)
            elif isinstance(dataset, ExponentialGrowthDatasetModel):
                path = self._process_exponential_growth_dataset(
                    context, cached_datasets_dir, dataset
                )
            else:
                raise NotImplementedError(f"Unsupported type: {type(dataset)}")
            df_filenames[dataset.dataset_id] = path

        if not df_filenames:
            logger.warning("No data matched %s", context.model.name)
            return None

        # All dataset columns need to be in the same order.
        context.consolidate_dataset_metadata()
        # TODO #202: Change this when we support long format.
        pivoted_columns = context.get_pivoted_columns()
        pivoted_columns_sorted = sorted(pivoted_columns)

        match context.model.result.column_type:
            case ColumnType.DIMENSION_QUERY_NAMES:
                dim_columns = context.get_all_dimension_query_names()
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case ColumnType.DIMENSION_TYPES:
                dim_columns = {x.value for x in DimensionType if x != DimensionType.TIME}
                time_columns = context.get_dimension_column_names(DimensionType.TIME)
            case _:
                raise NotImplementedError(f"BUG: unhandled {context.model.result.column_type=}")
        dim_columns -= time_columns
        for col in dim_columns:
            match context.model.result.column_type:
                case ColumnType.DIMENSION_QUERY_NAMES:
                    dimension_type = self._config.get_dimension(col).model.dimension_type
                case ColumnType.DIMENSION_TYPES:
                    dimension_type = DimensionType.from_column(col)
                case _:
                    raise NotImplementedError(
                        f"BUG: unhandled {context.model.result.column_type=}"
                    )
            if dimension_type == context.get_pivoted_dimension_type():
                dim_columns.remove(col)
                break
        dim_columns = sorted(dim_columns)
        time_columns = sorted(time_columns)
        expected_columns = time_columns + pivoted_columns_sorted + dim_columns

        datasets = {}
        for dataset_id, path in df_filenames.items():
            df = read_dataframe(path)
            unexpected = sorted(set(df.columns).difference(expected_columns))
            if unexpected:
                raise Exception(f"Unexpected columns are present in {dataset_id=} {unexpected=}")
            for column in pivoted_columns.difference(df.columns):
                df = df.withColumn(column, F.lit(None).cast(DoubleType()))
            datasets[dataset_id] = DatasetExpressionHandler(
                df.select(*expected_columns), time_columns + dim_columns, pivoted_columns_sorted
            )

        return evaluate_expression(context.model.project.dataset.expression, datasets).df

    def _build_filtered_record_ids_by_dimension_type(self, context: QueryContext):
        record_ids = {}
        base_query_names = self._config.get_base_dimension_query_names()

        for dim_filter in context.model.project.dataset.params.dimension_filters:
            dim_type = dim_filter.dimension_type
            query_name = dim_filter.dimension_query_name
            df = self._config.get_dimension_records(query_name)
            df = dim_filter.apply_filter(df).select("id")
            if query_name not in base_query_names:
                mapping_records = self._config.get_base_to_supplemental_mapping_records(query_name)
                df = (
                    mapping_records.join(df, on=mapping_records.to_id == df.id)
                    .selectExpr("from_id AS id")
                    .distinct()
                )

            if dim_type in record_ids:
                df = record_ids[dim_type].intersect(df)
            if df.rdd.isEmpty():
                raise DSGInvalidQuery(f"Query filter produced empty records: {dim_filter}")
            record_ids[dim_type] = df

        for dimension_type, ids in record_ids.items():
            context.set_record_ids_by_dimension_type(dimension_type, ids)

    def _process_dataset(
        self, context: QueryContext, cached_datasets_dir: Path, dataset_id: str
    ) -> Path:
        """Return a Path to the created DataFrame. Does not return a DataFrame object because
        the SparkSession will be restarted.

        """
        logger.info("Start processing query for dataset_id=%s", dataset_id)
        project_version = f"{context.model.project.project_id}__{context.model.project.version}"
        model_hash, text = context.model.project.dataset.params.serialize()
        hash_dir = cached_datasets_dir / project_version / model_hash
        if not hash_dir.exists():
            hash_dir.mkdir(parents=True)
            model_file = hash_dir / "model.json"
            model_file.write_text(text)
        cached_dataset_path = hash_dir / (dataset_id + ".parquet")
        metadata_file = cached_dataset_path.with_suffix(".json5")
        if try_read_dataframe(cached_dataset_path) is None:
            # An alternative solution is to call custom_spark_conf instead.
            # That changes some settings without restarting the SparkSession.
            # Results were not as good with that solution.
            # Observations on queries with comstock and resstock showed that Spark
            # used many fewer executors on the second query. That was with a standalone
            # cluster on Eagle with dynamic allocation enabled.
            # We don't understand why that is the case. It may not be an issue with YARN as
            # the cluster manager on AWS.
            # Queries on standalone clusters will be easier to debug if we restart the session
            # for each big job.
            with restart_spark_with_custom_conf(
                conf=context.model.project.get_spark_conf(dataset_id),
                force=True,
            ):
                logger.info("Build project-mapped dataset %s", dataset_id)
                # Call load_dataset instead of get_dataset because the latter won't be valid here
                # after the SparkSession restart.
                dataset = self.load_dataset(dataset_id)
                with Timer(timer_stats_collector, "build_project_mapped_dataset"):
                    df = dataset.make_project_dataframe_from_query(context, self._config)
                    write_dataframe_and_auto_partition(df, cached_dataset_path)
                    context.serialize_dataset_metadata_to_file(dataset.dataset_id, metadata_file)
        else:
            assert metadata_file.exists(), metadata_file
            context.set_dataset_metadata_from_file(dataset_id, metadata_file)
            logger.info("Use cached project-mapped dataset %s", dataset_id)

        logger.info("Finished processing query for dataset_id=%s", dataset_id)
        return cached_dataset_path

    def _process_exponential_growth_dataset(
        self,
        context: QueryContext,
        cached_datasets_dir: Path,
        dataset: ExponentialGrowthDatasetModel,
    ) -> Path:
        logger.info("Apply exponential growth for dataset_id=%s", dataset.initial_value_dataset_id)
        project_version = f"{context.model.project.project_id}__{context.model.project.version}"
        model_hash, text = context.model.project.dataset.params.serialize()
        hash_dir = cached_datasets_dir / project_version / model_hash
        if not hash_dir.exists():
            hash_dir.mkdir(parents=True)
            model_file = hash_dir / "model.json"
            model_file.write_text(text)
        cached_dataset_path = hash_dir / (dataset.dataset_id + ".parquet")
        metadata_file = cached_dataset_path.with_suffix(".json5")
        if try_read_dataframe(cached_dataset_path) is None:
            self._build_exponential_growth_dataset(
                context,
                cached_datasets_dir,
                dataset,
                cached_dataset_path,
                metadata_file,
            )
        else:
            assert metadata_file.exists(), metadata_file
            context.set_dataset_metadata_from_file(dataset.dataset_id, metadata_file)
            logger.info("Use cached project-mapped dataset %s", dataset.dataset_id)

        return cached_dataset_path

    @track_timing(timer_stats_collector)
    def _build_exponential_growth_dataset(
        self, context, cached_datasets_dir, dataset, dataset_path, metadata_file
    ):
        def get_myear_column(dataset_id):
            match context.model.result.column_type:
                case ColumnType.DIMENSION_TYPES:
                    return DimensionType.MODEL_YEAR.value
                case ColumnType.DIMENSION_QUERY_NAMES:
                    pass
                case _:
                    raise NotImplementedError(
                        f"BUG: unhandled {context.model.result.column_type=}"
                    )
            names = list(
                context.get_dimension_column_names(DimensionType.MODEL_YEAR, dataset_id=dataset_id)
            )
            assert len(names) == 1, f"{dataset_id=} {names=}"
            return names[0]

        iv_path = self._process_dataset(
            context, cached_datasets_dir, dataset.initial_value_dataset_id
        )
        gr_path = self._process_dataset(
            context, cached_datasets_dir, dataset.growth_rate_dataset_id
        )
        pivoted_columns = context.get_pivoted_columns(dataset_id=dataset.initial_value_dataset_id)
        pivoted_columns_gr = context.get_pivoted_columns(dataset_id=dataset.growth_rate_dataset_id)
        if pivoted_columns != pivoted_columns_gr:
            raise Exception(
                f"BUG: Mismatch in pivoted columns: "
                f"{pivoted_columns.symmetric_difference(pivoted_columns_gr)}"
            )

        model_year_column = get_myear_column(dataset.initial_value_dataset_id)
        model_year_column_gr = get_myear_column(dataset.growth_rate_dataset_id)
        if model_year_column != model_year_column_gr:
            raise Exception(
                "BUG: initial_value and growth rate datasets have different model_year columns: "
                f"{model_year_column=} {model_year_column_gr=}"
            )
        match context.model.result.column_type:
            case ColumnType.DIMENSION_QUERY_NAMES:
                time_columns = context.get_dimension_column_names(
                    DimensionType.TIME, dataset_id=dataset.initial_value_dataset_id
                )
            case ColumnType.DIMENSION_TYPES:
                dset = self.get_dataset(dataset.initial_value_dataset_id)
                time_dim = dset.config.get_dimension(DimensionType.TIME)
                time_columns = set(time_dim.get_load_data_time_columns())
            case _:
                raise NotImplementedError(f"BUG: unhandled {context.model.result.column_type=}")
        with restart_spark_with_custom_conf(
            conf=context.model.project.get_spark_conf(dataset.dataset_id),
            force=True,
        ):
            logger.info("Build projection dataset %s", dataset.dataset_id)
            iv_df = read_dataframe(iv_path)
            gr_df = read_dataframe(gr_path)
            if dataset.construction_method == "formula123":
                df = apply_growth_rate_123(
                    dataset, iv_df, gr_df, time_columns, model_year_column, pivoted_columns
                )
            else:
                raise NotImplementedError(f"BUG: Unsupported {dataset.construction_method=}")
            df = write_dataframe_and_auto_partition(df, dataset_path)
            context.set_dataset_metadata(
                dataset.dataset_id,
                pivoted_columns,
                context.model.result.column_type,
                context.get_pivoted_dimension_type(dataset_id=dataset.initial_value_dataset_id),
                TableFormatType.PIVOTED,
                self._config,
            )
            context.serialize_dataset_metadata_to_file(dataset.dataset_id, metadata_file)
