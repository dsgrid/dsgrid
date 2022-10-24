"""Interface to a dsgrid project."""

import logging

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.query.query_context import QueryContext

from dsgrid.utils.timing import timer_stats_collector, track_timing


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
        VersionInfo

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
        """Loads a dataset. Creates a view for each of its tables.

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
        dataset.create_views()
        self._datasets[dataset_id] = dataset
        return dataset

    def unload_dataset(self, dataset_id):
        """Loads a dataset. Creates a view for each of its tables.

        Parameters
        ----------
        dataset_id : str

        """
        dataset = self.get_dataset(dataset_id)
        dataset.delete_views()

    def _iter_datasets(self):
        for dataset in self.config.model.datasets:
            yield dataset

    def list_datasets(self):
        return [x.dataset_id for x in self._iter_datasets()]

    @track_timing(timer_stats_collector)
    def process_query(self, context: QueryContext):
        self._build_filtered_record_ids_by_dimension_type(context)

        dfs = []
        for dataset_id in context.model.project.dataset_ids:
            logger.info("Start processing query for dataset_id=%s", dataset_id)
            dataset = self.get_dataset(dataset_id)
            dfs.append(dataset.get_dataframe(context, self._config))
            logger.info("Finished processing query for dataset_id=%s", dataset_id)

        if not dfs:
            logger.warning("No data matched %s", context.name)
            return None

        # All dataset columns need to be in the same order.
        context.consolidate_dataset_metadata()
        # TODO: Change this when we support long format.
        pivoted_columns = context.get_pivoted_columns()
        pivoted_columns_sorted = sorted(pivoted_columns)
        dim_columns = context.get_all_dimension_query_names()
        time_columns = context.get_dimension_query_names(DimensionType.TIME)
        dim_columns -= time_columns
        for col in dim_columns:
            dimension_type = self._config.get_dimension(col).model.dimension_type
            if dimension_type == context.get_pivoted_dimension_type():
                dim_columns.remove(col)
                break
        dim_columns = sorted(dim_columns)
        time_columns = sorted(time_columns)
        expected_columns = time_columns + pivoted_columns_sorted + dim_columns
        for i, dataset_id in enumerate(context.model.project.dataset_ids):
            remaining = sorted(set(dfs[i].columns).difference(expected_columns))
            final_columns = expected_columns + remaining
            missing = pivoted_columns.difference(dfs[i].columns)
            for column in missing:
                dfs[i] = dfs[i].withColumn(column, F.lit(None).cast(DoubleType()))
            dfs[i] = dfs[i].select(*final_columns)

        main_df = dfs[0]
        if len(dfs) > 1:
            for df in dfs[1:]:
                if df.columns != main_df.columns:
                    raise Exception(
                        f"BUG: dataset columns must match. main={main_df.columns} new={df.columns} "
                        f"diff={set(main_df.columns).symmetric_difference(df.columns)}"
                    )
                main_df = main_df.union(df)

        return main_df

    def _build_filtered_record_ids_by_dimension_type(self, context: QueryContext):
        record_ids = {}
        base_query_names = self._config.get_base_dimension_query_names()

        for dim_filter in context.model.project.dimension_filters:
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
            record_ids[dim_type] = df

        for dimension_type, record_ids in record_ids.items():
            context.set_record_ids_by_dimension_type(dimension_type, record_ids)
