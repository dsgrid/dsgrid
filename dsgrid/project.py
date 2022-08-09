"""Interface to a dsgrid project."""

import logging

from pyspark.sql import SparkSession

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidParameter, DSGValueNotRegistered
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import ChainedAggregationModel
from dsgrid.registry.registry_manager import RegistryManager, get_registry_path
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

    @classmethod
    def load(
        cls,
        project_id,
        registry_path=None,
        version=None,
        offline_mode=False,
        remote_path=REMOTE_REGISTRY,
        use_remote_data=None,
    ):
        """Load a project from the registry.
        Parameters
        ----------
        project_id : str
        registry_path : str | None
        version : str | None
            Use the latest if not specified.
        offline_mode : bool
            If True, don't sync with remote registry
        remote_path: str, optional
            path of the remote registry; default is REMOTE_REGISTRY
        use_remote_data: bool, None
            If set, use load data tables from remote_path. If not set, auto-determine what to do
            based on HPC or AWS EMR environment variables.
        """
        registry_path = get_registry_path(registry_path=registry_path)
        manager = RegistryManager.load(
            registry_path,
            remote_path=remote_path,
            use_remote_data=use_remote_data,
            offline_mode=offline_mode,
        )
        dataset_manager = manager.dataset_manager
        project_manager = manager.project_manager
        config = project_manager.get_by_id(project_id, version=version)
        if version is None:
            version = project_manager.get_current_version(project_id)

        dataset_configs = {}
        for dataset_id in config.list_registered_dataset_ids():
            dataset_config = dataset_manager.get_by_id(dataset_id)
            dataset_configs[dataset_id] = dataset_config

        return cls(
            config,
            version,
            dataset_configs,
            manager.dimension_manager,
            manager.dimension_mapping_manager,
        )

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
        for dataset in self.config.datasets:
            yield dataset

    def list_datasets(self):
        return [x.dataset_id for x in self._iter_datasets()]

    @track_timing(timer_stats_collector)
    def process_query(self, context: QueryContext):
        self._handle_supplemental_query_names(context)
        self._build_filtered_record_ids_by_dimension_type(context)

        dfs = []
        for dataset_id in context.model.project.dataset_ids:
            logger.info("Start processing query for dataset_id=%s", dataset_id)
            dataset = self.get_dataset(dataset_id)
            dfs.append(dataset.get_dataframe(context))
            logger.info("Finished processing query for dataset_id=%s", dataset_id)

        if not dfs:
            logger.warning("No data matched %s", context.name)
            return None

        main_df = dfs[0]
        if len(dfs) > 1:
            for df in dfs[1:]:
                if df.columns != main_df.columns:
                    raise Exception(
                        f"BUG: dataset columns must match. main={main_df.columns} new={df.columns}"
                    )
                main_df = main_df.union(df)

        return self._convert_columns_to_query_names(main_df)

    def _build_filtered_record_ids_by_dimension_type(self, context: QueryContext):
        record_ids = {}
        base_dim_query_names = self._config.get_base_dimension_query_names()

        def default_records(dimension_type):
            return (
                self._config.get_base_dimension(dimension_type)
                .get_records_dataframe()
                .select("id")
            )

        for dim_filter in context.model.project.dimension_filters:
            dim_type = dim_filter.dimension_type
            df = record_ids.get(dim_type, default_records(dim_type))
            if dim_filter.query_name == base_dim_query_names[dim_type]:
                df = dim_filter.apply_filter(df, column="id")
            else:
                mapping_records = self._config.get_base_to_supplemental_mapping_records(
                    dim_filter.query_name
                )
                required_ids = mapping_records.select("from_id").distinct()
                df = df.join(required_ids, on=df.id == required_ids.from_id).drop("from_id")
                df = dim_filter.apply_filter(df, column="id")
            record_ids[dim_filter.dimension_type] = df

        for aggregation in context.model.project.metric_reductions:
            dim = self._config.get_dimension(aggregation.query_name)
            dim_type = dim.model.dimension_type
            mapping_records = self._config.get_base_to_supplemental_mapping_records(
                aggregation.query_name
            ).filter("to_id is not NULL")
            required_ids = mapping_records.select("from_id").distinct()
            df = record_ids.get(dim_type, default_records(dim_type))
            df = df.join(required_ids, on=df.id == required_ids.from_id).drop("from_id")
            # TODO: We could truncate the mapping records with any record that just got removed
            # for the life of this query. Save in the context.
            record_ids[dim_type] = df

        for dimension_type, record_ids in record_ids.items():
            context.set_record_ids_by_dimension_type(dimension_type, record_ids)

    def _handle_supplemental_query_names(self, context: QueryContext):
        def check_query_name(query_name, tag):
            dim = self._config.get_dimension(query_name)
            base_dim = self._config.get_base_dimension(dim.model.dimension_type)
            if dim.model.dimension_id == base_dim.model.dimension_id:
                raise DSGInvalidParameter(
                    f"{tag} cannot contain a base dimension: "
                    f"{dim.model.dimension_type}/{query_name}"
                )
            return dim, base_dim

        for query_name in context.model.supplemental_columns:
            check_query_name(query_name, "supplemental_columns")

        for aggregation in context.model.project.metric_reductions:
            query_name = aggregation.query_name
            dim, base_dim = check_query_name(query_name, "MetricReductionModel")
            context.add_metric_reduction_records(
                query_name,
                dim.model.dimension_type,
                self._config.get_base_to_supplemental_mapping_records(query_name),
            )

        for aggregation in context.model.aggregations:
            assert not isinstance(
                aggregation, ChainedAggregationModel
            ), "ChainedAggregationModel is not supported yet"

            for query_name in aggregation.group_by_columns:
                dim = self._config.get_dimension(query_name)
                base_dim = self._config.get_base_dimension(dim.model.dimension_type)
                if dim.model.dimension_id != base_dim.model.dimension_id:
                    context.add_required_dimension_mapping(aggregation.name, query_name)

    def _convert_columns_to_query_names(self, df):
        # All columns start off as base dimension names but need to be query names.
        columns = set(df.columns)
        base_query_names = self._config.get_base_dimension_query_names()
        for dim_type in DimensionType:
            if dim_type.value in columns:
                df = df.withColumnRenamed(dim_type.value, base_query_names[dim_type])
        return df
