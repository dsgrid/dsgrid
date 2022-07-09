"""Interface to a dsgrid project."""

import logging

from pyspark.sql import SparkSession

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.query import AlternateDimensionsModel, QueryContext
from dsgrid.registry.registry_manager import RegistryManager, get_registry_path


logger = logging.getLogger(__name__)


class Project:
    """Interface to a dsgrid project."""

    def __init__(self, config, dataset_configs, dimension_mgr, dimension_mapping_mgr):
        self._spark = SparkSession.getActiveSession()
        self._config = config
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

        dataset_configs = {}
        for dataset_id in config.list_registered_dataset_ids():
            dataset_config = dataset_manager.get_by_id(dataset_id)
            dataset_configs[dataset_id] = dataset_config

        return cls(
            config, dataset_configs, manager.dimension_manager, manager.dimension_mapping_manager
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

    def process_query(self, query_context: QueryContext):
        df = self._make_query_dataframe(query_context)
        # TODO: handle final aggregration
        # if query_context.model.aggregation is not None:
        name = query_context.model.name
        output_dir = query_context.model.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = output_dir / (name + ".parquet")
        df.write.mode("overwrite").parquet(str(filename))
        logger.info("Wrote query=%s output table to %s", name, filename)

    def _make_query_dataframe(self, query_context: QueryContext):
        for field in AlternateDimensionsModel.__fields__:
            model = getattr(query_context.model.alternate_dimensions, field, None)
            if model is not None:
                dim_type = DimensionType(field)
                query_context.set_alt_dimension_records(
                    dim_type,
                    self._config.get_base_to_supplemental_mapping_records(
                        dim_type, model.query_name
                    ),
                )

        dfs = []
        for dataset_id in query_context.model.dataset_ids:
            logger.info("Start processing query for dataset_id=%s", dataset_id)
            dataset = self.get_dataset(dataset_id)
            dfs.append(dataset.get_dataframe(query_context))
            logger.info("Finished processing query for dataset_id=%s", dataset_id)

        if not dfs:
            logger.warning("No data matched %s", query_context.name)
            return None

        if len(dfs) == 1:
            return dfs[0]

        main_df = dfs[0]
        for df in dfs[1:]:
            main_df = main_df.union(df)
        return main_df
