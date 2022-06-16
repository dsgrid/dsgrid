"""Interface to a dsgrid project."""

import logging

from pyspark.sql import SparkSession

from dsgrid.common import REMOTE_REGISTRY
from dsgrid.dataset.dataset import Dataset
from dsgrid.exceptions import DSGValueNotRegistered
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
        remote_path: str, optional
            path of the remote registry; default is REMOTE_REGISTRY
        use_remote_data: bool, None
            If set, use load data tables from remote_path. If not set, auto-determine what to do
            based on HPC or AWS EMR environment variables.
        version : str | None
            Use the latest if not specified.
        offline_mode : bool
            If True, don't sync with remote registry
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

    def get_dataset(self, dataset_id):
        """Returns a Dataset. Calls load_dataset if it hasn't already been loaded.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id not in self._datasets:
            raise ValueError(f"dataset {dataset_id} has not been loaded")
        return self._datasets[dataset_id]

    def load_dataset(self, dataset_id):
        """Loads a dataset. Creates a view for each of its tables.

        Parameters
        ----------
        dataset_id : str

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
        )
        dataset.create_views()
        self._datasets[dataset_id] = dataset

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
