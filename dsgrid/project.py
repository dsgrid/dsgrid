"""Interface to a dsgrid project."""

import itertools
import logging

from pyspark.sql import SparkSession

from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType  # ,MappingType
from dsgrid.dimension.store import DimensionStore
from dsgrid.exceptions import DSGInvalidField, DSGValueNotRegistered
from dsgrid.registry.registry_manager import RegistryManager, get_registry_path
from dsgrid.utils.spark import init_spark


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
    def load(cls, project_id, registry_path=None, version=None, offline_mode=False):
        """Load a project from the registry.
        Parameters
        ----------
        project_id : str
        registry_path : str | None
        version : str | None
            Use the latest if not specified.
        offline_mode : bool
            If True, don't sync with remote registry
        """
        registry_path = get_registry_path(registry_path=registry_path)
        manager = RegistryManager.load(registry_path, offline_mode=offline_mode)
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

    # ---------
    # datasets
    # ---------

    def _iter_datasets(self):
        """ only iter through loaded datasets """
        for dataset in self._datasets.values():
            yield dataset

    def list_datasets(self):
        dataset_ids_reg = self.config.list_registered_dataset_ids()
        dataset_ids_unreg = self.config.list_unregistered_dataset_ids()
        print(
            "Project has the following datasets:\n"
            f"- registered: {dataset_ids_reg} \n"
            f"- unregistered: {dataset_ids_unreg}"
        )

        return dataset_ids_reg, dataset_ids_unreg

    def load_dataset(self, dataset_id):
        """Loads dataset and project requirement. Creates a view for each of its tables.

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

    def get_dataset(self, dataset_id):  # need fixing
        """Returns a Dataset, calls load_dataset if it hasn't already been loaded.
            - raw datasets: load_data/lookup
            - remapped to project requirement: mapped_load_data/lookup

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id not in self._datasets:
            raise ValueError(f"dataset {dataset_id} has not been loaded")

        remapped_dataset = self._datasets[dataset_id]
        return self._datasets[dataset_id]

    # -----------
    # dimensions
    # -----------

    def _iter_base_dimensions(self):
        for dimension in self.config.base_dimensions.values():
            yield dimension

    def get_project_dimension_types(self):
        return [dim.model.dimension_type.value for dim in self._iter_base_dimensions()]

    def get_project_dimension(self, dimension_type):
        """ return dimension config """
        DimType = dimension_type
        if isinstance(DimType, str):
            DimType = DimensionType(DimType)
        for dimension in self._iter_base_dimensions():
            if dimension.model.dimension_type == DimType:
                return dimension
        raise DSGInvalidField(f"{dimension_type} is not stored")

    def get_geography(self):
        # TODO: is name right? cls?
        return self.get_project_dimension(
            DimensionType.GEOGRAPHY
        )  # TODO: may be delete, not super important

    # -------------------
    # dimension mappings
    # -------------------

    def get_dimension_mappings(self, dimension_type, mapping_type):
        if isinstance(dimension_type, str):
            dimension_type = DimensionType(dimension_type)
        if isinstance(mapping_type, str):
            mapping_type = MappingType(mapping_type)
        dimension = self.get_project_dimension(dimension_type)
        return getattr(dimension, mapping_type.value)  # not working
