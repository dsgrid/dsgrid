
from dsgrid.config.project_config import ProjectConfig, load_project_config
from dsgrid.analysis.datasets import Datasets
from dsgrid.dimension.base import DimensionType, MappingType
from dsgrid.dimension.store import DimensionStore
from dsgrid.exceptions import DSGInvalidField
from dsgrid.utils.spark import init_spark


class Project:
    """Interface to a dsgrid project."""
    def __init__(self, project_config, dimension_store, datasets, spark):
        self._spark = spark
        self._config = project_config
        self._dimension_store = dimension_store
        self._datasets = datasets

    @classmethod
    def load(cls, project_config_file, spark=None):
        """Load a project from a config file."""
        if spark is None:
            spark = init_spark("project")
        config = load_project_config(project_config_file)
        dimension_store = DimensionStore.load(config, spark)
        datasets = Datasets.load(dimension_store)
        return cls(config, dimension_store, datasets, spark)

    @property
    def config(self):
        return self._config

    @property
    def datasets(self):
        return self._datasets

    @property
    def spark(self):
        return self._spark

    @property
    def store(self):
        return self._dimension_store


    """
    The code below is subject to change.
    """
    def _iter_project_dimensions(self):
        for dimension in self.config.dimensions.project_dimensions:
            yield dimension

    def _iter_input_datasets(self):
        for dataset in self.config.input_datasets.datasets:
            yield dataset

    def list_input_datasets(self):
        return [x.dataset_id for x in self._iter_input_datasets()]

    def get_project_dimension(self, dimension_type):
        for dimension in self._iter_project_dimensions():
            if dimension.dimension_type == dimension_type:
                return dimension
        raise DSGInvalidField(f"{dimension_type} is not stored")

    def get_input_dataset(self, dataset_id):
        for dataset in self._iter_input_datasets():
            if dataset.dataset_id == dataset_id:
                return dataset
        raise DSGInvalidField(f"{dataset_id} is not stored")

    def get_geography(self):
        # TODO: is name right? cls?
        return self.get_project_dimension(DimensionType.GEOGRAPHY).name

    def get_dimension_mappings(self, dimension_type, mapping_type):
        if isinstance(mapping_type, str):
            mapping_type = MappingType(mapping_type)
        dimension = self.get_project_dimension(dimension_type)
        return getattr(dimension, mapping_type.value)
