
from dataclasses import fields
import itertools
import logging
import os


from dsgrid.dimension.base import DSGBaseDimensionModel
from dsgrid.dimension.dimension_records import DimensionRecords
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimension, DSGInvalidDimensionMapping
from dsgrid.config.project_config import (
    ProjectConfig, load_project_config, DimensionDirectMapping,
)
from dsgrid.utils.files import load_data
from dsgrid.utils.timing import timed_debug, timed_info


logger = logging.getLogger(__name__)


class DimensionStore:
    """Provides mapping functionality for project-defined dimensions."""

    REQUIRED_FIELDS = ("id", "name")

    def __init__(self, project_config, record_store):
        self._record_store = record_store
        self._store = {}  # {class type: DSGBaseDimensionModel}
        self._project_config = project_config
        self._dimension_direct_mappings = {}

    @classmethod
    @timed_debug
    def load(cls, project_config_file, spark=None):
        """Load a project's dimension dataset records.

        Parameters
        ----------
        project_config_file : str

        Returns
        -------
        DimensionStore

        """
        project_config = load_project_config(project_config_file)
        records = DimensionRecords(spark=spark)
        store = cls(project_config, records)
        for dimension in itertools.chain(
            store.project_dimensions,
            store.supplemental_dimensions
        ):
            # TODO: do we need to store project and supplemental in different
            # containers? We already have the info in project_config.
            store.add_dimension(dimension)
        return store

    @property
    def project_config(self):
        return self._project_config

    @property
    def project_dimensions(self):
        return self._project_config.dimensions.project_dimensions

    @property
    def supplemental_dimensions(self):
        return self._project_config.dimensions.supplemental_dimensions

    def add_dimension(self, dimension):
        """Add a dimension to the store.

        Parameters
        ----------
        dimension : DSGBaseDimensionModel

        """
        assert dimension.cls not in self._store
        self._store[dimension.cls] = dimension

        if getattr(dimension, "mappings", None):
            for mapping in dimension.mappings:
                self.add_dimension_mapping(dimension, mapping)

        if getattr(dimension, "records", None):
            self._record_store.add_dataframe(dimension)

    def add_dimension_mapping(self, dimension, mapping):
        """Add a dimension mapping to the store.

        Parameters
        ----------
        dimension : DSGBaseDimensionModel
        mapping : DimensionDirectMapping

        """
        # TODO: other mapping types
        assert isinstance(mapping, DimensionDirectMapping), mapping.__name__
        key = (dimension.cls, mapping.to_dimension)
        assert key not in self._dimension_direct_mappings
        self._dimension_direct_mappings[key] = mapping
        logger.debug("Added dimension mapping %s-%s", dimension.cls, mapping)

    def get_dimension(self, dimension_class):
        """Return a dimension from the store.

        Parameters
        ----------
        dimension_class : type
            subclass of DSGBaseDimensionModel

        Returns
        -------
        DSGBaseDimensionModel
            instance of DSGBaseDimensionModel

        """
        self._raise_if_dimension_not_stored(dimension_class)
        return self._store[dimension_class]

    def get_dimension_direct_mapping(self, from_dimension, to_dimension):
        """Return the mapping to perform a join.

        Parameters
        ----------
        from_dimension : type
        to_dimension : type

        Returns
        -------
        str

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if the mapping is not stored.

        """
        key = (from_dimension, to_dimension)
        self._raise_if_mapping_not_stored(key)
        return self._dimension_direct_mappings[key]

    def iter_dimension_classes(self, base_class=None):
        """Return an iterator over the stored dimension classes.

        Parameters
        ----------
        base_class : type | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        iterator
            model classes representing each dimension

        """
        if base_class is None:
            return self._store.keys()
        return (x for x in self._store if issubclass(x, base_class))

    def list_dimension_classes(self, base_class=None):
        """Return the stored dimension classes.

        Parameters
        ----------
        base_class : type | None
            If set, return subclasses of this abstract dimension class

        Returns
        -------
        list
            list of classes representing each dimension type

        """
        return sorted(list(self.iter_dimension_classes(base_class=base_class)),
                      key=lambda x: x.__name__)

    @property
    def record_store(self):
        """Return the DimensionRecords."""
        return self._record_store

    @property
    def spark(self):
        """Return the SparkSession instance."""
        return self._record_store.spark

    def get_dataset(self, dataset_id):
        """Return a dataset by ID."""
        for dataset in self._project_config.input_datasets.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset

        raise DSGInvalidField(f"no dataset with dataset_id={dataset_id}")

    def iter_datasets(self):
        for dataset in self._project_config.input_datasets.datasets:
            yield dataset

    def iter_dataset_ids(self):
        for dataset in self._project_config.input_datasets.datasets:
            yield dataset.dataset_id

    def _raise_if_dimension_not_stored(self, dimension_class):
        if dimension_class not in self._store:
            raise DSGInvalidDimension(f"{dimension_class} is not stored")

    def _raise_if_mapping_not_stored(self, key):
        if key not in self._dimension_direct_mappings:
            raise DSGInvalidDimensionMapping(f"{key} is not stored")
