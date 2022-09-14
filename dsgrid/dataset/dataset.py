"""Provides access to a dataset."""

import abc
import logging

from pyspark.sql import SparkSession

from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.query.query_context import QueryContext

logger = logging.getLogger(__name__)


class DatasetBase(abc.ABC):
    """Base class for datasets"""

    VIEW_NAMES = ("load_data_lookup", "load_data")

    def __init__(self, schema_handler):
        self._handler = schema_handler
        self._id = schema_handler.config.model.dataset_id
        # Can't use dashes in view names. This will need to be handled when we implement
        # queries based on dataset ID.
        # TODO: do we need a DimensionStore here?

    @property
    def dataset_id(self):
        return self._id

    def get_dataframe(self, query: QueryContext, project_config):
        return self._handler.get_dataframe(query, project_config)

    def _make_view_name(self, name):
        return f"{self._id}__{name}"

    def _make_view_names(self):
        return (f"{self._id}__{name}" for name in self.VIEW_NAMES)

    def create_views(self):
        """Create views for each of the tables in this dataset."""
        # TODO: should we create these in a separate database?
        # TODO DT: views should be created by the dataset handler
        self.load_data_lookup.createOrReplaceTempView(self._make_view_name("load_data_lookup"))
        self.load_data.createOrReplaceTempView(self._make_view_name("load_data"))

    def delete_views(self):
        """Delete views of the tables in this dataset."""
        spark = SparkSession.getActiveSession()
        for view in self._make_view_names():
            spark.catalog.dropTempView(view)

    # TODO: the following two methods need to abstract load_data_lookup
    # They can only be used with Standard dataset schema.

    @property
    def load_data(self):
        return self._handler._load_data

    @property
    def load_data_lookup(self):
        return self._handler._load_data_lookup


class Dataset(DatasetBase):
    """Represents a dataset used within a project."""

    @classmethod
    def load(
        cls, config, dimension_mgr, dimension_mapping_mgr, mapping_references, project_time_dim
    ):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager
        dimension_mapping_mgr : DimensionMappingRegistryManager
        mapping_references: List[DimensionMappingReferenceListModel]
        project_time_dim: TimeDimensionBaseConfig

        Returns
        -------
        Dataset

        """
        return cls(
            make_dataset_schema_handler(
                config,
                dimension_mgr,
                dimension_mapping_mgr,
                mapping_references=mapping_references,
                project_time_dim=project_time_dim,
            )
        )


class StandaloneDataset(DatasetBase):
    """Represents a dataset used outside of a project."""

    @classmethod
    def load(cls, config, dimension_mgr):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager

        Returns
        -------
        Dataset

        """
        return cls(make_dataset_schema_handler(config, dimension_mgr, None))
