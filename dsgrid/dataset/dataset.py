"""Provides access to a dataset."""

import abc
import logging

from sqlalchemy import Connection

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceListModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.query.query_context import QueryContext
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.spark.types import DataFrame

logger = logging.getLogger(__name__)


class DatasetBase(abc.ABC):
    """Base class for datasets"""

    def __init__(self, schema_handler: DatasetSchemaHandlerBase):
        self._config = schema_handler.config
        self._handler = schema_handler
        self._id = schema_handler.config.model.dataset_id
        # Can't use dashes in view names. This will need to be handled when we implement
        # queries based on dataset ID.

    @property
    def config(self) -> DatasetConfig:
        return self._config

    @property
    def dataset_id(self) -> str:
        return self._id

    @property
    def handler(self) -> DatasetSchemaHandlerBase:
        return self._handler


class Dataset(DatasetBase):
    """Represents a dataset used within a project."""

    @classmethod
    def load(
        cls,
        config: DatasetConfig,
        dimension_mgr: DimensionRegistryManager,
        dimension_mapping_mgr: DimensionMappingRegistryManager,
        store: DataStoreInterface,
        mapping_references: list[DimensionMappingReferenceListModel],
        conn: Connection | None = None,
    ):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager
        dimension_mapping_mgr : DimensionMappingRegistryManager
        mapping_references: list[DimensionMappingReferenceListModel]

        Returns
        -------
        Dataset

        """
        return cls(
            make_dataset_schema_handler(
                conn,
                config,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
            )
        )

    def make_project_dataframe(
        self, query: QueryContext, project_config: ProjectConfig
    ) -> DataFrame:
        return self._handler.make_project_dataframe(query, project_config)


class StandaloneDataset(DatasetBase):
    """Represents a dataset used outside of a project."""

    @classmethod
    def load(
        cls,
        config: DatasetConfig,
        dimension_mgr: DimensionRegistryManager,
        dimension_mapping_mgr: DimensionMappingRegistryManager,
        store: DataStoreInterface,
        mapping_references: list[DimensionMappingReferenceListModel] | None = None,
        conn: Connection | None = None,
    ):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager

        Returns
        -------
        Dataset

        """
        return cls(
            make_dataset_schema_handler(
                conn,
                config,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
            )
        )
