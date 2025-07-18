"""Provides access to a dataset."""

import abc
import logging
from typing import Optional

from sqlalchemy import Connection

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.project_config import ProjectConfig
from dsgrid.query.query_context import QueryContext
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
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
        config,
        dimension_mgr,
        dimension_mapping_mgr,
        mapping_references,
        project_time_dim,
        conn: Optional[Connection] = None,
    ):
        """Load a dataset from a store.

        Parameters
        ----------
        config : DatasetConfig
        dimension_mgr : DimensionRegistryManager
        dimension_mapping_mgr : DimensionMappingRegistryManager
        mapping_references: list[DimensionMappingReferenceListModel]
        project_time_dim: TimeDimensionBaseConfig

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
                mapping_references=mapping_references,
                project_time_dim=project_time_dim,
            )
        )

    def make_project_dataframe(
        self, query: QueryContext, project_config: ProjectConfig
    ) -> DataFrame:
        return self._handler.make_project_dataframe(query, project_config)


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
        return cls(make_dataset_schema_handler(None, config, dimension_mgr, None))
