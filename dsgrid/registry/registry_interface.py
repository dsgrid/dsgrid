import abc
import itertools
import logging
from typing import Any, Generator

import pandas as pd
from sqlalchemy import Connection, Engine

from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.config.dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.config.dimensions import DimensionBaseModel, handle_dimension_union
from dsgrid.config.mapping_tables import MappingTableModel
from dsgrid.config.project_config import ProjectConfigModel
from dsgrid.data_models import DSGBaseDatabaseModel, DSGBaseModel
from dsgrid.registry.common import (
    DatasetRegistryStatus,
    MODEL_TYPE_TO_ID_FIELD_MAPPING,
    RegistrationModel,
    RegistryType,
)
from dsgrid.registry.registry_database import RegistryDatabase


logger = logging.getLogger(__name__)


class RegistryInterfaceBase(abc.ABC):
    """Interface base class"""

    def __init__(self, db: RegistryDatabase):
        self._db = db

    @property
    def engine(self) -> Engine:
        """Return the sqlalchemy engine."""
        return self._db.engine

    @staticmethod
    @abc.abstractmethod
    def _model_type() -> RegistryType:
        """Return the model type."""

    @abc.abstractmethod
    def _get_model_id(self, model) -> str:
        """Return the dsgrid ID for the model."""

    @abc.abstractmethod
    def _get_model_id_field(self):
        """Return the field representing the model ID for the model."""

    def _insert_contains_edges(self, conn: Connection, model) -> None:
        """Add contains edges to any dependent documents."""

    @staticmethod
    @abc.abstractmethod
    def _make_dsgrid_model(db_data: dict) -> Any:
        """Convert the database object into a dsgrid model."""

    def delete_all(self, conn: Connection | None, model_id: str) -> None:
        """Delete all database instances with model_id."""
        if conn is None:
            with self._db.engine.begin() as conn:
                return self._db.delete_models(conn, self._model_type(), model_id)
        return self._db.delete_models(conn, self._model_type(), model_id)

    def get_by_version(self, conn: Connection | None, model_id, version) -> DSGBaseDatabaseModel:
        """Return the model by version"""
        return self._make_dsgrid_model(self._get_by_version(conn, model_id, version))

    def get_latest(self, conn: Connection | None, model_id) -> DSGBaseModel:
        """Return the model with the latest version."""
        return self._make_dsgrid_model(self._get_latest(conn, model_id))

    def get_latest_version(self, conn: Connection | None, model_id: str) -> str:
        """Return the latest version of the model_id.

        Parameters
        ----------
        model_id : str
            dsgrid identifier for a model item

        Returns
        -------
        str
        """
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.get_latest_version(conn, self._model_type(), model_id)
        return self._db.get_latest_version(conn, self._model_type(), model_id)

    def get_registration(
        self, conn: Connection | None, model: DSGBaseDatabaseModel
    ) -> RegistrationModel:
        """Return the registration information for the model."""
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.get_registration(conn, model.id)
        return self._db.get_registration(conn, model.id)

    def get_initial_registration(
        self, conn: Connection | None, model_id: str
    ) -> RegistrationModel:
        """Return the initial registration information for the model."""
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.get_initial_registration(conn, self._model_type(), model_id)
        return self._db.get_initial_registration(conn, self._model_type(), model_id)

    def has(self, conn: Connection | None, model_id: str, version: str | None = None) -> bool:
        """Return True if the model_id is stored in the database."""
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.has(conn, self._model_type(), model_id, version=version)
        return self._db.has(conn, self._model_type(), model_id, version=version)

    def insert(
        self,
        conn: Connection | None,
        model: DSGBaseDatabaseModel,
        registration: RegistrationModel,
    ) -> DSGBaseModel:
        """Add a new model in the database."""
        if conn is None:
            with self._db.engine.begin() as conn:
                return self._insert(conn, model, registration)
        return self._insert(conn, model, registration)

    def _insert(
        self, conn: Connection, model: DSGBaseDatabaseModel, registration: RegistrationModel
    ) -> DSGBaseModel:
        new_model = self._make_dsgrid_model(
            self._db.insert_model(
                conn, self._model_type(), model.model_dump(mode="json"), registration
            )
        )
        self._insert_contains_edges(conn, new_model)
        return new_model

    def insert_registration(
        self, conn: Connection | None, registration: RegistrationModel
    ) -> RegistrationModel:
        """Add the registration to the database.

        Returns
        -------
        database ID of registration entry
        """
        if conn is None:
            with self._db.engine.begin() as conn:
                return self._db.insert_registration(conn, registration)
        return self._db.insert_registration(conn, registration)

    def list_model_ids(self, conn: Connection | None = None) -> list[str]:
        """Return a list of all distinct dsgrid model IDs."""
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.list_model_ids(conn, self._model_type())
        return self._db.list_model_ids(conn, self._model_type())

    def iter_models(
        self,
        conn: Connection | None = None,
        all_versions: bool = False,
        filter_config: dict[str, Any] | None = None,
    ) -> Generator[DSGBaseDatabaseModel, None, None]:
        """Return a generator of dsgrid models converted from database objects.

        Parameters
        ----------
        all_versions : bool
            If False, return only the latest version of each model in the registry of this type.
            If True, return all versions of each model (such as all versions of every project).
        filter_config : None | dict
            If set, it must be a dict of model field names to values. Only return models that
            match the filter
        """
        if conn is None:
            with self._db.engine.connect() as conn:
                yield from self._iter_models(
                    conn, all_versions=all_versions, filter_config=filter_config
                )
        else:
            yield from self._iter_models(
                conn, all_versions=all_versions, filter_config=filter_config
            )

    def _iter_models(
        self,
        conn: Connection,
        all_versions: bool = False,
        filter_config: dict[str, Any] | None = None,
    ) -> Generator[DSGBaseDatabaseModel, None, None]:
        for data in self._db.iter_models(conn, self._model_type(), all_versions=all_versions):
            model = self._make_dsgrid_model(data)
            if filter_config is None or self._does_filter_match(model, filter_config):
                yield self._make_dsgrid_model(data)

    def get_containing_models(
        self,
        conn: Connection | None,
        model: DSGBaseDatabaseModel,
        version: str | None = None,
        parent_model_type: RegistryType | None = None,
    ) -> dict[RegistryType, list[DSGBaseDatabaseModel]]:
        """Return all models that contain the given model. If version is not set, use the current
        version of model. Only looks at the latest version of each parent model.
        """
        version_ = version or model.version
        if isinstance(model, ProjectConfigModel):
            model_type = RegistryType.PROJECT
        elif isinstance(model, DatasetConfigModel):
            model_type = RegistryType.DATASET
        elif isinstance(model, DimensionBaseModel):
            model_type = RegistryType.DIMENSION
        elif isinstance(model, DimensionMappingBaseModel):
            model_type = RegistryType.DIMENSION_MAPPING
        else:
            msg = str(type(model))
            raise NotImplementedError(msg)
        model_id = getattr(model, MODEL_TYPE_TO_ID_FIELD_MAPPING[model_type])
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._get_containing_models(
                    conn, model_type, model_id, version_, parent_model_type=parent_model_type
                )
        return self._get_containing_models(
            conn, model_type, model_id, version_, parent_model_type=parent_model_type
        )

    def _get_containing_models(
        self,
        conn: Connection,
        child_model_type: RegistryType,
        model_id: str,
        version: str,
        parent_model_type: RegistryType | None = None,
    ) -> dict[RegistryType, list[DSGBaseDatabaseModel]]:
        results = {x: [] for x in RegistryType}
        for model_type, data in self._db.get_containing_models(
            conn, child_model_type, model_id, version, parent_model_type=parent_model_type
        ):
            model = _INTERFACE_BY_TYPE[model_type]._make_dsgrid_model(data)
            results[model_type].append(model)
        return results

    @staticmethod
    def _does_filter_match(model: DSGBaseDatabaseModel, filter_config):
        for key, val in filter_config.items():
            if getattr(model, key) != val:
                return False
        return True

    def replace(self, conn: Connection | None, model: DSGBaseDatabaseModel):
        """Replace an existing model in the database."""
        if conn is None:
            with self._db.engine.begin() as conn:
                return self._replace(conn, model)
        return self._replace(conn, model)

    def _replace(self, conn: Connection, model: DSGBaseDatabaseModel):
        return self._db.replace_model(conn, model.model_dump(mode="json"))

    def update(
        self,
        conn: Connection | None,
        model: DSGBaseDatabaseModel,
        registration: RegistrationModel,
    ) -> DSGBaseDatabaseModel:
        """Update an existing model in the database."""
        if conn is None:
            with self._db.engine.begin() as conn:
                return self._update(conn, model, registration)
        return self._update(conn, model, registration)

    def _update(
        self, conn: Connection, model: DSGBaseDatabaseModel, registration: RegistrationModel
    ) -> DSGBaseDatabaseModel:
        new_model = self._make_dsgrid_model(
            self._db.update_model(
                conn, self._model_type(), model.model_dump(mode="json"), registration
            )
        )
        self._insert_contains_edges(conn, new_model)
        return new_model

    def _get_by_version(self, conn: Connection | None, model_id, version) -> dict[str, Any]:
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db._get_by_version(conn, self._model_type(), model_id, version)
        return self._db._get_by_version(conn, self._model_type(), model_id, version)

    def _get_latest(self, conn: Connection | None, model_id: str) -> dict[str, Any]:
        if conn is None:
            with self._db.engine.connect() as conn:
                return self._db.get_latest(conn, self._model_type(), model_id)
        return self._db.get_latest(conn, self._model_type(), model_id)

    @property
    def datasets(self) -> Generator[dict[str, Any], None, None]:
        with self.engine.connect() as conn:
            yield from self._db.iter_models(conn, RegistryType.DATASET)

    @property
    def dimensions(self) -> Generator[dict[str, Any], None, None]:
        with self.engine.connect() as conn:
            yield from self._db.iter_models(conn, RegistryType.DIMENSION)

    @property
    def dimension_mappings(self) -> Generator[dict[str, Any], None, None]:
        with self.engine.connect() as conn:
            yield from self._db.iter_models(conn, RegistryType.DIMENSION_MAPPING)

    @property
    def projects(self) -> Generator[dict[str, Any], None, None]:
        with self.engine.connect() as conn:
            yield from self._db.iter_models(conn, RegistryType.PROJECT)

    def sql(
        self, query: str, params: Any | None = None, conn: Connection | None = None
    ) -> pd.DataFrame:
        """Return the results of a query as a Pandas DataFrame."""
        if conn is None:
            with self.engine.connect() as conn:
                return pd.read_sql(query, con=conn, params=params)
        return pd.read_sql(query, con=conn, params=params)


class DatasetRegistryInterface(RegistryInterfaceBase):
    """Interface to database for datasets"""

    @staticmethod
    def _model_type() -> RegistryType:
        return RegistryType.DATASET

    def _get_model_id(self, model):
        return model.dataset_id

    def _get_model_id_field(self):
        return "dataset_id"

    @staticmethod
    def _make_dsgrid_model(db_data: dict):
        return DatasetConfigModel(**db_data)

    def _insert_contains_edges(self, conn: Connection, model):
        dim_intf = DimensionRegistryInterface(self._db)
        for ref in model.dimension_references:
            dim = dim_intf.get_by_version(conn, ref.dimension_id, ref.version)
            self._db.insert_contains_edge(conn, model.id, dim.id)


class DimensionRegistryInterface(RegistryInterfaceBase):
    """Interface to database for dimensions"""

    @staticmethod
    def _model_type() -> RegistryType:
        return RegistryType.DIMENSION

    def _get_model_id(self, model):
        return model.dimension_id

    def _get_model_id_field(self):
        return "dimension_id"

    @staticmethod
    def _make_dsgrid_model(db_data: dict):
        return handle_dimension_union([db_data])[0]


class DimensionMappingRegistryInterface(RegistryInterfaceBase):
    """Interface to database for dimension mappings"""

    @staticmethod
    def _model_type() -> RegistryType:
        return RegistryType.DIMENSION_MAPPING

    def _get_model_id(self, model):
        return model.mapping_id

    def _get_model_id_field(self):
        return "mapping_id"

    @staticmethod
    def _make_dsgrid_model(db_data: dict[str, Any]):
        return MappingTableModel(**db_data)

    def _insert_contains_edges(self, conn, model):
        dim_intf = DimensionRegistryInterface(self._db)
        for ref in (model.from_dimension, model.to_dimension):
            dim = dim_intf.get_by_version(conn, ref.dimension_id, ref.version)
            self._db.insert_contains_edge(conn, model.id, dim.id)


class ProjectRegistryInterface(RegistryInterfaceBase):
    """Interface to database for projects"""

    @staticmethod
    def _model_type() -> RegistryType:
        return RegistryType.PROJECT

    def _get_model_id(self, model):
        return model.project_id

    def _get_model_id_field(self):
        return "project_id"

    @staticmethod
    def _make_dsgrid_model(db_data: dict):
        return ProjectConfigModel(**db_data)

    def _insert_contains_edges(self, conn: Connection, model):
        dim_intf = DimensionRegistryInterface(self._db)
        dim_mapping_intf = DimensionMappingRegistryInterface(self._db)
        dataset_intf = DatasetRegistryInterface(self._db)

        for ref in itertools.chain(
            model.dimensions.base_dimension_references,
            model.dimensions.supplemental_dimension_references,
        ):
            dim = dim_intf.get_by_version(conn, ref.dimension_id, ref.version)
            self._db.insert_contains_edge(conn, model.id, dim.id)

        for ref in model.dimension_mappings.base_to_supplemental_references:
            mapping = dim_mapping_intf.get_by_version(conn, ref.mapping_id, ref.version)
            self._db.insert_contains_edge(conn, model.id, mapping.id)

        for dataset in model.datasets:
            if dataset.status == DatasetRegistryStatus.REGISTERED.value:
                dset = dataset_intf.get_by_version(conn, dataset.dataset_id, dataset.version)
                self._db.insert_contains_edge(conn, model.id, dset.id)

    def add_contains_dataset(
        self, conn: Connection, project: ProjectConfigModel, dataset: DatasetConfigModel
    ) -> None:
        assert project.id is not None
        assert dataset.id is not None
        self._db.insert_contains_edge(conn, project.id, dataset.id)


_INTERFACE_BY_TYPE = {
    RegistryType.PROJECT: ProjectRegistryInterface,
    RegistryType.DATASET: DatasetRegistryInterface,
    RegistryType.DIMENSION: DimensionRegistryInterface,
    RegistryType.DIMENSION_MAPPING: DimensionMappingRegistryInterface,
}
assert len(_INTERFACE_BY_TYPE) == len(RegistryType)
