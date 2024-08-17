import abc
import itertools
import logging
from typing import Optional

from pydantic import Field
from typing_extensions import Annotated

from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.config.dimensions import handle_dimension_union
from dsgrid.config.mapping_tables import MappingTableModel
from dsgrid.config.project_config import ProjectConfigModel
from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered
from .common import Collection, DatasetRegistryStatus, RegistrationModel, Edge, RegistryType
from .registry_database import RegistryDatabase


logger = logging.getLogger(__name__)


class RegistryModel(DSGBaseModel):
    """Base model for a registry root"""

    registry_type: str
    id: Annotated[
        Optional[str],
        Field(
            None,
            alias="_id",
            description="Registry database ID",
            json_schema_extra={
                "dsgrid_internal": True,
            },
        ),
    ]
    key: Annotated[
        Optional[str],
        Field(
            None,
            alias="_key",
            description="Registry database key",
            json_schema_extra={
                "dsgrid_internal": True,
            },
        ),
    ]
    rev: Annotated[
        Optional[str],
        Field(
            None,
            alias="_rev",
            description="Registry database revision",
            json_schema_extra={
                "dsgrid_internal": True,
            },
        ),
    ]


class DatasetRegistryModel(RegistryModel):
    """Data model for dataset roots"""

    dataset_id: str


class ProjectRegistryModel(RegistryModel):
    """Data model for project roots"""

    project_id: str


class RegistryInterfaceBase(abc.ABC):
    """Interface base class"""

    def __init__(self, db: RegistryDatabase):
        self._db = db

    @staticmethod
    @abc.abstractmethod
    def _collection_name():
        """Return the collection name"""

    @abc.abstractmethod
    def _get_model_id(self, model):
        """Return the dsgrid ID for the model."""

    @abc.abstractmethod
    def _get_model_id_field(self):
        """Return the field representing the model ID for the model."""

    @staticmethod
    @abc.abstractmethod
    def _get_root_class():
        """Return the class that represents the root of a registry storage for a dsgrid model."""

    def _insert_contains_edges(self, model):
        """Add contains edges to any dependent documents."""

    @staticmethod
    @abc.abstractmethod
    def _make_root_object(model_id):
        """Return a root object for storing dsgrid models in the database.

        Parameters
        ----------
        model_id : str

        Returns
        -------
        RegistryModel
        """

    @abc.abstractmethod
    def _make_dsgrid_model(self, db_data: dict):
        """Convert the database object into a dsgrid model."""

    @staticmethod
    @abc.abstractmethod
    def root_collection_name():
        """Return the registry collection name"""

    @staticmethod
    @abc.abstractmethod
    def _uses_model_id_in_db():
        """Returns True if the dsgrid model ID is used as the database ID."""

    def collection(self, name):
        return self._db.collection(name)

    def delete_all(self, model_id):
        """Delete all database instances with model_id."""
        if not self.has(model_id):
            return

        root_id = self._get_root_db_id(model_id)
        db_id = self.get_latest(model_id).id
        self._db.delete_document(db_id)
        self._db.delete_document(root_id)

    def get_by_version(self, model_id, version):
        """Return the model by version"""
        return self._make_dsgrid_model(self._get_by_version(model_id, version))

    def get_latest(self, model_id):
        """Return the model with the latest version."""
        return self._make_dsgrid_model(self._get_latest(model_id))

    def get_latest_version(self, model_id: str) -> str:
        """Return the latest version of the model_id.

        Parameters
        ----------
        model_id : str
            dsgrid identifier for a model item

        Returns
        -------
        str
        """
        try:
            return self._db.get_latest_version(self._get_root_db_id(model_id))
        except DSGValueNotRegistered:
            msg = (
                f"id='{model_id}' is not registered in the '{self._collection_name()}' collection"
            )
            raise DSGValueNotRegistered(msg)

    def get_registration(self, model) -> RegistrationModel:
        """Return the registration information for the model."""
        return self._db.get_registration(model.id)

    def has(self, model_id, version=None):
        """Return True if the model_id is stored in the database."""
        return self._db.has(self._get_root_db_id(model_id), version=version)

    def insert(self, model, registration: RegistrationModel):
        """Add a new model in the database."""
        model_id = self._get_model_id(model)
        if model.id is not None:
            raise Exception(f"{model.id=} cannot be set on insertion")
        if not self._uses_model_id_in_db():
            if model_id is not None:
                raise Exception(f"{model_id=} cannot be set on insertion")

        model.version = registration.version
        root = self._make_root_object(model_id)
        data = _serialize_new_model(root)
        if self._uses_model_id_in_db():
            data["_key"] = model_id
        res = self._db.collection(self.root_collection_name()).insert(data)
        db_keys = (("id", "_id"), ("key", "_key"), ("rev", "_rev"))
        for new, old in db_keys:
            data[new] = res[old]
        data.pop("_key", None)
        root = type(root)(**data)

        data = _serialize_new_model(model)
        if not self._uses_model_id_in_db():
            data[self._get_model_id_field()] = root.key
        res = self.collection(self._collection_name()).insert(data)
        for new, old in db_keys:
            data[new] = res[old]
        model = self._make_dsgrid_model(data)

        self._db.insert_latest_edge(root, model)
        self._db.insert_updated_to_edge(root, model, registration)
        assert model.version == registration.version
        self._insert_contains_edges(model)
        self._post_insert_handling(model, root)
        return model

    def iter_models(self, all_versions=False, filter_config=None):
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
        # Note that this implemenation is not as performant as a query that lets the database
        # pre-filter. This is not expected to matter.
        for reg in self._db.collection(self.root_collection_name()):
            if all_versions:
                for data in self._db.iter_updated_to_documents(reg["_id"]):
                    model = self._make_dsgrid_model(data)
                    if filter_config is None or self._does_filter_match(model, filter_config):
                        yield self._make_dsgrid_model(data)
            else:
                model = self.get_latest(reg["_key"])
                if filter_config is None or self._does_filter_match(model, filter_config):
                    yield model

    @staticmethod
    def _does_filter_match(model, filter_config):
        for key, val in filter_config.items():
            if getattr(model, key) != val:
                return False
        return True

    def _post_insert_handling(self, model, root):
        """Perform optional handling after a new model has been inserted."""

    def update(self, model, registration: RegistrationModel):
        """Update an existing model in the database."""
        model.version = registration.version
        cur_model = self.get_latest(self._get_model_id(model))
        data = _serialize_model(model)
        res = self.collection(self._collection_name()).insert(data)
        data.update(res)
        new_model = self._make_dsgrid_model(data)

        root_id = self._get_root_db_id(self._get_model_id(model))
        cls = self._get_root_class()
        root = cls(**self.collection(self.root_collection_name()).get(root_id))

        self._db.delete_latest_edge(root)
        self._db.insert_latest_edge(root, new_model)
        self._db.insert_updated_to_edge(cur_model, new_model, registration)
        assert model.version == registration.version
        self._insert_contains_edges(new_model)
        return new_model

    def replace(self, model, check_rev=False):
        """Replace an existing model in the database."""
        data = model.serialize()
        if not check_rev:
            data.pop("_rev")
        return self._db.replace_document(data)

    def _get_by_version(self, model_id, version):
        try:
            return self._db._get_by_version(self._get_root_db_id(model_id), version)
        except DSGValueNotRegistered:
            msg = (
                f"id='{model_id}' is not registered in the '{self._collection_name()}' collection"
            )
            raise DSGValueNotRegistered(msg)

    def _get_latest(self, model_id):
        try:
            return self._db.get_latest(self._get_root_db_id(model_id))
        except DSGValueNotRegistered:
            msg = (
                f"id='{model_id}' is not registered in the '{self._collection_name()}' collection"
            )
            raise DSGValueNotRegistered(msg)

    def _get_root_db_id(self, model_id):
        return f"{self.root_collection_name()}/{model_id}"

    @property
    def datasets(self):
        return self._db.collection(Collection.DATASETS.value)

    @property
    def dimensions(self):
        return self._db.collection(Collection.DIMENSIONS.value)

    @property
    def dimension_mappings(self):
        return self._db.collection(Collection.DIMENSION_MAPPINGS.value)

    @property
    def projects(self):
        return self._db.collection(Collection.PROJECTS.value)


class DatasetRegistryInterface(RegistryInterfaceBase):
    """Interface to database for datasets"""

    @staticmethod
    def _collection_name():
        return Collection.DATASETS.value

    def _get_model_id(self, model):
        return model.dataset_id

    def _get_model_id_field(self):
        return "dataset_id"

    def _make_dsgrid_model(self, db_data: dict):
        return DatasetConfigModel(**db_data)

    @staticmethod
    def _get_root_class():
        return DatasetRegistryModel

    def _insert_contains_edges(self, model):
        dim_intf = DimensionRegistryInterface(self._db)
        for ref in model.dimension_references:
            dim = dim_intf.get_by_version(ref.dimension_id, ref.version)
            self._db.insert_contains_edge(model.id, dim.id)

    @staticmethod
    def _make_root_object(model_id):
        return DatasetRegistryModel(dataset_id=model_id, registry_type=RegistryType.DATASET.value)

    def delete_all(self, model_id):
        super().delete_all(model_id)
        collection_name = f"{Collection.DATASET_DATA_ROOTS.value}/"
        db_id = f"{collection_name}/{model_id}"
        self._db.delete_document(db_id)

    @staticmethod
    def root_collection_name():
        return Collection.DATASET_ROOTS.value

    def _post_insert_handling(self, model: DatasetConfigModel, root):
        registration = self._db.get_initial_registration(root.id)
        reg_data = _serialize_new_model(registration)

        root_data = {"_key": model.dataset_id}
        root_res = self._db.collection(Collection.DATASET_DATA_ROOTS.value).insert(root_data)

        ddata = {"version": registration.version}
        dd_res = self._db.collection(Collection.DATASET_DATA.value).insert(ddata)
        ddata.update(dd_res)
        self._db.insert_edge(root_res["_id"], ddata["_id"], reg_data, Edge.UPDATED_TO)

    @staticmethod
    def _uses_model_id_in_db():
        return True


class DimensionRegistryInterface(RegistryInterfaceBase):
    """Interface to database for dimensions"""

    @staticmethod
    def _collection_name():
        return Collection.DIMENSIONS.value

    def _get_model_id(self, model):
        return model.dimension_id

    def _get_model_id_field(self):
        return "dimension_id"

    def _make_dsgrid_model(self, db_data: dict):
        return handle_dimension_union([db_data])[0]

    @staticmethod
    def _get_root_class():
        return RegistryModel

    @staticmethod
    def _make_root_object(_):
        return RegistryModel(registry_type=RegistryType.DIMENSION.value)

    def _post_insert_handling(self, model, root):
        self._insert_of_type_edge(root, model.dimension_type)

    @staticmethod
    def root_collection_name():
        return Collection.DIMENSION_ROOTS.value

    def _get_dimension_type_id(self, dimension_type: DimensionType):
        val = dimension_type.value
        return self._db.collection("dimension_types").get(val)["_id"]

    def _insert_of_type_edge(self, dim_reg, dimension_type: DimensionType):
        to_id = self._get_dimension_type_id(dimension_type)
        self._db.collection("of_type").insert({"_from": dim_reg.id, "_to": to_id})

    @staticmethod
    def _uses_model_id_in_db():
        return False


class DimensionMappingRegistryInterface(RegistryInterfaceBase):
    """Interface to database for dimension mappings"""

    @staticmethod
    def _collection_name():
        return Collection.DIMENSION_MAPPINGS.value

    def _get_model_id(self, model):
        return model.mapping_id

    def _get_model_id_field(self):
        return "mapping_id"

    def _make_dsgrid_model(self, db_data: dict):
        return MappingTableModel(**db_data)

    @staticmethod
    def _get_root_class():
        return RegistryModel

    def _insert_contains_edges(self, model):
        dim_intf = DimensionRegistryInterface(self._db)
        for ref in (model.from_dimension, model.to_dimension):
            dim = dim_intf.get_by_version(ref.dimension_id, ref.version)
            self._db.insert_contains_edge(model.id, dim.id)

    @staticmethod
    def _make_root_object(_):
        return RegistryModel(registry_type=RegistryType.DIMENSION_MAPPING.value)

    @staticmethod
    def root_collection_name():
        return Collection.DIMENSION_MAPPING_ROOTS.value

    @staticmethod
    def _uses_model_id_in_db():
        return False


class ProjectRegistryInterface(RegistryInterfaceBase):
    """Interface to database for projects"""

    @staticmethod
    def _collection_name():
        return Collection.PROJECTS.value

    def _get_model_id(self, model):
        return model.project_id

    def _get_model_id_field(self):
        return "project_id"

    def _make_dsgrid_model(self, db_data: dict):
        return ProjectConfigModel(**db_data)

    @staticmethod
    def _get_root_class():
        return ProjectRegistryModel

    def _insert_contains_edges(self, model):
        dim_intf = DimensionRegistryInterface(self._db)
        dim_mapping_intf = DimensionMappingRegistryInterface(self._db)
        dataset_intf = DatasetRegistryInterface(self._db)

        for ref in itertools.chain(
            model.dimensions.base_dimension_references,
            model.dimensions.supplemental_dimension_references,
        ):
            dim = dim_intf.get_by_version(ref.dimension_id, ref.version)
            self._db.insert_contains_edge(model.id, dim.id)

        for ref in model.dimension_mappings.base_to_supplemental_references:
            mapping = dim_mapping_intf.get_by_version(ref.mapping_id, ref.version)
            self._db.insert_contains_edge(model.id, mapping.id)

        for dataset in model.datasets:
            if dataset.status == DatasetRegistryStatus.REGISTERED.value:
                dset = dataset_intf.get_by_version(dataset.dataset_id, dataset.version)
                self._db.insert_contains_edge(model.id, dset.id)

    @staticmethod
    def _make_root_object(model_id):
        return ProjectRegistryModel(project_id=model_id, registry_type=RegistryType.PROJECT.value)

    @staticmethod
    def root_collection_name():
        return Collection.PROJECT_ROOTS.value

    @staticmethod
    def _uses_model_id_in_db():
        return True


def _serialize_model(model):
    data = model.serialize()
    for field in ("_id", "_key", "_rev"):
        data.pop(field, None)
    return data


def _serialize_new_model(model):
    data = model.serialize()
    for field in ("_id", "id", "_key", "key", "_rev", "rev"):
        val = data.pop(field, None)
        if val is not None:
            raise Exception(f"{field}={val} cannot be set on a new registry insertion")
    return data
