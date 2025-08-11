"""Manages the registry for dimensions"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Generator, Sequence, Union
from uuid import uuid4

from prettytable import PrettyTable
from sqlalchemy import Connection

from dsgrid.config.dimension_config_factory import get_dimension_config, load_dimension_config
from dsgrid.config.dimension_config import (
    DimensionBaseConfig,
    DimensionBaseConfigWithFiles,
    DimensionConfig,
)
from dsgrid.config.dimensions_config import DimensionsConfig
from dsgrid.config.dimensions import (
    DimensionBaseModel,
    DimensionModel,
    TimeDimensionBaseModel,
    DimensionReferenceModel,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.common import ConfigKey, RegistryType, VersionUpdateType
from dsgrid.registry.registry_interface import DimensionRegistryInterface
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import display_table
from .registration_context import RegistrationContext
from .dimension_update_checker import DimensionUpdateChecker
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionRegistryManager(RegistryManagerBase):
    """Manages registered dimensions."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._dimensions = {}  # key = ConfigKey, value = DimensionConfig

    @staticmethod
    def config_class():
        return DimensionConfig

    @property
    def db(self) -> DimensionRegistryInterface:
        return self._db

    @db.setter
    def db(self, db: DimensionRegistryInterface) -> None:
        self._db = db

    @staticmethod
    def name() -> str:
        return "Dimensions"

    def _replace_duplicates(
        self, config: DimensionsConfig, context: RegistrationContext
    ) -> set[str]:
        hashes = defaultdict(list)
        time_dims = {}
        for dimension in self._db.iter_models(context.connection, all_versions=True):
            if isinstance(dimension, TimeDimensionBaseModel):
                time_dims[dimension.id] = dimension
            else:
                assert isinstance(dimension, DimensionModel)
                hashes[dimension.file_hash].append(dimension)

        existing_ids = set()
        for i, dim in enumerate(config.model.dimensions):
            replace_dim = False
            existing = None
            if isinstance(dim, TimeDimensionBaseModel):
                existing = self._get_matching_time_dimension(time_dims.values(), dim)
                if existing is not None:
                    replace_dim = True
            elif dim.file_hash in hashes:
                for existing in hashes[dim.file_hash]:
                    if (
                        dim.dimension_type == existing.dimension_type
                        and dim.name == existing.name
                        and dim.description == existing.description
                    ):
                        replace_dim = True
                        break
                if not replace_dim:
                    logger.info(
                        "Register new dimension even though records are duplicate with "
                        "one or more existing dimensions. New name=%s",
                        dim.name,
                    )
            if replace_dim:
                assert existing is not None
                logger.info(
                    "Replace %s with existing dimension ID %s", dim.name, existing.dimension_id
                )
                config.model.dimensions[i] = existing
                existing_ids.add(existing.dimension_id)
        return existing_ids

    @staticmethod
    def _get_matching_time_dimension(existing_dims, new_dim):
        for time_dim in existing_dims:
            if type(time_dim) is not type(new_dim):
                continue
            match = True
            exclude = set(("dimension_id", "version", "id"))
            for field in type(new_dim).model_fields:
                if field not in exclude and getattr(new_dim, field) != getattr(time_dim, field):
                    match = False
                    break
            if match:
                return time_dim

        return None

    def get_by_id(
        self, config_id: str, version: str | None = None, conn: Connection | None = None
    ) -> DimensionBaseConfig:
        if version is None:
            version = self._db.get_latest_version(conn, config_id)

        key = ConfigKey(config_id, version)
        dimension = self._dimensions.get(key)
        if dimension is not None:
            return dimension

        if version is None:
            model = self.db.get_latest(conn, config_id)
        else:
            model = self.db.get_by_version(conn, config_id, version)

        config = get_dimension_config(model)
        self._dimensions[key] = config
        return config

    def list_ids(
        self,
        conn: Connection | None = None,
        dimension_type: DimensionType | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Return the dimension ids for the given type.

        Parameters
        ----------
        dimension_type
            If not provided, return all dimension ids.

        Returns
        -------
        list

        """
        if dimension_type is None:
            ids = super().list_ids(conn)
        else:
            ids = [
                x.dimension_id  # type: ignore
                for x in self.db.iter_models(
                    conn, filter_config={"dimension_type": dimension_type}
                )
            ]
        ids.sort()
        return ids

    def load_dimensions(
        self,
        dimension_references: Sequence[DimensionReferenceModel],
        conn: Connection | None = None,
    ) -> dict[ConfigKey, DimensionBaseConfig]:
        """Load dimensions from the database.

        Parameters
        ----------
        dimension_references
        conn
            Connection to the database, optional. If not provided, a new connection will be created.

        Returns
        -------
        dict
            ConfigKey to DimensionConfig
        """
        dimensions = {}
        for dim in dimension_references:
            key = ConfigKey(dim.dimension_id, dim.version)
            dimensions[key] = self.get_by_id(dim.dimension_id, version=dim.version, conn=conn)

        return dimensions

    def register_from_config(
        self,
        config: DimensionsConfig,
        context: RegistrationContext,
    ) -> list[str]:
        return self._register(config, context)

    def register(self, config_file: Path, submitter: str, log_message: str) -> list[str]:
        with RegistrationContext(
            self.db, log_message, VersionUpdateType.MAJOR, submitter
        ) as context:
            config = DimensionsConfig.load(config_file)
            return self.register_from_config(config, context=context)

    def _register(self, config, context: RegistrationContext) -> list[str]:
        existing_ids = self._replace_duplicates(config, context)
        registered_dimension_ids = []

        # This function will either register the dimension specified by each model or re-use an
        # existing ID. The returned list must be in the same order as the list of models.
        final_dimension_ids = []
        for dim in config.model.dimensions:
            if dim.id is None:
                assert dim.dimension_id is None
                dim.dimension_id = str(uuid4())
                dim.version = "1.0.0"
                dim = self.db.insert(context.connection, dim, context.registration)
                assert isinstance(dim, DimensionBaseModel)
                final_dimension_ids.append(dim.dimension_id)
                registered_dimension_ids.append(dim.dimension_id)
                logger.info(
                    "%s Registered dimension id=%s type=%s version=%s name=%s",
                    self._log_offline_mode_prefix(),
                    dim.id,
                    dim.dimension_type.value,
                    dim.version,
                    dim.name,
                )
            else:
                if dim.dimension_id not in existing_ids:
                    msg = f"Bug: {dim.dimension_id=} should have been in existing_ids"
                    raise Exception(msg)
                final_dimension_ids.append(dim.dimension_id)

        logger.info("Registered %s dimensions", len(config.model.dimensions))
        context.add_ids(RegistryType.DIMENSION, registered_dimension_ids, self)
        return final_dimension_ids

    def make_dimension_references(self, conn: Connection, dimension_ids: list[str]):
        """Return a list of dimension references from a list of registered dimension IDs.
        This assumes that the latest version of the dimensions will be used because they were
        just created.

        Parameters
        ----------
        dimension_ids : list[str]

        """
        refs = []
        for dim_id in dimension_ids:
            dim = self.db.get_latest(conn, dim_id)
            assert isinstance(dim, DimensionBaseModel)
            assert isinstance(dim.version, str)
            refs.append(
                DimensionReferenceModel(
                    dimension_id=dim_id,
                    type=dim.dimension_type,
                    version=dim.version,
                )
            )
        return refs

    def find_matching_dimensions(
        self, sorted_record_ids: list[str], dimension_type: DimensionType
    ) -> Generator[DimensionBaseConfigWithFiles, None, None]:
        """Yield all dimensions that match the given record IDs and dimension type."""
        with self.db.engine.connect() as conn:
            filter_config = {"dimension_type": dimension_type}
            for model in self.db.iter_models(filter_config=filter_config, conn=conn):
                assert isinstance(model, DimensionBaseModel)
                config = self.get_by_id(model.dimension_id, conn=conn)
                if sorted_record_ids == sorted(config.get_unique_ids()):
                    yield config

    def show(
        self,
        conn: Connection | None = None,
        filters: list[str] | None = None,
        max_width: Union[int, dict] | None = None,
        drop_fields: list[str] | None = None,
        dimension_ids: set[str] | None = None,
        return_table: bool = False,
        **kwargs,
    ):
        """Show registry in PrettyTable

        Parameters
        ----------
        filters : list or tuple
            List of filter expressions for reigstry content (e.g., filters=["Submitter==USER", "Description contains comstock"])
        max_width
            Max column width in PrettyTable, specify as a single value or as a dict of values by field name
        drop_fields
            List of field names not to show

        """

        if filters:
            logger.info("List registered dimensions for: %s", filters)

        table = PrettyTable(title="Dimensions")
        all_field_names = (
            "Type",
            "Query Name",
            "ID",
            "Version",
            "Date",
            "Submitter",
            "Description",
        )
        if drop_fields is None:
            table.field_names = all_field_names
        else:
            table.field_names = tuple(x for x in all_field_names if x not in drop_fields)

        if max_width is None:
            table._max_width = {
                "ID": 40,
                "Date": 10,
                "Description": 40,
            }
        if isinstance(max_width, int):
            table.max_width = max_width
        elif isinstance(max_width, dict):
            table._max_width = max_width

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for model in self.db.iter_models(conn):
            registration = self.db.get_registration(conn, model)
            if dimension_ids and model.dimension_id not in dimension_ids:
                continue

            all_fields = (
                model.dimension_type.value,
                model.name,
                model.dimension_id,
                model.version,
                registration.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                registration.submitter,
                registration.log_message,
            )
            if drop_fields is None:
                row = all_fields
            else:
                row = tuple(
                    y for (x, y) in zip(all_field_names, all_fields) if x not in drop_fields
                )

            if not filters or matches_filters(row, field_to_index, transformed_filters):
                rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        table.align = "l"
        if return_table:
            return table
        display_table(table)

    def update_from_file(
        self,
        config_file: Path,
        dimension_id: str,
        submitter: str,
        update_type: VersionUpdateType,
        log_message: str,
        version: str,
    ):
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = load_dimension_config(config_file)
            self._check_update(context.connection, config, dimension_id, version)
            self.update_with_context(config, context)

    def update(
        self,
        config,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: str | None = None,
    ) -> DimensionConfig:
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            return self.update_with_context(config, context)

    def update_with_context(self, config, context: RegistrationContext) -> DimensionConfig:
        old_config = self.get_by_id(config.model.dimension_id, conn=context.connection)
        checker = DimensionUpdateChecker(old_config.model, config.model)
        checker.run()
        cur_version = old_config.model.version
        old_key = ConfigKey(config.model.dimension_id, cur_version)
        model = self._update_config(config, context)
        new_key = ConfigKey(model.dimension_id, model.version)
        self._dimensions.pop(old_key, None)
        self._dimensions[new_key] = get_dimension_config(model)
        return self._dimensions[new_key]

    def finalize_registration(self, conn: Connection, config_ids: set[str], error_occurred: bool):
        if error_occurred:
            for key in [x for x in self._dimensions if x.id in config_ids]:
                self._dimensions.pop(key)

    def remove(self, dimension_id, conn: Connection | None = None):
        self.db.delete_all(conn, dimension_id)
        for key in [x for x in self._dimensions if x.id == dimension_id]:
            self._dimensions.pop(key)

        logger.info("Removed %s from the registry.", dimension_id)
