"""Manages the registry for dimension mappings"""

import logging
from collections import Counter

from pathlib import Path
from uuid import uuid4

import networkx as nx
from prettytable import PrettyTable
from sqlalchemy import Connection

from dsgrid.config.mapping_tables import MappingTableConfig
from dsgrid.config.dimension_mappings_config import DimensionMappingsConfig
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel
from dsgrid.exceptions import (
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
    DSGInvalidParameter,
)
from dsgrid.spark.types import F
from dsgrid.registry.registry_interface import DimensionMappingRegistryInterface
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.spark import models_to_dataframe
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import display_table
from .common import (
    ConfigKey,
    RegistryManagerParams,
    VersionUpdateType,
    RegistryType,
)
from .registration_context import RegistrationContext
from .dimension_mapping_update_checker import DimensionMappingUpdateChecker
from .dimension_registry_manager import DimensionRegistryManager
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class DimensionMappingRegistryManager(RegistryManagerBase):
    """Manages registered dimension mappings."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._mappings = {}  # ConfigKey to DimensionMappingConfig
        self._dimension_mgr = None

    @classmethod
    def load(cls, path, params: RegistryManagerParams, dimension_manager, db):
        mgr = cls._load(path, params)
        mgr.dimension_manager = dimension_manager
        mgr.db = db
        return mgr

    @staticmethod
    def config_class():
        return MappingTableConfig

    @property
    def db(self) -> DimensionMappingRegistryInterface:
        return self._db

    @db.setter
    def db(self, db: DimensionMappingRegistryInterface):
        self._db = db

    @staticmethod
    def name():
        return "Dimension Mappings"

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val: DimensionRegistryManager):
        self._dimension_mgr = val

    def _replace_duplicates(self, config: DimensionMappingsConfig, context: RegistrationContext):
        def make_key(model):
            return (
                model.from_dimension.dimension_id,
                model.to_dimension.dimension_id,
                model.file_hash,
            )

        hashes = {}
        for model in self.db.iter_models(context.connection, all_versions=True):
            key = make_key(model)
            if key in hashes:
                msg = f"Bug: the same file_hash exists in multiple mappings: {model.mapping_id} {key}"
                raise Exception(msg)
            hashes[key] = model

        existing_ids = set()
        for i, mapping in enumerate(config.model.mappings):
            key = make_key(mapping)
            existing = hashes.get(key)
            if existing is not None:
                logger.info(
                    "Replace mapping of %s to %s with existing mapping ID %s",
                    mapping.from_dimension.dimension_id,
                    mapping.to_dimension.dimension_id,
                    existing.mapping_id,
                )
                config.model.mappings[i] = existing
                existing_ids.add(existing.mapping_id)

        return existing_ids

    def _check_records_against_dimension_records(self, conn: Connection | None, config):
        """
        Check that records in mappings are subsets of from and to dimension records.
        """
        for mapping in config.model.mappings:
            actual_from_records = {x.from_id for x in mapping.records}
            from_dimension = self._dimension_mgr.get_by_id(
                mapping.from_dimension.dimension_id, conn=conn
            )
            allowed_from_records = from_dimension.get_unique_ids()
            diff = actual_from_records.difference(allowed_from_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                msg = (
                    f"Dimension mapping={mapping.filename} has invalid 'from_id' records: {diff}, "
                    f"they are missing from dimension_id={dim_id}"
                )
                raise DSGInvalidDimensionMapping(msg)

            # Note: this code cannot complete verify 'to' records. A dataset may be registering a
            # mapping to a project's dimension for a specific data source, but that information
            # is not available here.
            actual_to_records = {x.to_id for x in mapping.records}
            to_dimension = self._dimension_mgr.get_by_id(
                mapping.to_dimension.dimension_id, conn=conn
            )
            allowed_to_records = to_dimension.get_unique_ids()
            if None in actual_to_records:
                actual_to_records.remove(None)
            diff = actual_to_records.difference(allowed_to_records)
            if diff:
                dim_id = from_dimension.model.dimension_id
                msg = (
                    f"Dimension mapping={mapping.filename} has invalid 'to_id' records: {diff}, "
                    f"they are missing from dimension_id={dim_id}"
                )
                raise DSGInvalidDimensionMapping(msg)

    def validate_records(self, config: DimensionMappingsConfig):
        """Validate dimension mapping records.

        Check:
        - duplicate records in from_id and to_id columns per mapping archetype
        - sum of from_fraction by from_id per mapping archetype
        - sum of from_fraction by to_id per mapping archetype
        - special check for mapping_type=duplication

        """
        for mapping in config.model.mappings:
            actual_from_records = [x.from_id for x in mapping.records]
            self._check_for_duplicates_in_list(
                actual_from_records,
                mapping.archetype.allow_dup_from_records,
                "from_id",
                mapping.filename,
                mapping.mapping_type.value,
            )
            actual_to_records = [x.to_id for x in mapping.records if x.to_id is not None]

            self._check_for_duplicates_in_list(
                actual_to_records,
                mapping.archetype.allow_dup_to_records,
                "to_id",
                mapping.filename,
                mapping.mapping_type.value,
            )

            if mapping.archetype.check_fraction_sum_eq1_from_id:
                self._check_fraction_sum(
                    mapping.records,
                    mapping.filename,
                    mapping.mapping_type.value,
                    tolerance=mapping.from_fraction_tolerance,
                    group_by="from_id",
                )
            if mapping.archetype.check_fraction_sum_eq1_to_id:
                self._check_fraction_sum(
                    mapping.records,
                    mapping.filename,
                    mapping.mapping_type.value,
                    tolerance=mapping.to_fraction_tolerance,
                    group_by="to_id",
                )

            if mapping.mapping_type.value == "duplication":
                fractions = {x.from_fraction for x in mapping.records}
                if not (len(fractions) == 1 and 1 in fractions):
                    msg = (
                        f"dimension_mapping={mapping.filename} has mapping_type={mapping.mapping_type.value}, "
                        f"which does not allow non-one from_fractions. "
                        "\nConsider removing from_fraction column or using mapping_type: 'one_to_many_explicit_multipliers'. "
                    )
                    raise DSGInvalidDimensionMapping(msg)

    @staticmethod
    def _check_for_duplicates_in_list(
        lst: list, allow_dup: bool, id_type: str, mapping_name: str, mapping_type: str
    ):
        """Check list for duplicates"""
        dups = [x for x, n in Counter(lst).items() if n > 1]
        if len(dups) > 0 and not allow_dup:
            msg = (
                f"dimension_mapping={mapping_name} has mapping_type={mapping_type}, "
                f"which does not allow duplicated {id_type} records. \nDuplicated {id_type}={dups}. "
            )
            raise DSGInvalidDimensionMapping(msg)

    @staticmethod
    def _check_fraction_sum(
        mapping_records, mapping_name, mapping_type, tolerance, group_by="from_id"
    ):
        mapping_df = models_to_dataframe(mapping_records)
        mapping_sum_df = (
            mapping_df.groupBy(group_by)
            .agg(F.sum("from_fraction").alias("sum_fraction"))
            .sort("sum_fraction", group_by)
        )
        fracs_greater_than_one = mapping_sum_df.filter((F.col("sum_fraction") - 1.0) > tolerance)
        fracs_less_than_one = mapping_sum_df.filter(1.0 - F.col("sum_fraction") > tolerance)
        if fracs_greater_than_one.count() > 0:
            id_greater_than_one = {
                x[group_by] for x in fracs_greater_than_one[[group_by]].distinct().collect()
            }
            msg = (
                f"dimension_mapping={mapping_name} has mapping_type={mapping_type} and a "
                f"tolerance of {tolerance}, which does not allow from_fraction sum <> 1. "
                f"Mapping contains from_fraction sum greater than 1 for {group_by}={id_greater_than_one}. "
            )
            raise DSGInvalidDimensionMapping(msg)
        elif fracs_less_than_one.count() > 0:
            id_less_than_one = {
                x[group_by] for x in fracs_less_than_one[[group_by]].distinct().collect()
            }
            msg = (
                f"dimension_mapping={mapping_name} has mapping_type={mapping_type} and a"
                f" tolerance of {tolerance}, which does not allow from_fraction sum <> 1. "
                f"Mapping contains from_fraction sum less than 1 for {group_by}={id_less_than_one}. "
            )
            raise DSGInvalidDimensionMapping(msg)

    def get_by_id(
        self, mapping_id, version=None, conn: Connection | None = None
    ) -> MappingTableConfig:
        if version is None:
            version = self._db.get_latest_version(conn, mapping_id)

        key = ConfigKey(mapping_id, version)
        mapping = self._mappings.get(key)
        if mapping is not None:
            return mapping

        if version is None:
            model = self.db.get_latest(conn, mapping_id)
        else:
            model = self.db.get_by_version(conn, mapping_id, version)

        config = MappingTableConfig(model)
        self._mappings[key] = config
        return config

    def build_graph(self, conn: Connection | None = None) -> nx.Graph:
        """Build a graph of dimension mappings"""
        if conn is None:
            with self.db.engine.connect() as conn:
                return self._build_graph(conn)
        else:
            return self._build_graph(conn)

    def _build_graph(self, conn: Connection) -> nx.Graph:
        graph = nx.Graph()
        for model in self.db.iter_models(conn):
            graph.add_edge(model.from_dimension.dimension_id, model.to_dimension.dimension_id)
        return graph

    def list_mappings_between_dimensions(
        self, graph: nx.Graph, from_dimension_id: str, to_dimension_id: str
    ) -> list[DimensionMappingReferenceModel]:
        """List all mappings between two dimensions"""
        if not nx.has_path(graph, from_dimension_id, to_dimension_id):
            msg = f"There is no path between {from_dimension_id=} and {to_dimension_id=}"
            raise DSGInvalidDimensionMapping(msg)
        path = nx.shortest_path(graph, from_dimension_id, to_dimension_id)
        assert len(path) >= 2
        return [
            self.get_mapping_with_dimension_ids(path[i - 1], path[i]) for i in range(1, len(path))
        ]

    def get_mapping_with_dimension_ids(
        self, from_dimension_id: str, to_dimension_id: str, conn: Connection | None = None
    ) -> DimensionMappingReferenceModel:
        """Return a dimension mapping with the specified from and to dimension IDs.
        Only looks at the latest versions of the mappings.
        """
        valid_mappings: list[DimensionMappingReferenceModel] = []
        for mapping in self.db.iter_models(conn):
            if (
                mapping.from_dimension.dimension_id == from_dimension_id
                and mapping.to_dimension.dimension_id == to_dimension_id
            ):
                valid_mappings.append(
                    DimensionMappingReferenceModel(
                        from_dimension_type=mapping.from_dimension.dimension_type,
                        to_dimension_type=mapping.to_dimension.dimension_type,
                        mapping_id=mapping.mapping_id,
                        version=mapping.version,
                    )
                )
        if not valid_mappings:
            msg = f"No dimension mapping found with {from_dimension_id=} and {to_dimension_id=}"
            raise DSGInvalidParameter(msg)
        if len(valid_mappings) > 1:
            msg = (
                f"Multiple dimension mappings found with {from_dimension_id=} and "
                f"{to_dimension_id=} {valid_mappings=}"
            )
            raise DSGInvalidParameter(msg)
        return valid_mappings[0]

    def load_dimension_mappings(
        self,
        dimension_mapping_references: list[DimensionMappingReferenceModel],
        conn: Connection | None = None,
    ) -> dict[ConfigKey, MappingTableConfig]:
        """Load dimension_mappings from files.

        Parameters
        ----------
        dimension_mapping_references : list
            iterable of DimensionMappingReferenceModel instances

        Returns
        -------
        dict
            ConfigKey to DimensionMappingConfig

        """
        mappings: dict[ConfigKey, MappingTableConfig] = {}
        for ref in dimension_mapping_references:
            key = ConfigKey(ref.mapping_id, ref.version)
            mappings[key] = self.get_by_id(key.id, version=key.version, conn=conn)

        return mappings

    def make_dimension_mapping_references(
        self, mapping_ids: list[str], conn: Connection | None = None
    ) -> list[DimensionMappingReferenceModel]:
        """Return a list of dimension mapping references from a list of registered mapping IDs.

        Parameters
        ----------
        mapping_ids : list[str]

        Returns
        -------
        list[DimensionMappingReferenceModel]

        """
        refs = []
        for mapping_id in mapping_ids:
            mapping = self.db.get_latest(conn, mapping_id)
            refs.append(
                DimensionMappingReferenceModel(
                    from_dimension_type=mapping.from_dimension.dimension_type,
                    to_dimension_type=mapping.to_dimension.dimension_type,
                    mapping_id=mapping_id,
                    version=mapping.version,
                )
            )
        return refs

    def register(self, config_file, submitter, log_message):
        with RegistrationContext(
            self.db, log_message, VersionUpdateType.MAJOR, submitter
        ) as context:
            config = DimensionMappingsConfig.load(config_file)
            return self.register_from_config(config, context)

    def register_from_config(
        self,
        config: DimensionMappingsConfig,
        context: RegistrationContext,
    ):
        return self._register(config, context)

    def _register(self, config, context: RegistrationContext):
        conn = context.connection
        existing_ids = self._replace_duplicates(config, context)
        self._check_records_against_dimension_records(conn, config)
        self.validate_records(config)

        dimension_mapping_ids = []
        for mapping in config.model.mappings:
            from_id = mapping.from_dimension.dimension_id
            to_id = mapping.to_dimension.dimension_id
            if not self.dimension_manager.has_id(from_id, conn=conn):
                msg = f"from_dimension ID {from_id} is not registered"
                raise DSGValueNotRegistered(msg)
            if not self.dimension_manager.has_id(to_id, conn=conn):
                msg = f"to_dimension ID {to_id} is not registered"
                raise DSGValueNotRegistered(msg)

            if mapping.id is None:
                assert mapping.mapping_id is None
                mapping.mapping_id = str(uuid4())
                mapping.version = "1.0.0"
                mapping = self.db.insert(conn, mapping, context.registration)
            else:
                assert mapping.mapping_id in existing_ids
                continue
            logger.info(
                "%s Registered dimension mapping id=%s version=%s",
                self._log_offline_mode_prefix(),
                mapping.mapping_id,
                mapping.version,
            )
            dimension_mapping_ids.append(mapping.mapping_id)

        context.add_ids(RegistryType.DIMENSION_MAPPING, dimension_mapping_ids, self)
        dimension_mapping_ids.extend(existing_ids)
        return dimension_mapping_ids

    def update_from_file(
        self,
        config_file: Path,
        mapping_id: str,
        submitter: str,
        update_type: VersionUpdateType,
        log_message: str,
        version: str,
    ):
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = MappingTableConfig.load(config_file)
            self._check_update(context.connection, config, mapping_id, version)
            self.update_with_context(config, context)

    @track_timing(timer_stats_collector)
    def update(
        self,
        config: MappingTableConfig,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: str | None = None,
    ) -> MappingTableConfig:
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            return self.update_with_context(config, context)

    def update_with_context(
        self, config: MappingTableConfig, context: RegistrationContext
    ) -> MappingTableConfig:
        conn = context.connection
        old_config = self.get_by_id(config.model.mapping_id, conn=conn)
        checker = DimensionMappingUpdateChecker(old_config.model, config.model)
        checker.run()
        cur_version = old_config.model.version
        old_key = ConfigKey(config.model.mapping_id, cur_version)
        model = self._update_config(config, context)
        new_key = ConfigKey(config.model.mapping_id, model.version)
        self._mappings.pop(old_key, None)
        self._mappings[new_key] = MappingTableConfig(model)

        if not self.offline_mode:
            self.sync_push(self._path)

        return self._mappings[new_key]

    def finalize_registration(self, conn: Connection, config_ids: set[str], error_occurred: bool):
        if error_occurred:
            for key in [x for x in self._mappings if x.id in config_ids]:
                self._mappings.pop(key)

    def remove(self, mapping_id: str, conn: Connection | None = None):
        self.db.delete_all(conn, mapping_id)
        for key in [x for x in self._mappings if x.id == mapping_id]:
            self._mappings.pop(key)

    def show(
        self,
        conn: Connection | None = None,
        filters: list[str] | None = None,
        max_width: int | dict | None = None,
        drop_fields: list[str] | None = None,
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
            logger.info("List registered dimension_mappings for: %s", filters)

        table = PrettyTable(title="Dimension Mappings")
        all_field_names = (
            "Type [From, To]",
            "ID",
            "From ID",
            "To ID",
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
                "From ID": 40,
                "To ID": 40,
                "Date": 10,
                "Description": 34,
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
            from_dim = model.from_dimension.dimension_type.value
            to_dim = model.to_dimension.dimension_type.value
            all_fields = (
                f"[{from_dim}, {to_dim}]",
                model.mapping_id,
                model.from_dimension.dimension_id,
                model.to_dimension.dimension_id,
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
