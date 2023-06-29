"""Manages the registry for dimensions"""

import getpass
import logging
from typing import Union

from prettytable import PrettyTable

from dsgrid.config.dimension_config_factory import get_dimension_config, load_dimension_config
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.dimensions_config import DimensionsConfig
from dsgrid.config.dimensions import (
    TimeDimensionBaseModel,
    DimensionReferenceModel,
)
from dsgrid.registry.common import ConfigKey, make_initial_config_registration
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import display_table
from .common import RegistryType
from .registration_context import RegistrationContext
from .dimension_update_checker import DimensionUpdateChecker
from .registry_interface import DimensionRegistryInterface
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
    def db(self, db: DimensionRegistryInterface):
        self._db = db

    @staticmethod
    def name():
        return "Dimensions"

    def _replace_duplicates(self, config: DimensionsConfig):
        hashes = {}
        time_dims = {}
        for dimension in self._db.iter_models(all_versions=True):
            if isinstance(dimension, TimeDimensionBaseModel):
                time_dims[dimension.id] = dimension
            else:
                hashes[dimension.file_hash] = dimension

        # TODO: This only works if the matching dimension is the latest.
        existing_ids = set()
        for i, dim in enumerate(config.model.dimensions):
            replace_dim = False
            existing = None
            if isinstance(dim, TimeDimensionBaseModel):
                existing = self._get_matching_time_dimension(time_dims.values(), dim)
                if existing is not None:
                    replace_dim = True
            elif dim.file_hash in hashes:
                existing = hashes[dim.file_hash]
                if dim.dimension_type == existing.dimension_type:
                    if dim.name == existing.name and dim.display_name == existing.display_name:
                        replace_dim = True
                    else:
                        logger.info(
                            "Register new dimension even though records are duplicate: %s",
                            dim.name,
                        )
            if replace_dim:
                logger.info(
                    "Replace %s with existing dimension ID %s", dim.name, existing.dimension_id
                )
                config.model.dimensions[i] = existing
                existing_ids.add(existing.dimension_id)
        return existing_ids

    @staticmethod
    def _get_matching_time_dimension(existing_dims, new_dim):
        for time_dim in existing_dims:
            if type(time_dim) != type(new_dim):
                continue
            match = True
            exclude = set(("description", "dimension_id", "key", "id", "rev", "version"))
            for field in type(new_dim).__fields__:
                if field not in exclude and getattr(new_dim, field) != getattr(time_dim, field):
                    match = False
                    break
            if match:
                return time_dim

        return None

    def finalize_registration(self, config_ids: list[str], error_occurred: bool):
        if error_occurred:
            logger.info("Remove all intermediate dimensions after error")
            for dimension_id in config_ids:
                self.remove(dimension_id)

    def get_by_id(self, config_id, version=None):
        if version is None:
            version = self._db.get_latest_version(config_id)

        key = ConfigKey(config_id, version)
        dimension = self._dimensions.get(key)
        if dimension is not None:
            return dimension

        if version is None:
            model = self.db.get_latest(config_id)
        else:
            model = self.db.get_by_version(config_id, version)

        config = get_dimension_config(model)
        self._dimensions[key] = config
        return config

    def list_ids(self, dimension_type=None):
        """Return the dimension ids for the given type.

        Parameters
        ----------
        dimension_type : DimensionType

        Returns
        -------
        list

        """
        if dimension_type is None:
            ids = list(self.iter_ids())
        else:
            ids = [
                x.dimension_id
                for x in self.db.iter_models(filter_config={"dimension_type": dimension_type})
            ]
        ids.sort()
        return ids

    def load_dimensions(self, dimension_references):
        """Load dimensions from the database.

        Parameters
        ----------
        dimension_references : list
            iterable of DimensionReferenceModel instances


        Returns
        -------
        dict
            ConfigKey to DimensionConfig

        """
        dimensions = {}
        for dim in dimension_references:
            key = ConfigKey(dim.dimension_id, dim.version)
            dimensions[key] = self.get_by_id(dim.dimension_id, version=dim.version)

        return dimensions

    @track_timing(timer_stats_collector)
    def register_from_config(self, config: DimensionsConfig, submitter, log_message, context=None):
        error_occurred = False
        need_to_finalize = context is None
        if context is None:
            context = RegistrationContext()

        try:
            return self._register(config, submitter, log_message, context)
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    @track_timing(timer_stats_collector)
    def register(self, config_file, submitter, log_message):
        context = RegistrationContext()
        config = DimensionsConfig.load(config_file)
        return self.register_from_config(config, submitter, log_message, context=context)

    def _register(self, config, submitter, log_message, context):
        existing_ids = self._replace_duplicates(config)
        # TODO: check that id does not already exist in .dsgrid-registry

        registration = make_initial_config_registration(submitter, log_message)
        dimension_ids = []
        try:
            # Guarantee that registration of dimensions is all or none.
            for dim in config.model.dimensions:
                if dim.id is None:
                    dim = self.db.insert(dim, registration)
                else:
                    assert dim.dimension_id in existing_ids
                    continue
                logger.info(
                    "%s Registered dimension id=%s type=%s version=%s name=%s",
                    self._log_offline_mode_prefix(),
                    dim.id,
                    dim.dimension_type.value,
                    registration.version,
                    dim.name,
                )
                dimension_ids.append(dim.dimension_id)
        except Exception:
            if dimension_ids:
                logger.warning(
                    "Exception occured after partial completion of dimension registration."
                )
                for dimension_id in dimension_ids:
                    self.remove(dimension_id)
            raise

        logger.info(
            "Registered %s dimensions with version=%s",
            len(config.model.dimensions),
            registration.version,
        )

        context.add_ids(RegistryType.DIMENSION, dimension_ids, self)
        dimension_ids.extend(existing_ids)
        return dimension_ids

    def make_dimension_references(self, dimension_ids: list[str]):
        """Return a list of dimension references from a list of registered dimension IDs.
        This assumes that the latest version of the dimensions will be used because they were
        just created.

        Parameters
        ----------
        dimension_ids : list[str]

        """
        refs = []
        for dim_id in dimension_ids:
            dim = self.db.get_latest(dim_id)
            refs.append(
                DimensionReferenceModel(
                    dimension_id=dim_id,
                    dimension_type=dim.dimension_type,
                    version=dim.version,
                )
            )
        return refs

    def show(
        self,
        filters: list[str] = None,
        max_width: Union[int, dict] = None,
        drop_fields: list[str] = None,
        dimension_ids: set[str] = None,
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
        for model in self.db.iter_models():
            registration = self.db.get_registration(model)
            if dimension_ids and model.dimension_id not in dimension_ids:
                continue

            all_fields = (
                model.dimension_type.value,
                model.dimension_id,
                registration.version,
                registration.date.strftime("%Y-%m-%d %H:%M:%S"),
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
        self, config_file, dimension_id, submitter, update_type, log_message, version
    ):
        config = load_dimension_config(config_file)
        self._check_update(config, dimension_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    @track_timing(timer_stats_collector)
    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        old_config = self.get_by_id(config.model.dimension_id)
        checker = DimensionUpdateChecker(old_config.model, config.model)
        checker.run()
        cur_version = old_config.model.version
        old_key = ConfigKey(config.model.dimension_id, cur_version)
        model = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.model.dimension_id, model.version)
        self._dimensions.pop(old_key, None)
        self._dimensions[new_key] = get_dimension_config(model)
        return model

    def remove(self, dimension_id):
        self.db.delete_all(dimension_id)
        for key in [x for x in self._dimensions if x.id == dimension_id]:
            self._dimensions.pop(key)

        logger.info("Removed %s from the registry.", dimension_id)
