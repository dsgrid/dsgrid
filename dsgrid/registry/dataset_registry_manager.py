"""Manages the registry for dimension datasets"""

import logging
import os
from pathlib import Path
from typing import Optional, Type, Union

from prettytable import PrettyTable
from sqlalchemy import Connection

from dsgrid.config.dataset_config import (
    DatasetConfig,
    ALLOWED_DATA_FILES,
    ALLOWED_LOAD_DATA_FILENAMES,
)
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dimensions_config import DimensionsConfig, DimensionsConfigModel
from dsgrid.dataset.models import TableFormatType, UnpivotedTableFormatModel
from dsgrid.dimension.base_models import DimensionType, check_required_dimensions
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.registry.registry_interface import DatasetRegistryInterface
from dsgrid.utils.dataset import unpivot_dataframe
from dsgrid.utils.spark import (
    read_dataframe,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import check_uniqueness, display_table
from .common import (
    VersionUpdateType,
    ConfigKey,
    RegistryType,
)
from .registration_context import RegistrationContext
from .registry_manager_base import RegistryManagerBase

logger = logging.getLogger(__name__)


class DatasetRegistryManager(RegistryManagerBase):
    """Manages registered dimension datasets."""

    def __init__(
        self,
        path,
        fs_interface,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: DatasetRegistryInterface,
    ):
        super().__init__(path, fs_interface)
        self._datasets: dict[ConfigKey, DatasetConfig] = {}
        self._dimension_mgr = dimension_manager
        self._dimension_mapping_mgr = dimension_mapping_manager
        self._db = db

    @classmethod
    def load(
        cls,
        path: Path,
        params,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: DatasetRegistryInterface,
    ):
        return cls._load(path, params, dimension_manager, dimension_mapping_manager, db)

    @staticmethod
    def config_class() -> Type:
        return DatasetConfig

    @property
    def db(self) -> DatasetRegistryInterface:
        return self._db

    @staticmethod
    def name() -> str:
        return "Datasets"

    def _get_registry_data_path(self):
        if self._params.use_remote_data:
            dataset_path = self._params.remote_path
        else:
            dataset_path = str(self._params.base_path)
        return dataset_path

    @track_timing(timer_stats_collector)
    def _run_checks(self, conn: Connection, config: DatasetConfig):
        logger.info("Run dataset registration checks.")
        check_required_dimensions(config.model.dimension_references, "dataset dimensions")
        check_uniqueness((x.model.name for x in config.model.dimensions), "dimension name")
        if not os.environ.get("__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__"):
            self._check_dataset_consistency(conn, config)

    def _check_dataset_consistency(self, conn: Connection, config: DatasetConfig):
        schema_handler = make_dataset_schema_handler(
            conn, config, self._dimension_mgr, self._dimension_mapping_mgr
        )
        schema_handler.check_consistency()

    @property
    def dimension_manager(self) -> DimensionRegistryManager:
        return self._dimension_mgr

    @property
    def dimension_mapping_manager(self) -> DimensionMappingRegistryManager:
        return self._dimension_mapping_mgr

    def finalize_registration(self, conn: Connection, config_ids: set[str], error_occurred: bool):
        assert len(config_ids) == 1, config_ids
        if error_occurred:
            for dataset_id in config_ids:
                logger.info("Remove intermediate dataset after error")
                self.remove_data(dataset_id, conn=conn)
            for key in [x for x in self._datasets if x.id in config_ids]:
                self._datasets.pop(key)

        if not self.offline_mode:
            for dataset_id in config_ids:
                lock_file = self.get_registry_lock_file(dataset_id)
                self.cloud_interface.check_lock_file(lock_file)
                if not error_occurred:
                    self.sync_push(self.get_registry_data_directory(dataset_id))
                self.cloud_interface.remove_lock_file(lock_file)

    def get_by_id(self, dataset_id: str, version=None, conn: Optional[Connection] = None):
        if version is None:
            version = self._db.get_latest_version(conn, dataset_id)

        key = ConfigKey(dataset_id, version)
        dataset = self._datasets.get(key)
        if dataset is not None:
            return dataset

        if version is None:
            model = self.db.get_latest(conn, dataset_id)
        else:
            model = self.db.get_by_version(conn, dataset_id, version)

        dataset_path = self._get_registry_data_path()
        config = DatasetConfig.load_from_registry(model, dataset_path)
        self._update_dimensions(conn, config)
        self._datasets[key] = config
        return config

    def acquire_registry_locks(self, config_ids: list[str]):
        for dataset_id in config_ids:
            lock_file = self.get_registry_lock_file(dataset_id)
            self.cloud_interface.make_lock_file(lock_file)

    def get_registry_lock_file(self, config_id: str):
        return f"configs/.locks/{config_id}.lock"

    def _update_dimensions(self, conn: Optional[Connection], config: DatasetConfig):
        dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimension_references, conn=conn
        )
        config.update_dimensions(dimensions)

    def register(
        self,
        config_file: Path,
        dataset_path: Path,
        submitter: Optional[str] = None,
        log_message: Optional[str] = None,
        context: Optional[RegistrationContext] = None,
    ):
        config = DatasetConfig.load_from_user_path(config_file, dataset_path)
        if context is None:
            assert submitter is not None
            assert log_message is not None
            with RegistrationContext(
                self.db, log_message, VersionUpdateType.MAJOR, submitter
            ) as context:
                return self.register_from_config(config, dataset_path, context)
        else:
            return self.register_from_config(config, dataset_path, context)

    @track_timing(timer_stats_collector)
    def register_from_config(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        context: RegistrationContext,
    ):
        self._update_dimensions(context.connection, config)
        self._register_dataset_and_dimensions(
            config,
            dataset_path,
            context,
        )

    def _register_dataset_and_dimensions(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        context: RegistrationContext,
    ):
        logger.info("Start registration of dataset %s", config.model.dataset_id)
        # TODO S3: This requires downloading data to the local system.
        # Can we perform all validation on S3 with an EC2 instance?
        if str(dataset_path).startswith("s3://"):
            raise DSGInvalidDataset(
                f"Loading a dataset from S3 is not currently supported: {dataset_path}"
            )

        conn = context.connection
        self._check_if_already_registered(conn, config.model.dataset_id)

        if config.model.dimensions:
            dim_model = DimensionsConfigModel(dimensions=config.model.dimensions)
            dims_config = DimensionsConfig.load_from_model(dim_model)
            dimension_ids = self._dimension_mgr.register_from_config(dims_config, context=context)
            config.model.dimension_references += self._dimension_mgr.make_dimension_references(
                conn, dimension_ids
            )
            config.model.dimensions.clear()

        self._update_dimensions(conn, config)
        self._register(config, dataset_path, context)
        context.add_id(RegistryType.DATASET, config.model.dataset_id, self)

    def _register(
        self,
        config: DatasetConfig,
        dataset_path: Path,
        context: RegistrationContext,
    ):
        # Explanation for this order of operations:
        # 1. Check time consistency in the original dataset format.
        #    Many datasets are stored in pivoted format and have many value columns. If we
        #    check timestamps after unpivoting the dataset, we will multiply the required work
        #    by the number of columns.
        # 2. Write to the registry in unpivoted format before running the other checks.
        #    The final data is always stored in unpivoted format. We can reduce code if we
        #    transform pivoted tables first.
        #    In the nominal case where the dataset is valid, there is no difference in performance.
        #    In the failure case where the dataset is invalid, it will take longer to detect the
        #    errors.
        self._check_time_consistency(config, context)

        config.model.version = "1.0.0"
        dataset_registry_dir = self.get_registry_data_directory(config.model.dataset_id)
        if not dataset_registry_dir.parent.exists():
            msg = (
                f"The registry data path: {dataset_registry_dir.parent} does not exist "
                "(at least from the current computer). Please contact the dsgrid team if this is "
                "unexpected."
            )
            raise Exception(msg)

        dataset_path = dataset_registry_dir / config.model.version
        self.fs_interface.mkdir(dataset_path)

        config = self._write_to_registry(context.connection, config, dataset_path)

        try:
            self._run_checks(context.connection, config)
        except Exception:
            self.fs_interface.rm_tree(dataset_path)
            raise

        self._db.insert(context.connection, config.model, context.registration)
        logger.info(
            "%s Registered dataset %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.dataset_id,
            config.model.version,
        )

    def _check_time_consistency(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ) -> None:
        schema_handler = make_dataset_schema_handler(
            context.connection, config, self._dimension_mgr, self._dimension_mapping_mgr
        )
        schema_handler.check_time_consistency()

    def _write_to_registry(
        self,
        conn: Connection,
        orig_config: DatasetConfig,
        dataset_path: Path,
    ) -> DatasetConfig:
        config = self._copy_dataset_config(conn, orig_config)
        if config.get_table_format_type() == TableFormatType.PIVOTED:
            logger.info("Convert dataset %s from pivoted to unpivoted.", config.model.dataset_id)
            needs_unpivot = True
            pivoted_columns = config.get_pivoted_dimension_columns()
            pivoted_dimension_type = config.get_pivoted_dimension_type()
            config.model.data_schema.table_format = UnpivotedTableFormatModel()
        else:
            needs_unpivot = False
            pivoted_columns = None
            pivoted_dimension_type = None

        found_files = False
        for filename in ALLOWED_DATA_FILES:
            assert config.dataset_path is not None
            path = Path(config.dataset_path) / filename
            if path.exists():
                name = os.path.splitext(filename)[0]
                # Always write Parquet.
                dst = dataset_path / (name + ".parquet")
                # Writing with Spark is much faster than copying or rsync if there are
                # multiple nodes in the cluster - much more parallelism.
                df = read_dataframe(path)
                if filename in ALLOWED_LOAD_DATA_FILENAMES:
                    time_dim = config.get_dimension(DimensionType.TIME)
                    df = time_dim.convert_time_format(df, update_model=True)
                if needs_unpivot and filename in ALLOWED_LOAD_DATA_FILENAMES:
                    assert pivoted_columns is not None
                    assert pivoted_dimension_type is not None
                    time_columns = config.get_dimension(
                        DimensionType.TIME
                    ).get_load_data_time_columns()
                    existing_columns = set(df.columns)
                    if diff := set(time_columns) - existing_columns:
                        msg = f"Expected time columns are not present in the table: {diff=}"
                        raise DSGInvalidDataset(msg)
                    if diff := set(pivoted_columns) - existing_columns:
                        msg = f"Expected pivoted_columns are not present in the table: {diff=}"
                        raise DSGInvalidDataset(msg)
                    df = unpivot_dataframe(
                        df, pivoted_columns, pivoted_dimension_type.value, time_columns
                    )
                write_dataframe_and_auto_partition(df, dst)
                found_files = True
        if not found_files:
            msg = f"Did not find any data files in {config.dataset_path}"
            raise DSGInvalidDataset(msg)

        config.dataset_path = str(dataset_path)
        return config

    def _copy_dataset_config(self, conn: Connection, config: DatasetConfig) -> DatasetConfig:
        new_config = DatasetConfig(config.model)
        new_config.dataset_path = config.dataset_path
        self._update_dimensions(conn, new_config)
        return new_config

    def update_from_file(
        self,
        config_file: Path,
        dataset_id: str,
        submitter: str,
        update_type: VersionUpdateType,
        log_message: str,
        version: str,
        dataset_path: Optional[Path] = None,
    ):
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            conn = context.connection
            path = (
                self._params.base_path / "data" / dataset_id / version
                if dataset_path is None
                else dataset_path
            )
            config = DatasetConfig.load_from_user_path(config_file, path)
            self._update_dimensions(conn, config)
            self._check_update(conn, config, dataset_id, version)
            self.update_with_context(config, context)

    @track_timing(timer_stats_collector)
    def update(
        self,
        config: DatasetConfig,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: Optional[str] = None,
    ) -> DatasetConfig:
        lock_file_path = self.get_registry_lock_file(config.model.dataset_id)
        with self.cloud_interface.make_lock_file_managed(lock_file_path):
            # Note that projects will not pick up these changes until submit-dataset
            # is called again.
            with RegistrationContext(self.db, log_message, update_type, submitter) as context:
                return self.update_with_context(config, context)

    def update_with_context(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ) -> DatasetConfig:
        conn = context.connection
        dataset_id = config.model.dataset_id
        cur_model = self.get_by_id(dataset_id, conn=conn).model
        updated_model = self._update_config(config, context)
        updated_config = DatasetConfig(updated_model)
        updated_config.dataset_path = config.dataset_path
        self._update_dimensions(conn, updated_config)

        dataset_registry_dir = self.get_registry_data_directory(dataset_id)
        new_dataset_path = dataset_registry_dir / updated_config.model.version

        self.fs_interface.mkdir(new_dataset_path)
        updated_config = self._write_to_registry(conn, updated_config, new_dataset_path)

        self._run_checks(conn, updated_config)
        old_key = ConfigKey(dataset_id, cur_model.version)
        new_key = ConfigKey(dataset_id, updated_config.model.version)
        self._datasets.pop(old_key, None)
        self._datasets[new_key] = updated_config

        if not self.offline_mode:
            self.sync_push(self.get_registry_data_directory(dataset_id))

        return updated_config

    def remove(self, dataset_id: str, conn: Optional[Connection] = None):
        self.remove_data(dataset_id, conn=conn)
        self.db.delete_all(conn, dataset_id)
        for key in [x for x in self._datasets if x.id == dataset_id]:
            self._datasets.pop(key)

        logger.info("Removed %s from the registry.", dataset_id)

    def remove_data(self, dataset_id: str, conn: Optional[Connection] = None):
        config = self.get_by_id(dataset_id, conn=conn)
        if self.fs_interface.exists(config.dataset_path):
            self.fs_interface.rm_tree(Path(config.dataset_path).parent)

        logger.info("Removed data for %s from the registry.", dataset_id)

    def show(
        self,
        conn: Optional[Connection] = None,
        filters: list[str] | None = None,
        max_width: Union[int, dict] | None = None,
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
            logger.info("List registry for: %s", filters)

        table = PrettyTable(title=self.name())
        all_field_names = (
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
                "ID": 50,
                "Date": 10,
                "Description": 50,
            }
        if isinstance(max_width, int):
            table.max_width = max_width
        elif isinstance(max_width, dict):
            table._max_width = max_width

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for model in self.db.iter_models(conn, all_versions=True):
            registration = self.db.get_registration(conn, model)

            all_fields = (
                model.dataset_id,
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
