"""Manages the registry for dimension datasets"""

import logging
import os
from pathlib import Path
from typing import Any, Self, Type, Union

import pandas as pd
from prettytable import PrettyTable
from sqlalchemy import Connection

from dsgrid.common import SCALING_FACTOR_COLUMN, SYNC_EXCLUDE_LIST
from dsgrid.config.dataset_config import (
    DatasetConfig,
    DatasetConfigModel,
    user_schema_to_registry_schema,
)
from dsgrid.config.dataset_config import DataSchemaType
from dsgrid.config.file_schemas import read_data_file
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dimensions_config import DimensionsConfig, DimensionsConfigModel
from dsgrid.dataset.models import TableFormatType, UnpivotedTableFormatModel
from dsgrid.dimension.base_models import (
    check_required_dataset_dimensions,
)
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import (
    DimensionMappingRegistryManager,
)
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.registry_interface import DatasetRegistryInterface
from dsgrid.spark.functions import (
    get_spark_session,
    is_dataframe_empty,
)
from dsgrid.spark.types import DataFrame, F, StringType
from dsgrid.utils.dataset import split_expected_missing_rows, unpivot_dataframe
from dsgrid.utils.spark import (
    read_dataframe,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import check_uniqueness, display_table, make_unique_key
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
        store: DataStoreInterface,
    ):
        super().__init__(path, fs_interface)
        self._datasets: dict[ConfigKey, DatasetConfig] = {}
        self._dimension_mgr = dimension_manager
        self._dimension_mapping_mgr = dimension_mapping_manager
        self._db = db
        self._store = store

    @classmethod
    def load(
        cls,
        path: Path,
        params,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: DatasetRegistryInterface,
        store: DataStoreInterface,
    ) -> Self:
        return cls._load(path, params, dimension_manager, dimension_mapping_manager, db, store)

    @staticmethod
    def config_class() -> Type:
        return DatasetConfig

    @property
    def db(self) -> DatasetRegistryInterface:
        return self._db

    @property
    def store(self) -> DataStoreInterface:
        return self._store

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
    def _run_checks(
        self,
        conn: Connection,
        config: DatasetConfig,
        missing_dimension_associations: dict[str, DataFrame],
    ) -> None:
        logger.info("Run dataset registration checks.")
        check_required_dataset_dimensions(config.model.dimension_references, "dataset dimensions")
        check_uniqueness((x.model.name for x in config.model.dimensions), "dimension name")
        if not os.environ.get("__DSGRID_SKIP_CHECK_DATASET_CONSISTENCY__"):
            self._check_dataset_consistency(
                conn,
                config,
                missing_dimension_associations,
            )

    def _check_dataset_consistency(
        self,
        conn: Connection,
        config: DatasetConfig,
        missing_dimension_associations: dict[str, DataFrame],
    ) -> None:
        schema_handler = make_dataset_schema_handler(
            conn,
            config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            store=self._store,
        )
        schema_handler.check_consistency(missing_dimension_associations)

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
                self.remove_data(dataset_id, "1.0.0")
            for key in [x for x in self._datasets if x.id in config_ids]:
                self._datasets.pop(key)

        if not self.offline_mode:
            for dataset_id in config_ids:
                lock_file = self.get_registry_lock_file(dataset_id)
                self.cloud_interface.check_lock_file(lock_file)
                if not error_occurred:
                    self.sync_push(self.get_registry_data_directory(dataset_id))
                self.cloud_interface.remove_lock_file(lock_file)

    def get_by_id(
        self, config_id: str, version: str | None = None, conn: Connection | None = None
    ) -> DatasetConfig:
        if version is None:
            version = self._db.get_latest_version(conn, config_id)

        key = ConfigKey(config_id, version)
        dataset = self._datasets.get(key)
        if dataset is not None:
            return dataset

        if version is None:
            model = self.db.get_latest(conn, config_id)
        else:
            model = self.db.get_by_version(conn, config_id, version)

        config = DatasetConfig(model)
        if config.model.table_schema is not None:
            msg = f"Dataset {config_id} loaded from registry has table_schema set; expected None"
            raise DSGInvalidDataset(msg)
        if config.model.registry_schema is None:
            msg = f"Dataset {config_id} loaded from registry has registry_schema=None; expected a value"
            raise DSGInvalidDataset(msg)
        self._update_dimensions(conn, config)
        self._datasets[key] = config
        return config

    def acquire_registry_locks(self, config_ids: list[str]):
        """Acquire lock(s) on the registry for all config_ids.

        Parameters
        ----------
        config_ids : list[str]

        Raises
        ------
        DSGRegistryLockError
            Raised if a lock cannot be acquired.

        """
        for dataset_id in config_ids:
            lock_file = self.get_registry_lock_file(dataset_id)
            self.cloud_interface.make_lock_file(lock_file)

    def get_registry_lock_file(self, config_id: str):
        """Return registry lock file path.

        Parameters
        ----------
        config_id : str
            Config ID

        Returns
        -------
        str
            Lock file path
        """
        return f"configs/.locks/{config_id}.lock"

    def _update_dimensions(self, conn: Connection | None, config: DatasetConfig):
        dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimension_references, conn=conn
        )
        config.update_dimensions(dimensions)

    def register(
        self,
        config_file: Path,
        submitter: str | None = None,
        log_message: str | None = None,
        context: RegistrationContext | None = None,
        data_base_dir: Path | None = None,
        missing_associations_base_dir: Path | None = None,
    ):
        config = DatasetConfig.load_from_user_path(
            config_file,
            data_base_dir=data_base_dir,
            missing_associations_base_dir=missing_associations_base_dir,
        )
        if context is None:
            assert submitter is not None
            assert log_message is not None
            with RegistrationContext(
                self.db, log_message, VersionUpdateType.MAJOR, submitter
            ) as context:
                return self.register_from_config(
                    config,
                    context,
                )
        else:
            return self.register_from_config(
                config,
                context,
            )

    @track_timing(timer_stats_collector)
    def register_from_config(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ):
        self._update_dimensions(context.connection, config)
        self._register_dataset_and_dimensions(
            config,
            context,
        )

    def _register_dataset_and_dimensions(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ):
        logger.info("Start registration of dataset %s", config.model.dataset_id)

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
        self._register(
            config,
            context,
        )
        context.add_id(RegistryType.DATASET, config.model.dataset_id, self)

    def _register(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ):
        config.model.version = "1.0.0"
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
        self._write_to_registry(config)

        assoc_dfs = self._store.read_missing_associations_tables(
            config.model.dataset_id, config.model.version
        )
        try:
            self._run_checks(
                context.connection,
                config,
                assoc_dfs,
            )
        except Exception:
            self._store.remove_tables(config.model.dataset_id, config.model.version)
            raise

        if config.model.table_schema is not None:
            registry_schema = user_schema_to_registry_schema(config.model.table_schema)
            config.model.table_schema = None
            config.model.registry_schema = registry_schema
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
            context.connection,
            config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            store=None,
        )
        schema_handler.check_time_consistency()

    def _read_lookup_table_from_user_path(
        self, config: DatasetConfig
    ) -> tuple[DataFrame, DataFrame | None]:
        if config.lookup_file_schema is None:
            msg = "Cannot read lookup table without lookup file schema"
            raise DSGInvalidDataset(msg)

        df = read_data_file(config.lookup_file_schema)
        if "id" not in df.columns:
            msg = "load_data_lookup does not include an 'id' column"
            raise DSGInvalidDataset(msg)
        missing = df.filter("id IS NULL").drop("id")
        if is_dataframe_empty(missing):
            missing_df = None
        else:
            missing_df = missing
            if SCALING_FACTOR_COLUMN in missing_df.columns:
                missing_df = missing_df.drop(SCALING_FACTOR_COLUMN)
        return df, missing_df

    def _read_missing_associations_tables_from_user_path(
        self, config: DatasetConfig
    ) -> dict[str, DataFrame]:
        """Return all missing association tables keyed by the file path stem.
        Tables can be all-dimension-types-in-one or split by groups of dimension types.
        """
        dfs: dict[str, DataFrame] = {}
        missing_paths = config.missing_associations_paths
        if not missing_paths:
            return dfs

        def add_df(path):
            df = self._read_associations_file(path)
            key = make_unique_key(path.stem, dfs)
            dfs[key] = df

        for path in missing_paths:
            if path.suffix.lower() == ".parquet":
                add_df(path)
            elif path.is_dir():
                for file_path in path.iterdir():
                    if file_path.suffix.lower() in (".csv", ".parquet"):
                        add_df(file_path)
            elif path.suffix.lower() in (".csv", ".parquet"):
                add_df(path)
        return dfs

    @staticmethod
    def _read_associations_file(path: Path) -> DataFrame:
        if path.suffix.lower() == ".csv":
            df = get_spark_session().createDataFrame(pd.read_csv(path, dtype="string"))
        else:
            df = read_dataframe(path)
            for field in df.schema.fields:
                if field.dataType != StringType():
                    df = df.withColumn(field.name, F.col(field.name).cast(StringType()))
        return df

    def _read_table_from_user_path(
        self, config: DatasetConfig
    ) -> tuple[DataFrame, DataFrame | None]:
        """Read a table from a user-provided path. Split expected-missing rows into a separate
        DataFrame.

        Returns
        -------
        tuple[DataFrame, DataFrame | None]
            The first DataFrame contains the expected rows, and the second DataFrame contains the
            missing rows or will be None if there are no missing rows.
        """
        if config.data_file_schema is None:
            msg = "Cannot read table without data file schema"
            raise DSGInvalidDataset(msg)

        if config.get_table_format_type() == TableFormatType.PIVOTED:
            logger.info("Convert dataset %s from pivoted to unpivoted.", config.model.dataset_id)
            needs_unpivot = True
            pivoted_columns = config.get_pivoted_dimension_columns()
            pivoted_dimension_type = config.get_pivoted_dimension_type()
            config.model.table_schema.data_schema.table_format = UnpivotedTableFormatModel()
        else:
            needs_unpivot = False
            pivoted_columns = None
            pivoted_dimension_type = None

        df = read_data_file(config.data_file_schema)

        time_columns: list[str] = []
        time_dim = config.get_time_dimension()
        if time_dim is not None:
            time_columns.extend(time_dim.get_load_data_time_columns())
            df = time_dim.convert_time_format(df, update_model=True)

        if needs_unpivot:
            assert pivoted_columns is not None
            assert pivoted_dimension_type is not None
            existing_columns = set(df.columns)
            if diff := set(time_columns) - existing_columns:
                msg = f"Expected time columns are not present in the table: {diff=}"
                raise DSGInvalidDataset(msg)
            if diff := set(pivoted_columns) - existing_columns:
                msg = f"Expected pivoted_columns are not present in the table: {diff=}"
                raise DSGInvalidDataset(msg)
            df = unpivot_dataframe(df, pivoted_columns, pivoted_dimension_type.value, time_columns)

        return split_expected_missing_rows(df, time_columns)

    def _write_to_registry(
        self,
        config: DatasetConfig,
        orig_version: str | None = None,
    ) -> None:
        lk_df: DataFrame | None = None
        missing_dfs: dict[str, DataFrame] = {}
        match config.get_data_schema_type():
            case DataSchemaType.ONE_TABLE:
                if not config.has_user_schema:
                    assert (
                        orig_version is not None
                    ), "orig_version must be set if config came from the registry"
                    missing_dfs.update(
                        self._store.read_missing_associations_tables(
                            config.model.dataset_id, orig_version
                        )
                    )
                    ld_df = self._store.read_table(config.model.dataset_id, orig_version)
                else:
                    # Note: config will be updated if this is a pivoted table.
                    ld_df, missing_df1 = self._read_table_from_user_path(config)
                    missing_dfs2 = self._read_missing_associations_tables_from_user_path(config)
                    missing_dfs.update(
                        self._check_duplicate_missing_associations(missing_df1, missing_dfs2)
                    )

            case DataSchemaType.STANDARD:
                if not config.has_user_schema:
                    assert (
                        orig_version is not None
                    ), "orig_version must be set if config came from the registry"
                    lk_df = self._store.read_lookup_table(config.model.dataset_id, orig_version)
                    ld_df = self._store.read_table(config.model.dataset_id, orig_version)
                    missing_dfs = self._store.read_missing_associations_tables(
                        config.model.dataset_id, orig_version
                    )
                else:
                    # Note: config will be updated if this is a pivoted table.
                    ld_df, tmp = self._read_table_from_user_path(config)
                    if tmp is not None:
                        msg = (
                            "NULL rows cannot be present in the load_data table in standard format. "
                            "They must be provided in the load_data_lookup table."
                        )
                        raise DSGInvalidDataset(msg)
                    lk_df, missing_df1 = self._read_lookup_table_from_user_path(config)
                    missing_dfs2 = self._read_missing_associations_tables_from_user_path(config)
                    missing_dfs.update(
                        self._check_duplicate_missing_associations(missing_df1, missing_dfs2)
                    )
            case _:
                msg = f"Unsupported data schema type: {config.get_data_schema_type()}"
                raise Exception(msg)

        self._store.write_table(ld_df, config.model.dataset_id, config.model.version)
        if lk_df is not None:
            self._store.write_lookup_table(lk_df, config.model.dataset_id, config.model.version)
        if missing_dfs:
            self._store.write_missing_associations_tables(
                missing_dfs, config.model.dataset_id, config.model.version
            )

    @staticmethod
    def _check_duplicate_missing_associations(
        df1: DataFrame | None, dfs2: dict[str, DataFrame]
    ) -> dict[str, DataFrame]:
        if df1 is not None and dfs2:
            msg = "A dataset cannot have expected missing rows in the data and "
            "provide a missing_associations file. Provide one or the other."
            raise DSGInvalidDataset(msg)

        if df1 is not None:
            return {"missing_associations": df1}
        elif dfs2:
            return dfs2
        return {}

    def _copy_dataset_config(self, conn: Connection, config: DatasetConfig) -> DatasetConfig:
        new_config = DatasetConfig(config.model)
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
    ):
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            conn = context.connection
            # If config has UserDatasetSchema, load with validation; otherwise just load
            config = DatasetConfig.load(config_file)
            if config.has_user_schema:
                config = DatasetConfig.load_from_user_path(config_file)
            self._update_dimensions(conn, config)
            self._check_update(conn, config, dataset_id, version)
            self.update_with_context(
                config,
                context,
            )

    @track_timing(timer_stats_collector)
    def update(
        self,
        config: DatasetConfig,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: str | None = None,
    ) -> DatasetConfig:
        lock_file_path = self.get_registry_lock_file(config.model.dataset_id)
        with self.cloud_interface.make_lock_file_managed(lock_file_path):
            # Note that projects will not pick up these changes until submit-dataset
            # is called again.
            with RegistrationContext(self.db, log_message, update_type, submitter) as context:
                return self.update_with_context(
                    config,
                    context,
                )

    def update_with_context(
        self,
        config: DatasetConfig,
        context: RegistrationContext,
    ) -> DatasetConfig:
        conn = context.connection
        dataset_id = config.model.dataset_id
        cur_config = self.get_by_id(dataset_id, conn=conn)
        updated_model = self._update_config(config, context)
        updated_config = DatasetConfig(updated_model)
        self._update_dimensions(conn, updated_config)

        # Note: this method mutates updated_config.
        self._write_to_registry(updated_config, orig_version=cur_config.model.version)

        assoc_df = self._store.read_missing_associations_tables(
            updated_config.model.dataset_id, updated_config.model.version
        )
        try:
            self._run_checks(conn, updated_config, assoc_df)
        except Exception:
            self._store.remove_tables(
                updated_config.model.dataset_id, updated_config.model.version
            )
            raise

        old_key = ConfigKey(dataset_id, cur_config.model.version)
        new_key = ConfigKey(dataset_id, updated_config.model.version)
        self._datasets.pop(old_key, None)
        self._datasets[new_key] = updated_config

        if not self.offline_mode:
            self.sync_push(self.get_registry_data_directory(dataset_id))

        return updated_config

    def remove(self, config_id: str, conn: Connection | None = None):
        for key in [x for x in self._datasets if x.id == config_id]:
            self.remove_data(config_id, key.version)
            self._datasets.pop(key)

        self.db.delete_all(conn, config_id)
        logger.info("Removed %s from the registry.", config_id)

    def remove_data(self, dataset_id: str, version: str):
        self._store.remove_tables(dataset_id, version)
        logger.info("Removed data for %s version=%s from the registry.", dataset_id, version)

    def show(
        self,
        conn: Connection | None = None,
        filters: list[str] | None = None,
        max_width: Union[int, dict] | None = None,
        drop_fields: list[str] | None = None,
        return_table: bool = False,
        **kwargs: Any,
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
            assert isinstance(model, DatasetConfigModel)
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

    def sync_pull(self, path):
        """Synchronizes files from the remote registry to local.
        Deletes any files locally that do not exist on remote.

        path : Path
            Local path

        """
        remote_path = self.relative_remote_path(path)
        self.cloud_interface.sync_pull(
            remote_path, path, exclude=SYNC_EXCLUDE_LIST, delete_local=True
        )

    def sync_push(self, path):
        """Synchronizes files from the local path to the remote registry.

        path : Path
            Local path

        """
        remote_path = self.relative_remote_path(path)
        lock_file_path = self.get_registry_lock_file(path.name)
        self.cloud_interface.check_lock_file(lock_file_path)
        try:
            self.cloud_interface.sync_push(
                remote_path=remote_path, local_path=path, exclude=SYNC_EXCLUDE_LIST
            )
        except Exception:
            logger.exception(
                "Please report this error to the dsgrid team. The registry may need recovery."
            )
            raise
