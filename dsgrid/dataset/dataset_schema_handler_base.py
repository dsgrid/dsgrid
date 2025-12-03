import abc
import logging
import os
from pathlib import Path
from typing import Iterable, Self

import chronify
from sqlalchemy import Connection

import dsgrid
from dsgrid.chronify import create_store, create_in_memory_store
from dsgrid.config.annual_time_dimension_config import (
    AnnualTimeDimensionConfig,
    map_annual_time_to_date_time,
)
from dsgrid.config.dimension_config import (
    DimensionBaseConfig,
    DimensionBaseConfigWithFiles,
)
from dsgrid.config.noop_time_dimension_config import NoOpTimeDimensionConfig
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.index_time_dimension_config import IndexTimeDimensionConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dimension.time import TimeBasedDataAdjustmentModel
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.common import VALUE_COLUMN, BackendEngine
from dsgrid.config.dataset_config import (
    DatasetConfig,
    InputDatasetType,
)
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingReferenceModel,
)
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dataset.table_format_handler_factory import make_table_format_handler
from dsgrid.config.file_schemas import read_data_file
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidDimensionMapping
from dsgrid.dimension.time import (
    DaylightSavingAdjustmentModel,
)
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan, MapOperation
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import ColumnType
from dsgrid.spark.functions import (
    cache,
    except_all,
    is_dataframe_empty,
    join,
    make_temp_view_name,
    unpersist,
)
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.spark.types import DataFrame, F, use_duckdb
from dsgrid.units.convert import convert_units_unpivoted
from dsgrid.utils.dataset import (
    check_historical_annual_time_model_year_consistency,
    filter_out_expected_missing_associations,
    handle_dimension_association_errors,
    is_noop_mapping,
    map_stacked_dimension,
    add_time_zone,
    map_time_dimension_with_chronify_duckdb,
    map_time_dimension_with_chronify_spark_hive,
    map_time_dimension_with_chronify_spark_path,
    ordered_subset_columns,
    repartition_if_needed_by_mapping,
)

from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    check_for_nulls,
    create_dataframe_from_product,
    get_unique_values,
    persist_table,
    read_dataframe,
    save_to_warehouse,
    write_dataframe,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import (
    DimensionMappingRegistryManager,
)

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """define interface/required behaviors per dataset schema"""

    def __init__(
        self,
        config: DatasetConfig,
        conn: Connection | None,
        dimension_mgr: DimensionRegistryManager,
        dimension_mapping_mgr: DimensionMappingRegistryManager,
        mapping_references: list[DimensionMappingReferenceModel] | None = None,
    ):
        self._conn = conn
        self._config = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references: list[DimensionMappingReferenceModel] = mapping_references or []

    @classmethod
    @abc.abstractmethod
    def load(cls, config: DatasetConfig, *args, store: DataStoreInterface | None = None) -> Self:
        """Create a dataset schema handler by loading the data tables from files.

        Parameters
        ----------
        config: DatasetConfig
        store: DataStoreInterface | None
            If provided, the dataset must already be registered.
            If not provided, the dataset must not be registered and the file path must be
            available via the DatasetConfig.

        Returns
        -------
        DatasetSchemaHandlerBase
        """

    @abc.abstractmethod
    def check_consistency(self, missing_dimension_associations: dict[str, DataFrame]) -> None:
        """
        Check all data consistencies, including data columns, dataset to dimension records, and time
        """

    @abc.abstractmethod
    def check_time_consistency(self):
        """Check the time consistency of the dataset."""

    @abc.abstractmethod
    def _get_load_data_table(self) -> DataFrame:
        """Return the full load data table."""

    def _make_actual_dimension_association_table_from_data(self) -> DataFrame:
        return self._remove_non_dimension_columns(self._get_load_data_table()).distinct()

    def _make_expected_dimension_association_table_from_records(
        self, dimension_types: Iterable[DimensionType], context: ScratchDirContext
    ) -> DataFrame:
        """Return a dataframe containing one row for each unique dimension combination except time.
        Use dimensions in the dataset's dimension records.
        """
        data: dict[str, list[str]] = {}
        for dim_type in dimension_types:
            dim = self._config.get_dimension_with_records(dim_type)
            if dim is not None:
                data[dim_type.value] = list(dim.get_unique_ids())

        if not data:
            msg = "Bug: did not find any dimension records"
            raise Exception(msg)
        return create_dataframe_from_product(data, context)

    @track_timing(timer_stats_collector)
    def _check_dimension_associations(
        self, missing_dimension_associations: dict[str, DataFrame]
    ) -> None:
        """Check that a cross-join of dimension records is present, unless explicitly excepted."""
        logger.info("Check dimension associations")
        dsgrid_config = DsgridRuntimeConfig.load()
        scratch_dir = dsgrid_config.get_scratch_dir()
        with ScratchDirContext(scratch_dir) as context:
            assoc_by_records = self._make_expected_dimension_association_table_from_records(
                [x for x in DimensionType if x != DimensionType.TIME], context
            )
            assoc_by_data = self._make_actual_dimension_association_table_from_data()
            # This first check is redundant with the checks below. But, it is significantly
            # easier for users to debug.
            for column in assoc_by_records.columns:
                expected = get_unique_values(assoc_by_records, column)
                actual = get_unique_values(assoc_by_data, column)
                if actual != expected:
                    missing = sorted(expected.difference(actual))
                    extra = sorted(actual.difference(expected))
                    num_matching = len(actual.intersection(expected))
                    msg = (
                        f"Dataset records for dimension type {column} do not match expected "
                        f"values. {missing=} {extra=} {num_matching=}"
                    )
                    raise DSGInvalidDataset(msg)

            required_assoc = assoc_by_records
            if missing_dimension_associations:
                for missing_df in missing_dimension_associations.values():
                    required_assoc = filter_out_expected_missing_associations(
                        required_assoc, missing_df
                    )

            cols = sorted(required_assoc.columns)
            diff = except_all(required_assoc.select(*cols), assoc_by_data.select(*cols))
            cache(diff)
            try:
                if not is_dataframe_empty(diff):
                    handle_dimension_association_errors(diff, assoc_by_data, self.dataset_id)
                logger.info("Successfully checked dataset dimension associations")
            finally:
                unpersist(diff)

    def make_mapped_dimension_association_table(self, context: ScratchDirContext) -> DataFrame:
        """Return a dataframe containing one row for each unique dimension combination except time.
        Use mapped dimensions.
        """
        assoc_df = self._make_actual_dimension_association_table_from_data()
        mapping_plan = self.build_default_dataset_mapping_plan()
        with DatasetMappingManager(self.dataset_id, mapping_plan, context) as mapping_manager:
            df = (
                self._remap_dimension_columns(assoc_df, mapping_manager)
                .drop("fraction")
                .distinct()
            )
        check_for_nulls(df)
        return df

    def remove_expected_missing_mapped_associations(
        self, store: DataStoreInterface, df: DataFrame, context: ScratchDirContext
    ) -> DataFrame:
        """Remove expected missing associations from the full join of expected associations."""
        missing_associations = store.read_missing_associations_tables(
            self._config.model.dataset_id, self._config.model.version
        )
        if not missing_associations:
            return df

        final_df = df
        mapping_plan = self.build_default_dataset_mapping_plan()
        with DatasetMappingManager(self.dataset_id, mapping_plan, context) as mapping_manager:
            for missing_df in missing_associations.values():
                mapped_df = (
                    self._remap_dimension_columns(missing_df, mapping_manager)
                    .drop("fraction")
                    .distinct()
                )
                final_df = filter_out_expected_missing_associations(final_df, mapped_df)
        return final_df

    @abc.abstractmethod
    def filter_data(self, dimensions: list[DimensionSimpleModel], store: DataStoreInterface):
        """Filter the load data by dimensions and rewrite the files.

        dimensions : list[DimensionSimpleModel]
        store : DataStoreInterface
            The data store to use for reading and writing the data.
        """

    @property
    def connection(self) -> Connection | None:
        """Return the active sqlalchemy connection to the registry database."""
        return self._conn

    @property
    def dataset_id(self):
        return self._config.config_id

    @property
    def config(self):
        """Returns the DatasetConfig.

        Returns
        -------
        DatasetConfig

        """
        return self._config

    @abc.abstractmethod
    def make_project_dataframe(self, context, project_config) -> DataFrame:
        """Return a load_data dataframe with dimensions mapped to the project's with filters
        as specified by the QueryContext.

        Parameters
        ----------
        context : QueryContext
        project_config : ProjectConfig

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @abc.abstractmethod
    def make_mapped_dataframe(
        self,
        context: QueryContext,
        time_dimension: TimeDimensionBaseConfig | None = None,
    ) -> DataFrame:
        """Return a load_data dataframe with dimensions mapped as stored in the handler.

        Parameters
        ----------
        context
        time_dimension
            Required if the time dimension is being mapped.
            This should be the destination time dimension.

        """

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency(self, load_data_df: DataFrame):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        Callers must ensure that the dataset has a time dimension.
        """
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__"):
            logger.warning("Skip dataset time consistency checks.")
            return

        logger.info("Check dataset time consistency.")
        time_dim = self._config.get_time_dimension()
        assert time_dim is not None, "time cannot be checked if the dataset has no time dimension"
        time_cols = self._get_time_dimension_columns()
        time_dim.check_dataset_time_consistency(load_data_df, time_cols)
        if not isinstance(time_dim, NoOpTimeDimensionConfig):
            self._check_dataset_time_consistency_by_time_array(time_cols, load_data_df)
        self._check_model_year_time_consistency(load_data_df)

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency_with_chronify(self):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        Callers must ensure that the dataset has a time dimension.
        """
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__"):
            logger.warning("Skip dataset time consistency checks.")
            return

        logger.info("Check dataset time consistency.")
        file_schema = self._config.model.table_schema.data_file
        load_data_df = read_data_file(file_schema)
        chronify_schema = self._get_chronify_schema(load_data_df)

        data_file_path = Path(file_schema.path)
        if data_file_path.suffix == ".parquet" or not use_duckdb():
            scratch_dir = DsgridRuntimeConfig.load().get_scratch_dir()
            with ScratchDirContext(scratch_dir) as context:
                if data_file_path.suffix == ".csv":
                    # This is a workaround for time zone issues between Spark, Pandas,
                    # and Chronify when reading CSV files.
                    # Chronify can ingest them correctly when we go to Parquet first.
                    # This is really only a test issue because normal dsgrid users will not
                    # use Spark with CSV data files.
                    src_path = context.get_temp_filename(suffix=".parquet")
                    write_dataframe(load_data_df, src_path)
                else:
                    src_path = data_file_path
                store_file = context.get_temp_filename(suffix=".db")
                with create_store(store_file) as store:
                    # This performs all of the checks.
                    store.create_view_from_parquet(src_path, chronify_schema)
                    store.drop_view(chronify_schema.name)
        else:
            # For CSV and JSON files, use in-memory store with ingest_table.
            # This avoids the complexity of converting to parquet.
            with create_in_memory_store() as store:
                # ingest_table performs all of the time checks.
                store.ingest_table(load_data_df.toPandas(), chronify_schema)
                store.drop_table(chronify_schema.name)

        self._check_model_year_time_consistency(load_data_df)

    def _get_chronify_schema(self, df: DataFrame):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_cols = time_dim.get_load_data_time_columns()
        time_array_id_columns = [
            x
            for x in df.columns
            # If there are multiple weather years:
            #   - that are continuous, weather year needs to be excluded (one overall range).
            #   - that are not continuous, weather year needs to be included and chronify
            #     needs additional support. TODO: issue #340
            if x != DimensionType.WEATHER_YEAR.value
            and x
            in set(df.columns).difference(time_cols).difference(self._config.get_value_columns())
        ]
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            # We can ignore all pivoted columns but one for time checking.
            # Looking at the rest would be redundant.
            value_column = next(iter(self._config.get_pivoted_dimension_columns()))
        else:
            value_column = VALUE_COLUMN
        return chronify.TableSchema(
            name=make_temp_view_name(),
            time_config=time_dim.to_chronify(),
            time_array_id_columns=time_array_id_columns,
            value_column=value_column,
        )

    def _check_model_year_time_consistency(self, df: DataFrame):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if self._config.model.dataset_type == InputDatasetType.HISTORICAL and isinstance(
            time_dim, AnnualTimeDimensionConfig
        ):
            annual_cols = time_dim.get_load_data_time_columns()
            assert len(annual_cols) == 1
            annual_col = annual_cols[0]
            check_historical_annual_time_model_year_consistency(
                df, annual_col, DimensionType.MODEL_YEAR.value
            )

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency_by_time_array(self, time_cols, load_data_df):
        """Check that each unique time array has the same timestamps."""
        logger.info("Check dataset time consistency by time array.")
        unique_array_cols = set(DimensionType.get_allowed_dimension_column_names()).intersection(
            load_data_df.columns
        )
        counts = load_data_df.groupBy(*time_cols).count().select("count")
        distinct_counts = counts.select("count").distinct().collect()
        if len(distinct_counts) != 1:
            msg = (
                "All time arrays must be repeated the same number of times: "
                f"unique timestamp repeats = {len(distinct_counts)}"
            )
            raise DSGInvalidDataset(msg)
        ta_counts = load_data_df.groupBy(*unique_array_cols).count().select("count")
        distinct_ta_counts = ta_counts.select("count").distinct().collect()
        if len(distinct_ta_counts) != 1:
            msg = (
                "All combinations of non-time dimensions must have the same time array length: "
                f"unique time array lengths = {len(distinct_ta_counts)}"
            )
            raise DSGInvalidDataset(msg)

    def _check_load_data_unpivoted_value_column(self, df):
        logger.info("Check load data unpivoted columns.")
        if VALUE_COLUMN not in df.columns:
            msg = f"value_column={VALUE_COLUMN} is not in columns={df.columns}"
            raise DSGInvalidDataset(msg)

    def _convert_units(
        self,
        df: DataFrame,
        project_metric_records: DataFrame,
        mapping_manager: DatasetMappingManager,
    ):
        if not self._config.model.enable_unit_conversion:
            return df

        op = mapping_manager.plan.convert_units_op
        if mapping_manager.has_completed_operation(op):
            return df

        # Note that a dataset could have the same dimension record IDs as the project,
        # no mappings, but then still have different units.
        mapping_records = None
        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            if dim_type == DimensionType.METRIC:
                mapping_records = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version, conn=self.connection
                ).get_records_dataframe()
                break

        dataset_dim = self._config.get_dimension_with_records(DimensionType.METRIC)
        dataset_records = dataset_dim.get_records_dataframe()
        df = convert_units_unpivoted(
            df,
            DimensionType.METRIC.value,
            dataset_records,
            mapping_records,
            project_metric_records,
        )
        if op.persist:
            df = mapping_manager.persist_table(df, op)
        return df

    def _finalize_table(self, context: QueryContext, df: DataFrame, project_config: ProjectConfig):
        # TODO: remove ProjectConfig so that dataset queries can use this.
        # Issue #370
        table_handler = make_table_format_handler(
            self._config.get_table_format_type(),
            project_config,
            dataset_id=self.dataset_id,
        )

        time_dim = project_config.get_base_dimension(DimensionType.TIME)
        context.set_dataset_metadata(
            self.dataset_id,
            context.model.result.column_type,
            project_config.get_load_data_time_columns(time_dim.model.name),
        )

        if context.model.result.column_type == ColumnType.DIMENSION_NAMES:
            df = table_handler.convert_columns_to_query_names(
                df, self._config.model.dataset_id, context
            )

        return df

    @staticmethod
    def _get_pivoted_column_name(
        context: QueryContext, pivoted_dimension_type: DimensionType, project_config
    ):
        match context.model.result.column_type:
            case ColumnType.DIMENSION_NAMES:
                pivoted_column_name = project_config.get_base_dimension(
                    pivoted_dimension_type
                ).model.name
            case ColumnType.DIMENSION_TYPES:
                pivoted_column_name = pivoted_dimension_type.value
            case _:
                msg = str(context.model.result.column_type)
                raise NotImplementedError(msg)

        return pivoted_column_name

    def _get_dataset_to_project_mapping_records(self, dimension_type: DimensionType):
        config = self._get_dataset_to_project_mapping_config(dimension_type)
        if config is None:
            return config
        return config.get_records_dataframe()

    def _get_dataset_to_project_mapping_config(self, dimension_type: DimensionType):
        ref = self._get_dataset_to_project_mapping_reference(dimension_type)
        if ref is None:
            return ref
        return self._dimension_mapping_mgr.get_by_id(
            ref.mapping_id, version=ref.version, conn=self.connection
        )

    def _get_dataset_to_project_mapping_reference(self, dimension_type: DimensionType):
        for ref in self._mapping_references:
            if ref.from_dimension_type == dimension_type:
                return ref
        return

    def _get_mapping_to_dimension(
        self, dimension_type: DimensionType
    ) -> DimensionBaseConfig | None:
        ref = self._get_dataset_to_project_mapping_reference(dimension_type)
        if ref is None:
            return None
        config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id, conn=self._conn)
        return self._dimension_mgr.get_by_id(
            config.model.to_dimension.dimension_id, conn=self._conn
        )

    def _get_project_metric_records(self, project_config: ProjectConfig) -> DataFrame:
        metric_dim_query_name = getattr(
            project_config.get_dataset_base_dimension_names(self._config.model.dataset_id),
            DimensionType.METRIC.value,
        )
        if metric_dim_query_name is None:
            # This is a workaround for dsgrid projects created before the field
            # base_dimension_names was added to InputDatasetModel.
            metric_dims = project_config.list_base_dimensions(dimension_type=DimensionType.METRIC)
            if len(metric_dims) > 1:
                msg = (
                    "The dataset's base_dimension_names value is not set and "
                    "there are multiple metric dimensions in the project. Please re-register the "
                    f"dataset with dataset_id={self._config.model.dataset_id}."
                )
                raise DSGInvalidDataset(msg)
            metric_dim_query_name = metric_dims[0].model.name
        return project_config.get_dimension_records(metric_dim_query_name)

    def _get_time_dimension_columns(self):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_cols = time_dim.get_load_data_time_columns()
        return time_cols

    def _iter_dataset_record_ids(self, context: QueryContext):
        for dim_type, project_record_ids in context.get_record_ids().items():
            dataset_mapping = self._get_dataset_to_project_mapping_records(dim_type)
            if dataset_mapping is None:
                dataset_record_ids = project_record_ids
            else:
                dataset_record_ids = (
                    join(
                        dataset_mapping.withColumnRenamed("from_id", "dataset_record_id"),
                        project_record_ids,
                        "to_id",
                        "id",
                    )
                    .select("dataset_record_id")
                    .withColumnRenamed("dataset_record_id", "id")
                    .distinct()
                )
            yield dim_type, dataset_record_ids

    @staticmethod
    def _list_dimension_columns(df: DataFrame) -> list[str]:
        columns = DimensionType.get_allowed_dimension_column_names()
        return [x for x in df.columns if x in columns]

    def _list_dimension_types_in_load_data(self, df: DataFrame) -> list[DimensionType]:
        dims = [DimensionType(x) for x in DatasetSchemaHandlerBase._list_dimension_columns(df)]
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            pivoted_type = self._config.get_pivoted_dimension_type()
            assert pivoted_type is not None
            dims.append(pivoted_type)
        return dims

    def _prefilter_pivoted_dimensions(self, context: QueryContext, df):
        for dim_type, dataset_record_ids in self._iter_dataset_record_ids(context):
            if dim_type == self._config.get_pivoted_dimension_type():
                # Drop columns that don't match requested project record IDs.
                cols_to_keep = {x.id for x in dataset_record_ids.collect()}
                cols_to_drop = set(self._config.get_pivoted_dimension_columns()).difference(
                    cols_to_keep
                )
                if cols_to_drop:
                    df = df.drop(*cols_to_drop)

        return df

    def _prefilter_stacked_dimensions(self, context: QueryContext, df):
        for dim_type, dataset_record_ids in self._iter_dataset_record_ids(context):
            # Drop rows that don't match requested project record IDs.
            tmp = dataset_record_ids.withColumnRenamed("id", "dataset_record_id")
            if dim_type.value not in df.columns:
                # This dimensions is stored in another table (e.g., lookup or load_data)
                continue
            df = join(df, tmp, dim_type.value, "dataset_record_id").drop("dataset_record_id")

        return df

    def _prefilter_time_dimension(self, context: QueryContext, df):
        # TODO #196:
        return df

    def build_default_dataset_mapping_plan(self) -> DatasetMappingPlan:
        """Build a default mapping order of dimensions to a project."""
        mappings: list[MapOperation] = []
        for ref in self._mapping_references:
            config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id, conn=self.connection)
            dim = self._dimension_mgr.get_by_id(
                config.model.to_dimension.dimension_id, conn=self.connection
            )
            mappings.append(
                MapOperation(
                    name=dim.model.name,
                    mapping_reference=ref,
                )
            )

        return DatasetMappingPlan(dataset_id=self._config.model.dataset_id, mappings=mappings)

    def check_dataset_mapping_plan(
        self, mapping_plan: DatasetMappingPlan, project_config: ProjectConfig
    ) -> None:
        """Check that a user-defined mapping plan is valid."""
        req_dimensions: dict[DimensionType, DimensionMappingReferenceModel] = {}
        actual_mapping_dims: dict[DimensionType, str] = {}

        for ref in self._mapping_references:
            assert ref.to_dimension_type not in req_dimensions
            req_dimensions[ref.to_dimension_type] = ref

        dataset_id = mapping_plan.dataset_id
        indexes_to_remove: list[int] = []
        for i, mapping in enumerate(mapping_plan.mappings):
            to_dim = project_config.get_dimension(mapping.name)
            if to_dim.model.dimension_type == DimensionType.TIME:
                msg = (
                    f"DatasetMappingPlan for {dataset_id=} is invalid because specification "
                    f"of the time dimension is not supported: {mapping.name}"
                )
                raise DSGInvalidDimensionMapping(msg)
            if to_dim.model.dimension_type in actual_mapping_dims:
                msg = (
                    f"DatasetMappingPlan for {dataset_id=} is invalid because it can only "
                    f"support mapping one dimension for a given dimension type. "
                    f"type={to_dim.model.dimension_type} "
                    f"first={actual_mapping_dims[to_dim.model.dimension_type]} "
                    f"second={mapping.name}"
                )
                raise DSGInvalidDimensionMapping(msg)

            from_dim = self._config.get_dimension(to_dim.model.dimension_type)
            supp_dim_names = {
                x.model.name
                for x in project_config.list_supplemental_dimensions(to_dim.model.dimension_type)
            }
            if mapping.name in supp_dim_names:
                # This could be useful if we wanted to use DatasetMappingPlan for mapping
                # a single dataset to a project's dimensions without being concerned about
                # aggregrations. As it stands, we can are only using this within our
                # project query process. We need much more handling to make that work.
                msg = (
                    "DatasetMappingPlan for {dataset_id=} is invalid because it specifies "
                    f"a supplemental dimension: {mapping.name}"
                )
            elif to_dim.model.dimension_type not in req_dimensions:
                msg = (
                    f"DatasetMappingPlan for {dataset_id=} is invalid because there is no "
                    f"dataset-to-project-base mapping defined for {to_dim.model.label}"
                )
                raise DSGInvalidDimensionMapping(msg)

            ref = req_dimensions[to_dim.model.dimension_type]
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version, conn=self.connection
            )
            if (
                from_dim.model.dimension_id == mapping_config.model.from_dimension.dimension_id
                and to_dim.model.dimension_id == mapping_config.model.to_dimension.dimension_id
            ):
                mapping.mapping_reference = ref
                actual_mapping_dims[to_dim.model.dimension_type] = mapping.name

        for index in indexes_to_remove:
            mapping_plan.mappings.pop(index)

        if diff_dims := set(req_dimensions.keys()).difference(actual_mapping_dims.keys()):
            req = sorted((x.value for x in req_dimensions))
            act = sorted((x.value for x in actual_mapping_dims))
            diff = sorted((x.value for x in diff_dims))
            msg = (
                "If a mapping order is specified for a dataset, it must include all "
                "dimension types that require mappings to the project base dimension.\n"
                f"Required dimension types: {req}\nActual dimension types: {act}\n"
                f"Difference: {diff}"
            )
            raise DSGInvalidDimensionMapping(msg)

    def _remap_dimension_columns(
        self,
        df: DataFrame,
        mapping_manager: DatasetMappingManager,
        filtered_records: dict[DimensionType, DataFrame] | None = None,
    ) -> DataFrame:
        """Map the table's dimensions according to the plan.

        Parameters
        ----------
        df
            The dataframe to map.
        mapping_manager
            Manages checkpointing and order of the mapping operations.
        filtered_records
            If not None, use these records to filter the table.
            If None, do not persist any intermediate tables.
            If not None, use this context to persist intermediate tables if required.
        """
        completed_operations = mapping_manager.get_completed_mapping_operations()
        for dim_mapping in mapping_manager.plan.mappings:
            if dim_mapping.name in completed_operations:
                logger.info(
                    "Skip mapping operation %s because the result exists in a checkpointed file.",
                    dim_mapping.name,
                )
                continue
            assert dim_mapping.mapping_reference is not None
            ref = dim_mapping.mapping_reference
            dim_type = ref.from_dimension_type
            column = dim_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version, conn=self.connection
            )
            logger.info(
                "Mapping dimension type %s mapping_type=%s",
                dim_type,
                mapping_config.model.mapping_type,
            )
            records = mapping_config.get_records_dataframe()
            if filtered_records is not None and dim_type in filtered_records:
                records = join(records, filtered_records[dim_type], "to_id", "id").drop("id")

            if is_noop_mapping(records):
                logger.info("Skip no-op mapping %s.", ref.mapping_id)
                continue
            if column in df.columns:
                persisted_file: Path | None = None
                df = map_stacked_dimension(df, records, column)
                df, persisted_file = repartition_if_needed_by_mapping(
                    df,
                    mapping_config.model.mapping_type,
                    mapping_manager.scratch_dir_context,
                    repartition=dim_mapping.handle_data_skew,
                )
                if dim_mapping.persist and persisted_file is None:
                    mapping_manager.persist_table(df, dim_mapping)
                if persisted_file is not None:
                    mapping_manager.save_checkpoint(persisted_file, dim_mapping)

        return df

    def _apply_fraction(
        self,
        df,
        value_columns,
        mapping_manager: DatasetMappingManager,
        agg_func=None,
    ):
        op = mapping_manager.plan.apply_fraction_op
        if "fraction" not in df.columns:
            return df
        if mapping_manager.has_completed_operation(op):
            return df
        agg_func = agg_func or F.sum
        # Maintain column order.
        agg_ops = [
            agg_func(F.col(x) * F.col("fraction")).alias(x)
            for x in [y for y in df.columns if y in value_columns]
        ]
        gcols = set(df.columns) - value_columns - {"fraction"}
        df = df.groupBy(*ordered_subset_columns(df, gcols)).agg(*agg_ops)
        df = df.drop("fraction")
        if op.persist:
            df = mapping_manager.persist_table(df, op)
        return df

    @track_timing(timer_stats_collector)
    def _convert_time_dimension(
        self,
        load_data_df: DataFrame,
        to_time_dim: TimeDimensionBaseConfig,
        value_column: str,
        mapping_manager: DatasetMappingManager,
        wrap_time_allowed: bool,
        time_based_data_adjustment: TimeBasedDataAdjustmentModel,
        to_geo_dim: DimensionBaseConfigWithFiles | None = None,
    ):
        op = mapping_manager.plan.map_time_op
        if mapping_manager.has_completed_operation(op):
            return load_data_df
        self._validate_daylight_saving_adjustment(time_based_data_adjustment)
        time_dim = self._config.get_time_dimension()
        assert time_dim is not None
        if time_dim.model.is_time_zone_required_in_geography():
            if self._config.model.use_project_geography_time_zone:
                if to_geo_dim is None:
                    msg = "Bug: to_geo_dim must be provided if time zone is required in geography."
                    raise Exception(msg)
                logger.info("Add time zone from project geography dimension.")
                geography_dim = to_geo_dim
            else:
                logger.info("Add time zone from dataset geography dimension.")
                geography_dim = self._config.get_dimension(DimensionType.GEOGRAPHY)
            load_data_df = add_time_zone(load_data_df, geography_dim)

        if isinstance(time_dim, AnnualTimeDimensionConfig):
            if not isinstance(to_time_dim, DateTimeDimensionConfig):
                msg = f"Annual time can only be mapped to DateTime: {to_time_dim.model.time_type}"
                raise NotImplementedError(msg)

            return map_annual_time_to_date_time(
                load_data_df,
                time_dim,
                to_time_dim,
                {value_column},
            )

        config = dsgrid.runtime_config
        if not time_dim.supports_chronify():
            # annual time is returned above
            # no mapping for no-op
            assert isinstance(
                time_dim, NoOpTimeDimensionConfig
            ), "Only NoOp and AnnualTimeDimensionConfig do not currently support Chronify"
            return load_data_df
        match (config.backend_engine, config.use_hive_metastore):
            case (BackendEngine.SPARK, True):
                table_name = make_temp_view_name()
                load_data_df = map_time_dimension_with_chronify_spark_hive(
                    df=save_to_warehouse(load_data_df, table_name),
                    table_name=table_name,
                    value_column=value_column,
                    from_time_dim=time_dim,
                    to_time_dim=to_time_dim,
                    scratch_dir_context=mapping_manager.scratch_dir_context,
                    time_based_data_adjustment=time_based_data_adjustment,
                    wrap_time_allowed=wrap_time_allowed,
                )

            case (BackendEngine.SPARK, False):
                filename = persist_table(
                    load_data_df,
                    mapping_manager.scratch_dir_context,
                    tag="query before time mapping",
                )
                load_data_df = map_time_dimension_with_chronify_spark_path(
                    df=read_dataframe(filename),
                    filename=filename,
                    value_column=value_column,
                    from_time_dim=time_dim,
                    to_time_dim=to_time_dim,
                    scratch_dir_context=mapping_manager.scratch_dir_context,
                    time_based_data_adjustment=time_based_data_adjustment,
                    wrap_time_allowed=wrap_time_allowed,
                )
            case (BackendEngine.DUCKDB, _):
                load_data_df = map_time_dimension_with_chronify_duckdb(
                    df=load_data_df,
                    value_column=value_column,
                    from_time_dim=time_dim,
                    to_time_dim=to_time_dim,
                    scratch_dir_context=mapping_manager.scratch_dir_context,
                    time_based_data_adjustment=time_based_data_adjustment,
                    wrap_time_allowed=wrap_time_allowed,
                )

        if time_dim.model.is_time_zone_required_in_geography():
            load_data_df = load_data_df.drop("time_zone")

        if op.persist:
            load_data_df = mapping_manager.persist_table(load_data_df, op)
        return load_data_df

    def _validate_daylight_saving_adjustment(self, time_based_data_adjustment):
        if (
            time_based_data_adjustment.daylight_saving_adjustment
            == DaylightSavingAdjustmentModel()
        ):
            return
        time_dim = self._config.get_time_dimension()
        if not isinstance(time_dim, IndexTimeDimensionConfig):
            assert time_dim is not None
            msg = f"time_based_data_adjustment.daylight_saving_adjustment does not apply to {time_dim.model.time_type=} time type, it applies to INDEX time type only."
            logger.warning(msg)

    def _remove_non_dimension_columns(self, df: DataFrame) -> DataFrame:
        allowed_columns = self._list_dimension_columns(df)
        return df.select(*allowed_columns)
