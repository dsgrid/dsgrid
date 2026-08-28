import ibis
import logging
from typing import Any, Self, cast

from dsgrid.common import SCALING_FACTOR_COLUMN, TIME_ZONE_COLUMN, VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.models import ValueFormat
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DatasetDimensionRequirements, DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.models import DatasetQueryModel, ProjectQueryModel
from dsgrid.query.query_context import QueryContext
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.ibis.functions import cache, unpersist
from dsgrid.ibis.operations import (
    coalesce,
    drop_columns,
    except_all,
    intersect,
    join_multiple_columns,
    union_all,
)
from dsgrid.ibis.table_utils import count_distinct, count_rows, table_to_records
from dsgrid.ibis.types import is_string_column
from dsgrid.utils.dataset import (
    apply_scaling_factor,
    convert_types_if_necessary,
)
from dsgrid.config.file_schema import read_data_file
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.null_checks import check_for_nulls
from dsgrid.utils.timing import Timer, timer_stats_collector, track_timing


logger = logging.getLogger(__name__)


class TwoTableDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """Handler for TWO_TABLE dataset format (load_data + load_data_lookup tables)."""

    def __init__(self, load_data_df, load_data_lookup, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df
        self._load_data_lookup = load_data_lookup

    @classmethod
    def load(
        cls,
        config: DatasetConfig,
        *args,
        store: DataStoreInterface | None = None,
        scratch_dir_context: ScratchDirContext | None = None,
        **kwargs,
    ) -> Self:
        if store is None:
            if config.data_file_schema is None:
                msg = "Cannot load dataset without data file schema or store"
                raise DSGInvalidDataset(msg)
            if config.lookup_file_schema is None:
                msg = "TWO_TABLE format requires lookup_data_file"
                raise DSGInvalidDataset(msg)
            load_data_df = read_data_file(
                config.data_file_schema, scratch_dir_context=scratch_dir_context
            )
            load_data_lookup = read_data_file(
                config.lookup_file_schema, scratch_dir_context=scratch_dir_context
            )
        else:
            load_data_df = store.read_table(config.model.dataset_id, config.model.version)
            load_data_lookup = store.read_lookup_table(
                config.model.dataset_id, config.model.version
            )

        load_data_df = convert_types_if_necessary(load_data_df)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        load_data_lookup = convert_types_if_necessary(load_data_lookup)
        return cls(load_data_df, load_data_lookup, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(
        self,
        expected_dimension_associations: dict[str, ibis.Table],
        missing_dimension_associations: dict[str, ibis.Table],
        scratch_dir_context: ScratchDirContext,
        requirements: DatasetDimensionRequirements,
    ) -> None:
        self._check_lookup_data_consistency()
        self._check_dataset_internal_consistency()
        self._check_dimension_associations(
            expected_dimension_associations,
            missing_dimension_associations,
            scratch_dir_context,
            requirements,
        )

    @track_timing(timer_stats_collector)
    def check_time_consistency(self):
        time_dim = self._config.get_time_dimension()
        if time_dim is None:
            return None

        if time_dim.supports_chronify():
            self._check_dataset_time_consistency_with_chronify()
        else:
            self._check_dataset_time_consistency(self._get_load_data_table())

    def get_base_load_data_table(self) -> ibis.Table:
        return self._load_data

    def _get_load_data_table(self) -> ibis.Table:
        return join_multiple_columns(self._load_data, self._load_data_lookup, ["id"])

    def _make_actual_dimension_association_table_from_data(self) -> ibis.Table:
        """Override base implementation to avoid joining all time-series rows.

        The base class joins the full load_data (potentially billions of time-step rows)
        with the lookup table and then calls .distinct(). For the two-table schema the
        same result can be obtained far more cheaply:
          1. Select only the columns from load_data that are dimension columns (typically
             just "metric"), plus "id", and take distinct — eliminating all time steps.
          2. Join that small table with the lookup's dimension columns.
        This reduces the data scanned from O(ids × metrics × time_steps) to
        O(ids × metrics).
        """
        ld_dim_cols = self._list_dimension_columns(self._load_data)
        distinct_ld = self._load_data.select("id", *ld_dim_cols).distinct()

        lkp_dim_cols = self._list_dimension_columns(self._load_data_lookup)
        distinct_lkp = self._load_data_lookup.select("id", *lkp_dim_cols).distinct()

        joined = join_multiple_columns(distinct_ld, distinct_lkp, ["id"])
        return joined.select(*ld_dim_cols, *lkp_dim_cols).distinct()

    def make_project_dataframe(
        self, context: QueryContext, project_config: ProjectConfig
    ) -> ibis.Table:
        lk_df = self._load_data_lookup
        lk_df = self._prefilter_stacked_dimensions(context, lk_df)

        query = cast(ProjectQueryModel, context.model)
        plan = query.project.get_dataset_mapping_plan(self.dataset_id)
        if plan is None:
            plan = self.build_default_dataset_mapping_plan()
        with context.dataset_mapping_manager(self.dataset_id, plan) as mapping_manager:
            ld_df = mapping_manager.try_read_checkpointed_table()
            if ld_df is None:
                ld_df = self._load_data
                ld_df = self._prefilter_stacked_dimensions(context, ld_df)
                ld_df = self._prefilter_time_dimension(context, ld_df)
                ld_df = drop_columns(join_multiple_columns(ld_df, lk_df, ["id"]), "id")

            ld_df = self._remap_dimension_columns(
                ld_df,
                mapping_manager,
                filtered_records=context.get_record_ids(),
            )
            if SCALING_FACTOR_COLUMN in ld_df.columns:
                ld_df = apply_scaling_factor(ld_df, VALUE_COLUMN, mapping_manager)

            ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN}, mapping_manager)
            project_metric_records = self._get_project_metric_records(project_config)
            ld_df = self._convert_units(ld_df, project_metric_records, mapping_manager)
            input_dataset = project_config.get_dataset(self._config.model.dataset_id)
            ld_df = self._convert_time_dimension(
                load_data_df=ld_df,
                to_time_dim=project_config.get_base_time_dimension(),
                value_column=VALUE_COLUMN,
                mapping_manager=mapping_manager,
                wrap_time_allowed=input_dataset.wrap_time_allowed,
                time_based_data_adjustment=input_dataset.time_based_data_adjustment,
                to_geo_dim=project_config.get_base_dimension(DimensionType.GEOGRAPHY),
            )
            return self._finalize_table(context, ld_df, project_config)

    def make_mapped_dataframe(
        self,
        context: QueryContext,
        time_dimension: TimeDimensionBaseConfig | None = None,
    ) -> ibis.Table:
        query = context.model
        assert isinstance(query, DatasetQueryModel)
        plan = query.mapping_plan
        if plan is None:
            plan = self.build_default_dataset_mapping_plan()
        geography_dimension = self._get_mapping_to_dimension(DimensionType.GEOGRAPHY)
        metric_dimension = self._get_mapping_to_dimension(DimensionType.METRIC)
        with context.dataset_mapping_manager(self.dataset_id, plan) as mapping_manager:
            ld_df = mapping_manager.try_read_checkpointed_table()
            if ld_df is None:
                ld_df = self._load_data
                lk_df = self._load_data_lookup
                ld_df = drop_columns(join_multiple_columns(ld_df, lk_df, ["id"]), "id")

            ld_df = self._remap_dimension_columns(
                ld_df,
                mapping_manager,
            )
            if SCALING_FACTOR_COLUMN in ld_df.columns:
                ld_df = apply_scaling_factor(ld_df, VALUE_COLUMN, mapping_manager)

            ld_df = self._apply_fraction(ld_df, {VALUE_COLUMN}, mapping_manager)
            if metric_dimension is not None:
                metric_dimension = cast(Any, metric_dimension)
                metric_records = metric_dimension.get_records_dataframe()
                ld_df = self._convert_units(ld_df, metric_records, mapping_manager)
            if time_dimension is not None:
                ld_df = self._convert_time_dimension(
                    load_data_df=ld_df,
                    to_time_dim=time_dimension,
                    value_column=VALUE_COLUMN,
                    mapping_manager=mapping_manager,
                    wrap_time_allowed=query.wrap_time_allowed,
                    time_based_data_adjustment=query.time_based_data_adjustment,
                    to_geo_dim=geography_dimension,
                )
        return ld_df

    @track_timing(timer_stats_collector)
    def _check_lookup_data_consistency(self):
        """Dimension check in load_data_lookup, excludes time.

        Checks:
        - Data matches record for each dimension.
        - All data dimension combinations exist. Time is handled separately.
        - No NULL values in dimension columns.
        """
        logger.info("Check lookup data consistency.")
        found_id = False
        dimension_types = set()
        for col in self._load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            if col in (SCALING_FACTOR_COLUMN, TIME_ZONE_COLUMN):
                continue
            if not is_string_column(self._load_data_lookup, col):
                msg = f"dimension column {col} must have data type = StringType"
                raise DSGInvalidDataset(msg)
            dimension_types.add(DimensionType.from_column(col))

        if not found_id:
            msg = "load_data_lookup does not include an 'id' column"
            raise DSGInvalidDataset(msg)

        check_for_nulls(self._load_data_lookup)
        load_data_dimensions = set(self._list_dimension_types_in_load_data(self._load_data))
        expected_dimensions = {
            d
            for d in DimensionType.get_dimension_types_allowed_as_columns()
            if d not in load_data_dimensions
        }
        missing_dimensions = expected_dimensions.difference(dimension_types)
        if missing_dimensions:
            msg = (
                f"load_data_lookup is missing dimensions: {missing_dimensions}. "
                "If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

    @track_timing(timer_stats_collector)
    def _check_dataset_internal_consistency(self):
        """Check load_data dimensions and id series."""
        logger.info("Check dataset internal consistency.")
        assert (
            self._config.get_value_format() == ValueFormat.STACKED
        ), self._config.get_value_format()
        self._check_load_data_unpivoted_value_column(self._load_data)

        time_dim = self._config.get_time_dimension()
        time_columns: set[str] = set()
        if time_dim is not None:
            time_columns = set(time_dim.get_load_data_time_columns())
        allowed_columns = (
            DimensionType.get_allowed_dimension_column_names()
            .union(time_columns)
            .union({VALUE_COLUMN, TIME_ZONE_COLUMN, "id", "scaling_factor"})
        )

        found_id = False
        for column in self._load_data.columns:
            if column not in allowed_columns:
                msg = f"{column=} is not expected in load_data"
                raise DSGInvalidDataset(msg)
            if column == "id":
                found_id = True

        if not found_id:
            msg = "load_data does not include an 'id' column"
            raise DSGInvalidDataset(msg)

        check_for_nulls(self._load_data)
        ld_ids = self._load_data.select("id").distinct()
        ldl_ids = self._load_data_lookup.select("id").distinct()
        ldl_id_count = count_rows(ldl_ids)
        data_id_count = count_rows(ld_ids)
        joined = join_multiple_columns(ld_ids, ldl_ids, ["id"])
        count = count_rows(joined)

        if data_id_count != count or ldl_id_count != count:
            with Timer(timer_stats_collector, "show load_data and load_data_lookup ID diff"):
                # Cache so the diff is computed once and reused by the count and
                # sample below. The number of rows shouldn't be a problem.
                diff = cache(except_all(union_all(ld_ids, ldl_ids), intersect(ld_ids, ldl_ids)))
                diff_count = count_rows(diff)
                limit = 100
                diff_list = table_to_records(diff.limit(limit))
                unpersist(diff)
                logger.error(
                    "load_data and load_data_lookup have %s different IDs. Limited to %s: %s",
                    diff_count,
                    limit,
                    diff_list,
                )
            msg = f"Data IDs for {self._config.config_id} data/lookup are inconsistent"
            raise DSGInvalidDataset(msg)

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel], store: DataStoreInterface):
        lookup = self._load_data_lookup
        load_df = self._load_data
        lookup_columns = set(lookup.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in lookup_columns:
                lookup = lookup.filter(lookup[column].isin(dim.record_ids))

        # Cache while running one count per trivial dimension; the CachedTable is
        # garbage-collected after the loop and Ibis releases the cached data.
        cached_lookup = cache(lookup)
        columns_to_drop = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = count_distinct(cached_lookup, col)
            assert count == 1, f"{dim}: count"
            columns_to_drop.append(col)
        del cached_lookup
        lookup = drop_columns(lookup, *columns_to_drop)

        lookup2 = coalesce(lookup, 1)
        store.replace_lookup_table(lookup2, self.dataset_id, self._config.model.version)
        # Re-read the lookup after the replace so that subsequent operations do not reference
        # the previous on-disk part files, which have been deleted.
        lookup2 = store.read_lookup_table(self.dataset_id, self._config.model.version)
        load_df = join_multiple_columns(load_df, lookup2.select("id").distinct(), ["id"])
        ld_columns = set(load_df.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in ld_columns:
                load_df = load_df.filter(load_df[column].isin(dim.record_ids))

        store.replace_table(load_df, self.dataset_id, self._config.model.version)
        logger.info("Rewrote simplified %s", self._config.model.dataset_id)
