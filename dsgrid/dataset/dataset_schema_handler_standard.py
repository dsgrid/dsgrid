import logging
from pathlib import Path

from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.dataset.models import TableFormatType
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.query_context import QueryContext
from dsgrid.spark.functions import (
    cache,
    coalesce,
    collect_list,
    cross_join,
    except_all,
    intersect,
    is_dataframe_empty,
    unpersist,
)
from dsgrid.spark.types import (
    DataFrame,
    StringType,
)
from dsgrid.utils.dataset import (
    add_null_rows_from_load_data_lookup,
    apply_scaling_factor,
    convert_types_if_necessary,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    read_dataframe,
    get_unique_values,
    overwrite_dataframe_file,
    write_dataframe_and_auto_partition,
)
from dsgrid.utils.timing import Timer, timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


class StandardDatasetSchemaHandler(DatasetSchemaHandlerBase):
    """define interface/required behaviors for STANDARD dataset schema"""

    def __init__(self, load_data_df, load_data_lookup, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._load_data = load_data_df
        self._load_data_lookup = load_data_lookup

    @classmethod
    def load(cls, config: DatasetConfig, *args, **kwargs):
        load_data_df = convert_types_if_necessary(read_dataframe(config.load_data_path))
        time_dim = config.get_dimension(DimensionType.TIME)
        load_data_df = time_dim.convert_time_format(load_data_df)
        load_data_lookup = read_dataframe(config.load_data_lookup_path)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        load_data_lookup = convert_types_if_necessary(load_data_lookup)
        return cls(load_data_df, load_data_lookup, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self):
        self._check_lookup_data_consistency()
        self._check_dataset_internal_consistency()

    @track_timing(timer_stats_collector)
    def check_time_consistency(self):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.supports_chronify():
            self._check_dataset_time_consistency_with_chronify()
        else:
            self._check_dataset_time_consistency(
                self._load_data.join(self._load_data_lookup, on="id")
            )

    def make_dimension_association_table(self, context: ScratchDirContext) -> DataFrame:
        lk_df = self._load_data_lookup.filter("id is not NULL")
        dim_cols = self._list_dimension_columns(self._load_data)
        df = self._load_data.select("id", *dim_cols).distinct()
        df = df.join(lk_df, on="id").drop("id")
        mapping_plan = self.build_default_dataset_mapping_plan()
        with DatasetMappingManager(self.dataset_id, mapping_plan, context) as mapping_manager:
            df = self._remap_dimension_columns(df, mapping_manager).drop("fraction")
        with DatasetMappingManager(self.dataset_id, mapping_plan, context) as mapping_manager:
            null_lk_df = self._remap_dimension_columns(
                self._load_data_lookup.filter("id is NULL"),
                mapping_manager,
            ).drop("fraction")
        return add_null_rows_from_load_data_lookup(
            df, cross_join(null_lk_df, df.select(*dim_cols).distinct())
        )

    def make_project_dataframe(
        self, context: QueryContext, project_config: ProjectConfig
    ) -> DataFrame:
        lk_df = self._load_data_lookup
        lk_df = self._prefilter_stacked_dimensions(context, lk_df)
        null_lk_df = lk_df.filter("id is NULL")
        lk_df = lk_df.filter("id is not NULL")

        plan = context.model.project.get_dataset_mapping_plan(self.dataset_id)
        if plan is None:
            plan = self.build_default_dataset_mapping_plan()
        with context.dataset_mapping_manager(self.dataset_id, plan) as mapping_manager:
            ld_df = mapping_manager.try_read_checkpointed_table()
            if ld_df is None:
                ld_df = self._load_data
                ld_df = self._prefilter_stacked_dimensions(context, ld_df)
                ld_df = self._prefilter_time_dimension(context, ld_df)
                ld_df = ld_df.join(lk_df, on="id").drop("id")

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
            lk_plan = self.build_default_dataset_mapping_plan()
            with DatasetMappingManager(
                self.dataset_id, lk_plan, context.scratch_dir_context
            ) as lk_mapping_manager:
                null_lk_df = self._remap_dimension_columns(
                    null_lk_df, lk_mapping_manager, filtered_records=context.get_record_ids()
                )
            ld_df = self._convert_time_dimension(
                ld_df, project_config, VALUE_COLUMN, mapping_manager
            )
            ld_df = add_null_rows_from_load_data_lookup(ld_df, null_lk_df)
            return self._finalize_table(context, ld_df, project_config)

    @track_timing(timer_stats_collector)
    def _check_lookup_data_consistency(self):
        """Dimension check in load_data_lookup, excludes time:
        * check that data matches record for each dimension.
        * check that all data dimension combinations exist. Time is handled separately.
        * Check for any NULL values in dimension columns.
        """
        logger.info("Check lookup data consistency.")
        found_id = False
        dimension_types = set()
        for col in self._load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            if col == SCALING_FACTOR_COLUMN:
                continue
            if self._load_data_lookup.schema[col].dataType != StringType():
                msg = f"dimension column {col} must have data type = StringType"
                raise DSGInvalidDataset(msg)
            dimension_types.add(DimensionType.from_column(col))

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        load_data_dimensions = set(self._list_dimension_types_in_load_data(self._load_data))
        expected_dimensions = {
            d
            for d in DimensionType.get_dimension_types_allowed_as_columns()
            if d not in load_data_dimensions
        }
        missing_dimensions = expected_dimensions.difference(dimension_types)
        if missing_dimensions:
            raise DSGInvalidDataset(
                f"load_data_lookup is missing dimensions: {missing_dimensions}. "
                "If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = self._config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            lookup_records = get_unique_values(self._load_data_lookup, name)
            if None in lookup_records:
                raise DSGInvalidDataset(
                    f"{self._config.config_id} has a NULL value for {dimension_type}"
                )
            if dim_records != lookup_records:
                logger.error(
                    "Mismatch in load_data_lookup records. dimension=%s mismatched=%s",
                    name,
                    lookup_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data_lookup records do not match dimension records for {name}"
                )

    @track_timing(timer_stats_collector)
    def _check_dataset_internal_consistency(self):
        """Check load_data dimensions and id series."""
        logger.info("Check dataset internal consistency.")
        assert (
            self._config.get_table_format_type() == TableFormatType.UNPIVOTED
        ), self._config.get_table_format_type()
        self._check_load_data_unpivoted_value_column(self._load_data)

        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_load_data_time_columns())
        allowed_columns = (
            DimensionType.get_allowed_dimension_column_names()
            .union(time_columns)
            .union({VALUE_COLUMN, "id", "scaling_factor"})
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

        ld_ids = self._load_data.select("id").distinct()
        ldl_ids = self._load_data_lookup.select("id").distinct()

        with Timer(timer_stats_collector, "check load_data for nulls"):
            if not is_dataframe_empty(self._load_data.select("id").filter("id is NULL")):
                raise DSGInvalidDataset(
                    f"load_data for dataset {self._config.config_id} has a null ID"
                )

        with Timer(timer_stats_collector, "check load_data ID count"):
            data_id_count = ld_ids.count()

        with Timer(timer_stats_collector, "compare load_data and load_data_lookup IDs"):
            joined = ld_ids.join(ldl_ids, on="id")
            count = joined.count()

        if data_id_count != count:
            with Timer(timer_stats_collector, "show load_data and load_data_lookup ID diff"):
                diff = except_all(ld_ids.unionAll(ldl_ids), intersect(ld_ids, ldl_ids))
                # Only run the query once (with Spark). Number of rows shouldn't be a problem.
                cache(diff)
                diff_count = diff.count()
                limit = 100
                diff_list = diff.limit(limit).collect()
                unpersist(diff)
                logger.error(
                    "load_data and load_data_lookup have %s different IDs. Limited to %s: %s",
                    diff_count,
                    limit,
                    diff_list,
                )
            raise DSGInvalidDataset(
                f"Data IDs for {self._config.config_id} data/lookup are inconsistent"
            )

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel]):
        lookup = self._load_data_lookup
        cache(lookup)
        load_df = self._load_data
        lookup_columns = set(lookup.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in lookup_columns:
                lookup = lookup.filter(lookup[column].isin(dim.record_ids))

        drop_columns = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = lookup.select(col).distinct().count()
            assert count == 1, f"{dim}: count"
            drop_columns.append(col)
        lookup = lookup.drop(*drop_columns)

        lookup2 = coalesce(lookup, 1)
        lookup2 = overwrite_dataframe_file(self._config.load_data_lookup_path, lookup2)
        unpersist(lookup)
        logger.info("Rewrote simplified %s", self._config.load_data_lookup_path)
        ids = collect_list(lookup2.select("id").distinct(), "id")
        load_df = self._load_data.filter(self._load_data.id.isin(ids))
        ld_columns = set(load_df.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in ld_columns:
                load_df = load_df.filter(load_df[column].isin(dim.record_ids))

        path = Path(self._config.load_data_path)
        if path.suffix == ".csv":
            # write_dataframe_and_auto_partition doesn't support CSV yet
            overwrite_dataframe_file(path, load_df)
        else:
            write_dataframe_and_auto_partition(load_df, path)
        logger.info("Rewrote simplified %s", self._config.load_data_path)
