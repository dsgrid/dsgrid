import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DimensionSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.dataset import (
    apply_scaling_factor,
)
from dsgrid.utils.spark import (
    create_dataframe_from_ids,
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
        load_data_df = read_dataframe(config.load_data_path)
        time_dim = config.get_dimension(DimensionType.TIME)
        load_data_df = time_dim.convert_time_format(load_data_df)
        load_data_lookup = read_dataframe(config.load_data_lookup_path)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        return cls(load_data_df, load_data_lookup, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self):
        self._check_lookup_data_consistency()
        self._check_dataset_time_consistency(self._load_data)
        self._check_dataset_internal_consistency()

    def make_dimension_association_table(self) -> DataFrame:
        df = self._remap_dimension_columns(self._load_data_lookup, False)
        df = self._remove_non_dimension_columns(df).distinct()
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            pivoted_cols = self.get_pivoted_dimension_columns_mapped_to_project()
            pivoted_dims = create_dataframe_from_ids(
                pivoted_cols, self._config.get_pivoted_dimension_type().value
            )
            df = df.crossJoin(pivoted_dims)

        return df

    def make_project_dataframe(self, project_config):
        # TODO: Can we remove NULLs at registration time?
        null_lk_df = self._load_data_lookup.filter("id is NULL")
        lk_df = self._load_data_lookup.filter("id is not NULL")
        ld_df = self._load_data

        ld_df = ld_df.join(lk_df, on="id").drop("id")

        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            # There is currently no case that needs model years or value columns.
            ld_df = self._convert_time_dimension(ld_df, project_config)

        # TODO: This might need to handle data skew in the future.
        null_lk_df = self._remap_dimension_columns(null_lk_df, False)
        ld_df = self._remap_dimension_columns(ld_df, True)
        value_columns = set(ld_df.columns).intersection(self.get_value_columns_mapped_to_project())
        if SCALING_FACTOR_COLUMN in ld_df.columns:
            ld_df = apply_scaling_factor(ld_df, value_columns)
        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)
        if not convert_time_before_project_mapping:
            model_year_dim = project_config.get_base_dimension(DimensionType.MODEL_YEAR)
            model_years = get_unique_values(model_year_dim.get_records_dataframe(), "id")
            ld_df = self._convert_time_dimension(
                ld_df, project_config, model_years=model_years, value_columns=value_columns
            )

        ld_df = self._handle_unpivot_column_rename(ld_df)
        return self._add_null_values(ld_df, null_lk_df)

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        lk_df = self._load_data_lookup
        ld_df = self._load_data

        self._check_aggregations(context)
        ld_df = self._prefilter_stacked_dimensions(context, ld_df)
        lk_df = self._prefilter_stacked_dimensions(context, lk_df)
        null_lk_df = lk_df.filter("id is NULL")
        lk_df = lk_df.filter("id is not NULL")
        if self._config.get_table_format_type() == TableFormatType.PIVOTED:
            ld_df = self._prefilter_pivoted_dimensions(context, ld_df)
        ld_df = self._prefilter_time_dimension(context, ld_df)

        ld_df = ld_df.join(lk_df, on="id").drop("id")

        convert_time_before_project_mapping = self._convert_time_before_project_mapping()
        if convert_time_before_project_mapping:
            # There is currently no case that needs model years or value columns.
            ld_df = self._convert_time_dimension(ld_df, project_config)

        ld_df = self._remap_dimension_columns(
            ld_df,
            True,
            filtered_records=context.get_record_ids(),
            handle_data_skew=True,
            scratch_dir_context=context.scratch_dir_context,
        )
        value_columns = set(ld_df.columns).intersection(self.get_value_columns_mapped_to_project())
        if "scaling_factor" in ld_df.columns:
            ld_df = apply_scaling_factor(ld_df, value_columns)

        ld_df = self._apply_fraction(ld_df, value_columns)
        project_metric_records = project_config.get_base_dimension(
            DimensionType.METRIC
        ).get_records_dataframe()
        ld_df = self._convert_units(ld_df, project_metric_records, value_columns)
        null_lk_df = self._remap_dimension_columns(
            null_lk_df, False, filtered_records=context.get_record_ids()
        )

        if not convert_time_before_project_mapping:
            m_year_df = context.try_get_record_ids_by_dimension_type(DimensionType.MODEL_YEAR)
            if m_year_df is None:
                model_years = project_config.get_base_dimension(
                    DimensionType.MODEL_YEAR
                ).get_unique_ids()
            else:
                model_years = get_unique_values(m_year_df, "id")
            ld_df = self._convert_time_dimension(
                ld_df, project_config, model_years=model_years, value_columns=value_columns
            )

        ld_df = self._add_null_values(ld_df, null_lk_df)
        return self._finalize_table(context, ld_df, value_columns, project_config)

    @staticmethod
    def _add_null_values(ld_df, null_lk_df):
        if not null_lk_df.rdd.isEmpty():
            for col in set(ld_df.columns).difference(null_lk_df.columns):
                null_lk_df = null_lk_df.withColumn(col, F.lit(None))
            ld_df = ld_df.union(null_lk_df.select(*ld_df.columns))

        return ld_df

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
            if not self._load_data_lookup.schema[col].dataType is StringType():
                msg = f"dimension column {col} must have data type = StringType"
                raise DSGInvalidDataset(msg)
            dimension_types.add(DimensionType.from_column(col))

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        load_data_dimensions = {DimensionType.TIME}
        match self._config.get_table_format_type():
            case TableFormatType.PIVOTED:
                load_data_dimensions.add(DimensionType(self._config.get_pivoted_dimension_type()))
            case TableFormatType.UNPIVOTED:
                for col in self._load_data.columns:
                    if col not in ("id", VALUE_COLUMN):
                        load_data_dimensions.add(DimensionType(col))
            case _:
                msg = str(self._config.get_table_format_type())
                raise NotImplementedError(msg)
        expected_dimensions = {d for d in DimensionType if d not in load_data_dimensions}
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
        match self._config.get_table_format_type():
            case TableFormatType.PIVOTED:
                self._check_load_data_pivoted_columns()
            case TableFormatType.UNPIVOTED:
                self._check_load_data_unpivoted_value_column(self._load_data)
        ld_ids = self._load_data.select("id").distinct()
        ldl_ids = self._load_data_lookup.select("id").distinct()

        with Timer(timer_stats_collector, "check load_data for nulls"):
            if not self._load_data.select("id").filter("id is NULL").rdd.isEmpty():
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
                diff = ld_ids.unionAll(ldl_ids).exceptAll(ld_ids.intersect(ldl_ids))
                # TODO: Starting with Python 3.10 and Spark 3.3.0, this fails unless we call cache.
                # Works fine on Python 3.9 and Spark 3.2.0. Haven't debugged further.
                # The size should not cause a problem.
                diff.cache()
                diff_count = diff.count()
                limit = 100
                if diff_count < limit:
                    diff_list = diff.collect()
                else:
                    diff_list = diff.limit(limit).collect()
                logger.error(
                    "load_data and load_data_lookup have %s different IDs: %s",
                    diff_count,
                    diff_list,
                )
            raise DSGInvalidDataset(
                f"Data IDs for {self._config.config_id} data/lookup are inconsistent"
            )

    def _check_load_data_pivoted_columns(self):
        logger.info("Check load data pivoted columns.")
        dim_type = self._config.get_pivoted_dimension_type()
        dimension_records = set(self._config.get_pivoted_dimension_columns())
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_load_data_time_columns())

        found_id = False
        pivoted_cols = set()
        for col in self._load_data.columns:
            if col == "id":
                found_id = True
                continue
            if col in time_columns:
                continue
            if col in dimension_records:
                pivoted_cols.add(col)
            else:
                raise DSGInvalidDataset(f"column={col} is not expected in load_data.")

        if not found_id:
            raise DSGInvalidDataset("load_data does not include an 'id' column")

        if dimension_records != pivoted_cols:
            missing = dimension_records.difference(pivoted_cols)
            raise DSGInvalidDataset(
                f"load_data is missing {missing} columns for dimension={dim_type.value} based on records."
            )

    @track_timing(timer_stats_collector)
    def filter_data(self, dimensions: list[DimensionSimpleModel]):
        lookup = self._load_data_lookup
        lookup.cache()
        load_df = self._load_data
        lookup_columns = set(lookup.columns)
        time_columns = set(
            self._config.get_dimension(DimensionType.TIME).get_load_data_time_columns()
        )
        pivoted_columns_to_remove = set()
        pivoted_dimension_type = self._config.get_pivoted_dimension_type()
        if pivoted_dimension_type is not None:
            pivoted_columns = set(load_df.columns) - lookup_columns - time_columns
            for dim in dimensions:
                column = dim.dimension_type.value
                if column in lookup_columns:
                    lookup = lookup.filter(lookup[column].isin(dim.record_ids))
                elif dim.dimension_type == pivoted_dimension_type:
                    pivoted_columns_to_remove = pivoted_columns.difference(dim.record_ids)

        drop_columns = []
        for dim in self._config.model.trivial_dimensions:
            col = dim.value
            count = lookup.select(col).distinct().count()
            assert count == 1, f"{dim}: count"
            drop_columns.append(col)
        lookup = lookup.drop(*drop_columns)

        lookup2 = lookup.coalesce(1)
        lookup2 = overwrite_dataframe_file(self._config.load_data_lookup_path, lookup2)
        lookup.unpersist()
        logger.info("Rewrote simplified %s", self._config.load_data_lookup_path)
        ids = next(iter(lookup2.select("id").distinct().select(F.collect_list("id")).first()))

        load_df = self._load_data.filter(self._load_data.id.isin(ids))
        load_df = load_df.drop(*pivoted_columns_to_remove)
        path = Path(self._config.load_data_path)
        if path.suffix == ".csv":
            # write_dataframe_and_auto_partition doesn't support CSV yet
            overwrite_dataframe_file(path, load_df)
        else:
            write_dataframe_and_auto_partition(load_df, path)
        logger.info("Rewrote simplified %s", self._config.load_data_path)
