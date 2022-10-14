import itertools
import logging
from pathlib import Path
from typing import List

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DatasetSimpleModel
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dataset.pivoted_table import PivotedTableHandler
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidQuery
from dsgrid.query.models import TableFormatType
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.dataset import (
    check_null_value_in_unique_dimension_rows,
)
from dsgrid.utils.spark import (
    # create_dataframe_from_pandas,
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
        load_data_lookup = read_dataframe(config.load_data_lookup_path, cache=True)
        load_data_lookup = config.add_trivial_dimensions(load_data_lookup)
        return cls(load_data_df, load_data_lookup, config, *args, **kwargs)

    @track_timing(timer_stats_collector)
    def check_consistency(self):
        self._check_lookup_data_consistency()
        self._check_dataset_time_consistency(self._load_data)
        self._check_dataset_internal_consistency()

    @track_timing(timer_stats_collector)
    def get_unique_dimension_rows(self):
        """Get distinct combinations of remapped dimensions, including id.
        Check each col in combination for null value."""
        dim_table = self._remap_dimension_columns(self._load_data_lookup).distinct()
        check_null_value_in_unique_dimension_rows(dim_table)
        return dim_table

    def get_time_zone_mapping(self):
        lk_df = self._load_data_lookup.filter("id is not NULL")
        return self._add_time_zone(lk_df).select("id", "time_zone").distinct()

    def make_project_dataframe(self):
        # TODO: Can we remove NULLs at registration time?
        lk_df = self._load_data_lookup.filter("id is not NULL")
        ld_df = self._convert_time_dimension(self._load_data, self.get_time_zone_mapping())
        lk_df = self._remap_dimension_columns(lk_df)
        ld_df = self._remap_dimension_columns(
            ld_df,
            # Some pivot columns may have been removed.
            pivoted_columns=set(self._load_data.columns).intersection(
                self.get_pivoted_dimension_columns()
            ),
        )
        # TODO: handle fraction application
        # Currently this requires fraction = 1.0
        ld_df = ld_df.join(lk_df, on="id").drop("id")
        return ld_df

    def make_project_dataframe_from_query(self, context: QueryContext, project_config):
        lk_df = self._load_data_lookup.filter("id is not NULL")
        ld_df = self._convert_time_dimension(self._load_data, self.get_time_zone_mapping())

        self._check_aggregations(context)
        lk_df, ld_df = self._prefilter_dataset(context, lk_df, ld_df)

        lk_df = self._remap_dimension_columns(lk_df)
        # Some pivoted columns may have been removed in pre-filtering.
        pivoted_columns = set(ld_df.columns).intersection(self.get_pivoted_dimension_columns())
        ld_df = self._remap_dimension_columns(ld_df, pivoted_columns=pivoted_columns)

        pivoted_columns = set(ld_df.columns).intersection(
            self.get_pivoted_dimension_columns_mapped_to_project()
        )
        context.add_dataset_metadata(self.dataset_id)
        context.set_pivoted_columns(pivoted_columns, dataset_id=self.dataset_id)
        context.set_pivoted_dimension_type(
            self.get_pivoted_dimension_type(), dataset_id=self.dataset_id
        )
        context.set_table_format_type(TableFormatType.PIVOTED, dataset_id=self.dataset_id)
        for dim_type, name in project_config.get_base_dimension_to_query_name_mapping().items():
            context.add_dimension_query_name(dim_type, name, dataset_id=self.dataset_id)

        # It should be cheaper to do this before the join with lookup.
        table_handler = PivotedTableHandler(project_config, dataset_id=self.dataset_id)
        ld_df = table_handler.process_pivoted_aggregations(
            ld_df, context.model.project.per_dataset_aggregations, context
        )

        # TODO: handle fraction application
        # Currently this requires fraction = 1.0
        ld_df = ld_df.join(lk_df, on="id").drop("id")

        ld_df = table_handler.convert_columns_to_query_names(ld_df)
        ld_df = table_handler.process_stacked_aggregations(
            ld_df, context.model.project.per_dataset_aggregations, context
        )

        return ld_df

    def _check_aggregations(self, context):
        pivoted_type = self.get_pivoted_dimension_type()
        for agg in itertools.chain(
            context.model.project.per_dataset_aggregations, context.model.result.aggregations
        ):
            if not getattr(agg.dimensions, pivoted_type.value):
                raise DSGInvalidQuery(
                    f"Pivoted dimension type {pivoted_type.value} is not included in an aggregation"
                )

    def _prefilter_dataset(self, context: QueryContext, lk_df, ld_df):
        # TODO: prefilter time

        for dim_type, project_record_ids in context.iter_record_ids_by_dimension_type():
            dataset_mapping = self._get_dataset_to_project_mapping_records(dim_type)
            if dataset_mapping is None:
                dataset_record_ids = project_record_ids
            else:
                dataset_record_ids = (
                    dataset_mapping.withColumnRenamed("from_id", "dataset_record_id")
                    .join(
                        project_record_ids,
                        on=dataset_mapping.to_id == project_record_ids.id,
                    )
                    .selectExpr("dataset_record_id AS id")
                    .distinct()
                )
            if dim_type == self.get_pivoted_dimension_type():
                # Drop columns that don't match requested project record IDs.
                cols_to_keep = {x.id for x in dataset_record_ids.collect()}
                cols_to_drop = set(self.get_pivoted_dimension_columns()).difference(cols_to_keep)
                if cols_to_drop:
                    ld_df = ld_df.drop(*cols_to_drop)
            else:
                # Drop rows that don't match requested project record IDs.
                tmp = dataset_record_ids.withColumnRenamed("id", "dataset_record_id")
                lk_df = lk_df.join(
                    tmp,
                    on=lk_df[dim_type.value] == tmp.dataset_record_id,
                ).drop("dataset_record_id")

        return lk_df, ld_df

    @track_timing(timer_stats_collector)
    def get_dataframe(self, query_context: QueryContext, project_config):
        return self.make_project_dataframe_from_query(
            context=query_context,
            project_config=project_config,
        )

    @track_timing(timer_stats_collector)
    def _check_lookup_data_consistency(self):
        """Dimension check in load_data_lookup, excludes time:
        * check that data matches record for each dimension.
        * check that all data dimension combinations exist. Time is handled separately.
        """
        logger.info("Check lookup data consistency.")
        found_id = False
        dimension_types = set()
        for col in self._load_data_lookup.columns:
            if col == "id":
                found_id = True
                continue
            dimension_types.add(DimensionType.from_column(col))

        if not found_id:
            raise DSGInvalidDataset("load_data_lookup does not include an 'id' column")

        load_data_dimensions = (
            DimensionType.TIME,
            self._config.model.data_schema.load_data_column_dimension,
        )
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
        self._check_load_data_columns()
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

    @track_timing(timer_stats_collector)
    def _check_load_data_columns(self):
        logger.info("Check load data columns.")
        dim_type = self._config.model.data_schema.load_data_column_dimension
        dimension_records = set(self.get_pivoted_dimension_columns())
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())

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
    def filter_data(self, dimensions: List[DatasetSimpleModel]):
        lookup = self._load_data_lookup
        # lookup.cache()
        pivoted_dimension_type = self.get_pivoted_dimension_type()
        pivoted_columns = set(self.get_pivoted_dimension_columns())
        pivoted_columns_to_keep = set()
        lookup_columns = set(lookup.columns)
        for dim in dimensions:
            column = dim.dimension_type.value
            if column in lookup_columns:
                lookup = lookup.filter(lookup[column].isin(dim.record_ids))
            elif dim.dimension_type == pivoted_dimension_type:
                pivoted_columns_to_keep.update(set(dim.record_ids))

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
        pivoted_columns_to_remove = list(pivoted_columns.difference(pivoted_columns_to_keep))
        load_df = load_df.drop(*pivoted_columns_to_remove)
        path = Path(self._config.load_data_path)
        if path.suffix == ".csv":
            # write_dataframe_and_auto_partition doesn't support CSV yet
            overwrite_dataframe_file(path, load_df)
        else:
            write_dataframe_and_auto_partition(load_df, path)
        logger.info("Rewrote simplified %s", self._config.load_data_path)
