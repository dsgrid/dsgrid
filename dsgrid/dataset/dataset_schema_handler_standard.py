import logging

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.query import QueryContext
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import Timer, timer_stats_collector, track_timing
from dsgrid.dataset.dataset_schema_handler_base import DatasetSchemaHandlerBase
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset

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
        self._check_null_value_in_unique_dimension_rows(dim_table)

        return dim_table

    def make_project_dataframe(
        self, load_data_df=None, lookup_df=None, query_context: QueryContext = None
    ):
        ld_df = self._remap_dimension_columns(load_data_df or self._load_data)
        lk_df = self._remap_dimension_columns(lookup_df or self._load_data_lookup)

        if query_context is not None:
            for dimension_type, records in query_context.iter_alt_dimension_records():
                column = dimension_type.value
                if column == self.get_pivot_dimension_type().value:
                    # This class requires that the pivot dimension is metric and so operation
                    # is supported.
                    model = getattr(query_context.model.alternate_dimensions, column)
                    ld_df = self._map_and_reduce_pivot_dimension(
                        ld_df,
                        records,
                        set(self.get_pivot_dimension_columns_mapped_to_project()),
                        operation=model.operation,
                        keep_individual_fields=model.keep_individual_fields,
                    )
                else:
                    ld_df = self._map_and_reduce_non_pivot_dimension(
                        ld_df,
                        records,
                        column,
                    )
                if column in lk_df.columns:
                    lk_df = self._map_and_reduce_non_pivot_dimension(lk_df, records, column)
                logger.info("Replaced dimensions with alternate records %s", column)

        ld_df = ld_df.join(lk_df.drop("fraction"), on="id").drop("id")

        # Map the time here because some columns may have been collapsed above.
        ld_df = self._convert_time_dimension(ld_df)

        # TODO: consider persisting this table to a Parquet file
        # We can a registry of cached tables that are identified by query parameters.
        # Can we use a hash of the stringified version of QueryModel as pertains to this dataset?
        return ld_df

    @track_timing(timer_stats_collector)
    def get_dataframe(self, query_context: QueryContext):
        # TODO: handle pre-filtering of load data.
        #   - time could be filtered
        #   - columns could be dropped or aggregated
        ld_df = self._load_data
        lk_df = self._filter_project_dimensions(self._load_data_lookup, query_context)
        return self.make_project_dataframe(
            load_data_df=ld_df, lookup_df=lk_df, query_context=query_context
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
                f"load_data_lookup is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
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
        dimension_records = set(self.get_pivot_dimension_columns())
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())

        found_id = False
        pivot_cols = set()
        for col in self._load_data.columns:
            if col == "id":
                found_id = True
                continue
            if col in time_columns:
                continue
            if col in dimension_records:
                pivot_cols.add(col)
            else:
                raise DSGInvalidDataset(f"column={col} is not expected in load_data.")

        if not found_id:
            raise DSGInvalidDataset("load_data does not include an 'id' column")

        if dimension_records != pivot_cols:
            missing = dimension_records.difference(pivot_cols)
            raise DSGInvalidDataset(
                f"load_data is missing {missing} columns for dimension={dim_type.value} based on records."
            )
