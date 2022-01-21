from dsgrid.config.dataset_config import (
    DatasetConfig,
    check_load_data_filename,
    check_load_data_lookup_filename,
)
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.utils.timing import timer_stats_collector, Timer


class OneTableDataSchemaHandler(DatasetSchemaHandlerBase):
    """ define interface/required behaviors for ONETABLE dataset schema """

    def __init__(self, config):
        self._config = config

    def check_consistency(self):
        load_data_df = read_dataframe(check_load_data_filename(path), cache=True)
        load_data_df = config.add_trivial_dimensions(load_data_df)
        with Timer(timer_stats_collector, "check_one_table_data_consistency"):
            self._check_one_table_data_consistency(config, load_data_df)
        with Timer(timer_stats_collector, "check_dataset_time_consistency"):
            self._check_dataset_time_consistency(config, load_data_df)

    def _check_one_table_data_consistency(self, config: DatasetConfig, load_data):
        dimension_types = []
        metric_cols = []

        time_dim = config.get_dimension(DimensionType.TIME)
        time_columns = set(time_dim.get_timestamp_load_data_columns())

        for col in load_data.columns:
            if col in time_columns:
                continue
            try:
                dimension_types.append(DimensionType(col))
            except ValueError:
                metric_cols.append(col)

        dimension_types.append(DimensionType.METRIC)  # add metric type

        # check dim outside of metric and time
        expected_dimensions = set([d for d in DimensionType if d != DimensionType.TIME])
        if len(dimension_types) != len(expected_dimensions):
            raise DSGInvalidDataset(
                f"load_data does not have the correct number of dimensions specified between trivial and non-trivial dimensions."
            )

        missing_dimensions = expected_dimensions.difference(dimension_types)
        if len(missing_dimensions) != 0:
            raise DSGInvalidDataset(
                f"load_data is missing dimensions: {missing_dimensions}. If these are trivial dimensions, make sure to specify them in the Dataset Config."
            )

        dimension_types = [
            d for d in dimension_types if d != DimensionType.TIME
        ]  # remove time type
        for dimension_type in dimension_types:
            name = dimension_type.value
            dimension = config.get_dimension(dimension_type)
            dim_records = dimension.get_unique_ids()
            if dimension_type == DimensionType.METRIC:
                data_records = set(metric_cols)
            else:
                data_records = get_unique_values(load_data, name)
            if dim_records != data_records:
                logger.error(
                    "Mismatch in load_data records. dimension=%s mismatched=%s",
                    name,
                    data_records.symmetric_difference(dim_records),
                )
                raise DSGInvalidDataset(
                    f"load_data records do not match dimension records for {name}"
                )

    def _check_dataset_time_consistency(self, config: DatasetConfig, load_data_df):
        time_dim = config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
