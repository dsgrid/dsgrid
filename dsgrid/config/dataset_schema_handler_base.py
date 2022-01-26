import abc
import collections
import logging

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """ define interface/required behaviors per dataset schema """

    @abc.abstractmethod
    def check_consistency(self):
        """
        Check all data consistencies, including data columns, dataset to dimension records, and time
        """

    def get_pivot_dimension_columns(self):
        """Get cols for the dimension that is pivoted in load_data.

        Returns
        -------
        List: List of column names.

        """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return sorted(list(self._config.get_dimension(dim_type).get_unique_ids()))

    def get_columns_for_unique_arrays(self, time_dim, load_data_df):
        """Returns the list of dimension columns aginst which the number of timestamps is checked.

        Returns
        -------
        List: List of column names.

        """
        time_col = time_dim.get_timestamp_load_data_columns()
        pivot_cols = self.get_pivot_dimension_columns()
        groupby_cols = list(set(load_data_df.columns).difference(set(pivot_cols + time_col)))

        return groupby_cols

    def _check_dataset_time_consistency(self, config: DatasetConfig, load_data_df):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        """
        time_dim = config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
        self._check_dataset_time_consistency_by_dimensions(time_dim, load_data_df)

    def _check_dataset_time_consistency_by_dimensions(self, time_dim, load_data_df):
        """
        Check dataset time consistency such that all dimension combinations return the same set of time ranges.
        """
        time_ranges = time_dim.get_time_ranges()
        assert len(time_ranges) == 1, len(time_ranges)
        time_range = time_ranges[0]
        # TODO: need to support validation of multiple time ranges: DSGRID-173

        expected_timestamps = time_range.list_time_range()
        expected_count = len(expected_timestamps)

        groupby_cols = self.get_columns_for_unique_arrays(time_dim, load_data_df)
        time_col = time_dim.get_timestamp_load_data_columns()
        timestamps_by_dims = (
            load_data_df[time_col + groupby_cols]
            .groupby(groupby_cols)
            .agg(F.countDistinct(*time_col).alias("distinct_timestamps"))
        )

        distinct_counts = timestamps_by_dims.select("distinct_timestamps").distinct()
        if distinct_counts.count() != 1:
            for row in timestamps_by_dims.collect():
                if row.distinct_timestamps != len(expected_timestamps):
                    logger.error(
                        "load_data row with dimensions=%s does not have %s %s: actual=%s",
                        self._show_selected_keys_from_dict(row, groupby_cols),
                        len(expected_timestamps),
                        time_col[0],
                        row.distinct_timestamps,
                    )

            raise DSGInvalidDataset(
                f"One or more arrays do not have {len(expected_timestamps)} timestamps"
            )

        val = distinct_counts.collect()[0].distinct_timestamps
        if val != expected_count:
            raise DSGInvalidDataset(
                f"load_data arrays do not have {len(expected_timestamps)} "
                "timestamps: actual={row.distinct_timestamps}"
            )

    @staticmethod
    def _show_selected_keys_from_dict(dct, keys: list):
        """Return a subset of a dictionary based on a list of keys.

        Returns
        -------
        Dict

        """
        dct_selected = []
        for key in keys:
            dct_selected.append(dct[key])
        return dict(zip(keys, dct_selected))

    @staticmethod
    def _check_for_duplicates_in_cols(lst: list, table_name: str):
        """Check list for duplicates

        Returns
        -------
        Set: Input list in set form if no duplicates found.

        """
        dups = [x for x, n in collections.Counter(lst).items() if n > 1]
        if len(dups) > 0:
            raise DSGInvalidDataset(f"{table_name} contains duplicated column name(s)={dups}.")

        return set(lst)
