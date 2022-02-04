import abc
import collections
import logging
import re
import os
from typing import List

import pyspark.sql.functions as F

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.spark import models_to_dataframe
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """ define interface/required behaviors per dataset schema """

    def __init__(self, config, dimension_mgr, dimension_mapping_mgr, mapping_references=None):
        self._config = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references = mapping_references

    @classmethod
    @abc.abstractmethod
    def load(cls, config: DatasetConfig):
        """Create a dataset schema handler by loading the data tables from files.

        Parameters
        ----------
        config: DatasetConfig

        Returns
        -------
        DatasetSchemaHandlerBase

        """

    @abc.abstractmethod
    def check_consistency(self):
        """
        Check all data consistencies, including data columns, dataset to dimension records, and time
        """

    @abc.abstractmethod
    def get_unique_dimension_rows(self):
        """Return a dataframe containing unique dimension combinations that exist in the rows of
        the data table.

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @property
    def config(self):
        """Returns the DatasetConfig.

        Returns
        -------
        DatasetConfig

        """
        return self._config

    def get_pivot_dimension_type(self):
        """Return the DimensionType of the columns pivoted in the load_data table.

        Returns
        -------
        DimensionType

        """
        return self._config.model.data_schema.load_data_column_dimension

    def get_pivot_dimension_columns(self):
        """Get cols for the dimension that is pivoted in load_data.

        Returns
        -------
        List: List of column names.

        """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return sorted(list(self._config.get_dimension(dim_type).get_unique_ids()))

    def get_pivot_dimension_columns_mapped_to_project(self):
        """Get columns for the dimension that is pivoted in load_data and remap them to the
        project's record names. The returned dict will not include columns that the project does
        not care about.

        Returns
        -------
        dict
            Mapping of pivoted dimension column names to project record names.

        """
        columns = set(self.get_pivot_dimension_columns())
        dim_type = self.get_pivot_dimension_type()
        for ref in self._mapping_references:
            if ref.from_dimension_type.value == dim_type:
                mapping_config = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                )
                mapping = {
                    x.from_id: x.to_id for x in mapping_config.model.records if x.to_id is not None
                }

                diff = set(mapping.keys()).difference(columns)
                if diff:
                    raise DSGInvalidDataset(
                        f"Dataset pivoted dimension columns do not match mapping: {diff}"
                    )
                return mapping

        return {x: x for x in columns}

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

    def _check_dataset_time_consistency(self, load_data_df):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        """
        if os.environ.get("__DSGRID_SKIP_DATASET_TIME_CONSISTENCY_CHECKS__"):
            return

        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
        if time_dim.model.time_type != TimeDimensionType.NOOP:
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

    def _remap_dimension_columns(self, df):
        # This will likely become a common activity when running queries.
        # May need to cache the result. But that will likely be in Project, not here.
        # This method could be moved elsewhere.
        for ref in self._mapping_references:
            column = ref.from_dimension_type.value
            if column in df.columns:
                mapping_config = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                )
                records = models_to_dataframe(mapping_config.model.records).filter(
                    "to_id is not NULL"
                )
                df = (
                    df.join(records, df[column] == records.from_id)
                    .drop("from_id")
                    .drop(column)
                    .withColumnRenamed("to_id", column)
                )
        return df
