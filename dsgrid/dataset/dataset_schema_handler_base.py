import abc
import logging
import os
from typing import List

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.simple_models import DatasetSimpleModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.dataset import (
    map_and_reduce_stacked_dimension,
    map_and_reduce_pivoted_dimension,
    add_time_zone,
)
from dsgrid.utils.spark import get_unique_values
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """define interface/required behaviors per dataset schema"""

    def __init__(
        self,
        config,
        dimension_mgr,
        dimension_mapping_mgr,
        mapping_references=None,
        project_time_dim=None,
    ):
        self._config = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references = mapping_references
        self._project_time_dim = project_time_dim

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

    @abc.abstractmethod
    def filter_data(self, dimensions: List[DatasetSimpleModel]):
        """Filter the load data by dimensions and rewrite the files.

        dimensions : List[DimensionSimpleModel]
        """

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

    def get_pivoted_dimension_type(self):
        """Return the DimensionType of the columns pivoted in the load_data table.

        Returns
        -------
        DimensionType

        """
        return self._config.model.data_schema.load_data_column_dimension

    def get_pivoted_dimension_columns(self):
        """Get cols for the dimension that is pivoted in load_data.

        Returns
        -------
        List: List of column names.

        """
        dim_type = self._config.model.data_schema.load_data_column_dimension
        return sorted(list(self._config.get_dimension(dim_type).get_unique_ids()))

    @track_timing(timer_stats_collector)
    def get_pivoted_dimension_columns_mapped_to_project(self):
        """Get columns for the dimension that is pivoted in load_data and remap them to the
        project's record names. The returned dict will not include columns that the project does
        not care about.

        Returns
        -------
        dict
            Mapping of pivoted dimension column names to project record names.

        """
        columns = set(self.get_pivoted_dimension_columns())
        dim_type = self.get_pivoted_dimension_type()
        for ref in self._mapping_references:
            if ref.from_dimension_type == dim_type:
                mapping_config = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                )
                records = mapping_config.get_records_dataframe()
                from_ids = get_unique_values(records, "from_id")
                to_ids = get_unique_values(
                    records.select("to_id").filter("to_id IS NOT NULL"), "to_id"
                )
                diff = from_ids.symmetric_difference(columns)
                if diff:
                    raise DSGInvalidDataset(
                        f"Dimension_mapping={mapping_config.config_id} does not have the same "
                        f"record IDs as the dataset={self._config.config_id} columns: {diff}"
                    )
                return to_ids

        return columns

    @track_timing(timer_stats_collector)
    def get_columns_for_unique_arrays(self, time_dim, load_data_df):
        """Returns the list of dimension columns aginst which the number of timestamps is checked.

        Returns
        -------
        List: List of column names.

        """
        time_col = time_dim.get_timestamp_load_data_columns()
        pivoted_cols = self.get_pivoted_dimension_columns()
        groupby_cols = list(set(load_data_df.columns).difference(set(pivoted_cols + time_col)))

        return groupby_cols

    @abc.abstractmethod
    def make_project_dataframe(self, project_config):
        """Return a load_data dataframe with dimensions mapped to the project's.

        Parameters
        ----------
        project_config: ProjectConfig

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @abc.abstractmethod
    def make_project_dataframe_from_query(self, context, project_config):
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

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency(self, load_data_df):
        """Check dataset time consistency such that:
        1. time range(s) match time config record;
        2. all dimension combinations return the same set of time range(s).

        """
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TIME_CONSISTENCY__"):
            logger.warning("Skip dataset time consistency checks.")
            return

        logger.info("Check dataset time consistency.")
        time_dim = self._config.get_dimension(DimensionType.TIME)
        time_dim.check_dataset_time_consistency(load_data_df)
        if time_dim.model.time_type != TimeDimensionType.NOOP:
            self._check_dataset_time_consistency_by_time_array(time_dim, load_data_df)

    @track_timing(timer_stats_collector)
    def _check_dataset_time_consistency_by_time_array(self, time_dim, load_data_df):
        """Check that each unique time array has the same timestamps."""
        logger.info("Check dataset time consistency by time array.")
        unique_array_cols = self.get_columns_for_unique_arrays(time_dim, load_data_df)
        time_cols = time_dim.get_timestamp_load_data_columns()
        counts = load_data_df.groupBy(*time_cols).count().select("count")
        distinct_counts = counts.select("count").distinct().collect()
        if len(distinct_counts) != 1:
            raise DSGInvalidDataset(
                "All time arrays must be repeated the same number of times: "
                f"unique timestamp repeats = {len(distinct_counts)}"
            )
        ta_counts = load_data_df.groupBy(*unique_array_cols).count().select("count")
        distinct_ta_counts = ta_counts.select("count").distinct().collect()
        if len(distinct_ta_counts) != 1:
            raise DSGInvalidDataset(
                "All combinations of non-time dimensions must have the same time array length: "
                f"unique time array lengths = {len(distinct_ta_counts)}"
            )

    def _get_dataset_to_project_mapping_records(self, dimension_type: DimensionType):
        config = self._get_dataset_to_project_mapping_config(dimension_type)
        if config is None:
            return config
        return config.get_records_dataframe()

    def _get_dataset_to_project_mapping_config(self, dimension_type: DimensionType):
        ref = self._get_dataset_to_project_mapping_reference(dimension_type)
        if ref is None:
            return ref
        return self._dimension_mapping_mgr.get_by_id(ref.mapping_id, version=ref.version)

    def _get_dataset_to_project_mapping_reference(self, dimension_type: DimensionType):
        for ref in self._mapping_references:
            if ref.from_dimension_type == dimension_type:
                return ref
        return

    @track_timing(timer_stats_collector)
    def _remap_dimension_columns(self, df, pivoted_columns=None):
        if pivoted_columns is None:
            pivoted_columns = set(self.get_pivoted_dimension_columns())
        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            column = dim_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version
            )
            records = mapping_config.get_records_dataframe()
            if column in df.columns:
                df = map_and_reduce_stacked_dimension(df, records, column)
            elif (
                column == self.get_pivoted_dimension_type().value
                and not pivoted_columns.difference(df.columns)
            ):
                # The dataset might have columns unwanted by the project.
                columns_to_remove = get_unique_values(records.filter("to_id IS NULL"), "from_id")
                if columns_to_remove:
                    df = df.drop(*columns_to_remove)
                    pivoted_columns.difference_update(columns_to_remove)
                # TODO: Do we want operation to be configurable?
                operation = "sum"
                df, _, _ = map_and_reduce_pivoted_dimension(
                    df,
                    records,
                    pivoted_columns,
                    operation,
                    rename=False,
                )
            elif (
                column == self.get_pivoted_dimension_type().value
                and pivoted_columns.intersection(df.columns)
            ):
                raise Exception(
                    f"Unhandled case: column={column} pivoted_columns={pivoted_columns} "
                    f"df.columns={df.columns}"
                )
            # else nothing to do

        return df

    def _convert_time_before_project_mapping(self):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        return (
            time_dim.model.is_time_zone_required_in_geography()
            and not self._config.model.use_project_geography_time_zone
        )

    @track_timing(timer_stats_collector)
    def _convert_time_dimension(self, load_data_df, project_config):
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.model.is_time_zone_required_in_geography():
            if self._config.model.use_project_geography_time_zone:
                geography_dim = project_config.get_base_dimension(DimensionType.GEOGRAPHY)
            else:
                geography_dim = self._config.get_dimension(DimensionType.GEOGRAPHY)
            load_data_df = add_time_zone(load_data_df, geography_dim)

        load_data_df = time_dim.convert_dataframe(
            df=load_data_df,
            project_time_dim=self._project_time_dim,
        )

        if time_dim.model.is_time_zone_required_in_geography():
            load_data_df = load_data_df.drop("time_zone")
        return load_data_df
