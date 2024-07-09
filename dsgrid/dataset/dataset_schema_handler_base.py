import abc
import logging
import os
from typing import Optional, Union

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from dsgrid.config.annual_time_dimension_config import (
    AnnualTimeDimensionConfig,
    map_annual_historical_time_to_date_time,
)
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig

import dsgrid.units.energy as energy
from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import DatasetConfig, InputDatasetType
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingReferenceModel,
)
from dsgrid.config.simple_models import DatasetSimpleModel
from dsgrid.dataset.models import TableFormatType
from dsgrid.dataset.table_format_handler_factory import make_table_format_handler
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.dimension.time import (
    TimeDimensionType,
    DaylightSavingAdjustmentModel,
)
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import ColumnType
from dsgrid.utils.dataset import (
    check_historical_annual_time_model_year_consistency,
    is_noop_mapping,
    map_and_reduce_stacked_dimension,
    add_time_zone,
    ordered_subset_columns,
    repartition_if_needed_by_mapping,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


class DatasetSchemaHandlerBase(abc.ABC):
    """define interface/required behaviors per dataset schema"""

    def __init__(
        self,
        config,
        dimension_mgr,
        dimension_mapping_mgr,
        mapping_references: Union[None, list[DimensionMappingReferenceModel]] = None,
        project_time_dim=None,
    ):
        self._config: DatasetConfig = config
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr
        self._mapping_references: list[DimensionMappingReferenceModel] = mapping_references or []
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
    def make_dimension_association_table(self) -> DataFrame:
        """Return a dataframe containing one row for each unique dimension combination except time."""

    @abc.abstractmethod
    def filter_data(self, dimensions: list[DatasetSimpleModel]):
        """Filter the load data by dimensions and rewrite the files.

        dimensions : list[DimensionSimpleModel]
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

    @abc.abstractmethod
    def make_project_dataframe(self, project_config, scratch_dir_context: ScratchDirContext):
        """Return a load_data dataframe with dimensions mapped to the project's.

        Parameters
        ----------
        project_config: ProjectConfig
        scratch_dir_context: ScratchDirContext

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
        time_cols = self._get_time_dimension_columns()
        time_dim.check_dataset_time_consistency(load_data_df, time_cols)
        if time_dim.model.time_type != TimeDimensionType.NOOP:
            self._check_dataset_time_consistency_by_time_array(time_cols, load_data_df)
        self._check_model_year_time_consistency(load_data_df)

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
        for col in time_cols:
            load_data_df = load_data_df.filter(f"{col} is not null")
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

    def _check_load_data_unpivoted_value_column(self, df):
        logger.info("Check load data unpivoted columns.")
        if VALUE_COLUMN not in df.columns:
            raise DSGInvalidDataset(f"value_column={VALUE_COLUMN} is not in columns={df.columns}")

    def _convert_units(self, df, project_metric_records, value_columns):
        if not self._config.model.enable_unit_conversion:
            return df

        # Note that a dataset could have the same dimension record IDs as the project,
        # no mappings, but then still have different units.
        mapping_records = None
        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            if dim_type == DimensionType.METRIC:
                mapping_records = self._dimension_mapping_mgr.get_by_id(
                    ref.mapping_id, version=ref.version
                ).get_records_dataframe()
                break

        dataset_records = self._config.get_dimension(DimensionType.METRIC).get_records_dataframe()
        return energy.convert_units_unpivoted(
            df,
            DimensionType.METRIC.value,
            dataset_records,
            mapping_records,
            project_metric_records,
        )

    def _finalize_table(self, context: QueryContext, df, project_config):
        table_handler = make_table_format_handler(
            self._config.get_table_format_type(),
            project_config,
            dataset_id=self.dataset_id,
        )

        if context.model.result.column_type == ColumnType.DIMENSION_QUERY_NAMES:
            df = table_handler.convert_columns_to_query_names(df)

        context.set_dataset_metadata(
            self.dataset_id,
            context.model.result.column_type,
            project_config,
        )

        return df

    @staticmethod
    def _get_pivoted_column_name(
        context: QueryContext, pivoted_dimension_type: DimensionType, project_config
    ):
        match context.model.result.column_type:
            case ColumnType.DIMENSION_QUERY_NAMES:
                pivoted_column_name = project_config.get_base_dimension(
                    pivoted_dimension_type
                ).model.dimension_query_name
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
        return self._dimension_mapping_mgr.get_by_id(ref.mapping_id, version=ref.version)

    def _get_dataset_to_project_mapping_reference(self, dimension_type: DimensionType):
        for ref in self._mapping_references:
            if ref.from_dimension_type == dimension_type:
                return ref
        return

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
                    dataset_mapping.withColumnRenamed("from_id", "dataset_record_id")
                    .join(
                        project_record_ids,
                        on=dataset_mapping.to_id == project_record_ids.id,
                    )
                    .selectExpr("dataset_record_id AS id")
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
            df = df.join(tmp, on=df[dim_type.value] == tmp.dataset_record_id).drop(
                "dataset_record_id"
            )

        return df

    def _prefilter_time_dimension(self, context: QueryContext, df):
        # TODO #196:
        return df

    def _remap_dimension_columns(
        self,
        df: DataFrame,
        filtered_records: None | dict = None,
        handle_data_skew=False,
        scratch_dir_context: Optional[ScratchDirContext] = None,
    ) -> DataFrame:
        if handle_data_skew and scratch_dir_context is None:
            msg = "Bug: conflicting inputs: handle_data_skew requires a scratch_dir_context"
            raise Exception(msg)

        for ref in self._mapping_references:
            dim_type = ref.from_dimension_type
            column = dim_type.value
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                ref.mapping_id, version=ref.version
            )
            logger.info(
                "Mapping dimension type %s mapping_type=%s",
                dim_type,
                mapping_config.model.mapping_type,
            )
            records = mapping_config.get_records_dataframe()
            if filtered_records is not None and dim_type in filtered_records:
                records = records.join(
                    filtered_records[dim_type], on=records.to_id == filtered_records[dim_type].id
                ).drop("id")

            if is_noop_mapping(records):
                logger.info("Skip no-op mapping %s.", ref.mapping_id)
                continue
            if column in df.columns:
                df = map_and_reduce_stacked_dimension(df, records, column)
                if handle_data_skew:
                    assert scratch_dir_context is not None
                    df = repartition_if_needed_by_mapping(
                        df,
                        mapping_config.model.mapping_type,
                        scratch_dir_context,
                    )

        return df

    def _apply_fraction(self, df, value_columns, agg_func=None):
        if "fraction" not in df.columns:
            return df
        agg_func = agg_func or F.sum
        # Maintain column order.
        agg_ops = [
            agg_func(F.col(x) * F.col("fraction")).alias(x)
            for x in [y for y in df.columns if y in value_columns]
        ]
        gcols = set(df.columns) - value_columns - {"fraction"}
        df = df.groupBy(*ordered_subset_columns(df, gcols)).agg(*agg_ops)
        return df.drop("fraction")

    @track_timing(timer_stats_collector)
    def _convert_time_dimension(
        self,
        load_data_df,
        project_config,
        value_columns: set[str],
        scratch_dir_context: ScratchDirContext,
    ):
        input_dataset_model = project_config.get_dataset(self._config.model.dataset_id)
        wrap_time_allowed = input_dataset_model.wrap_time_allowed
        time_based_data_adjustment = input_dataset_model.time_based_data_adjustment
        self._validate_daylight_saving_adjustment(time_based_data_adjustment)
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.model.is_time_zone_required_in_geography():
            if self._config.model.use_project_geography_time_zone:
                geography_dim = project_config.get_base_dimension(DimensionType.GEOGRAPHY)
            else:
                geography_dim = self._config.get_dimension(DimensionType.GEOGRAPHY)
            load_data_df = add_time_zone(load_data_df, geography_dim)

        if isinstance(time_dim, AnnualTimeDimensionConfig):
            project_time_dim = project_config.get_base_dimension(DimensionType.TIME)
            if not isinstance(project_time_dim, DateTimeDimensionConfig):
                msg = f"Annual time can only be mapped to DateTime: {project_time_dim.model.time_type}"
                raise NotImplementedError(msg)
            if self._config.model.dataset_type == InputDatasetType.HISTORICAL:
                return map_annual_historical_time_to_date_time(
                    load_data_df,
                    time_dim,
                    project_time_dim,
                    value_columns,
                )
            msg = f"Cannot map AnnualTime for a dataset of type {self._config.model.dataset_type}"
            raise NotImplementedError(msg)

        load_data_df = time_dim.convert_dataframe(
            load_data_df,
            self._project_time_dim,
            value_columns,
            scratch_dir_context,
            wrap_time_allowed=wrap_time_allowed,
            time_based_data_adjustment=time_based_data_adjustment,
        )

        if time_dim.model.is_time_zone_required_in_geography():
            load_data_df = load_data_df.drop("time_zone")

        return load_data_df

    def _validate_daylight_saving_adjustment(self, time_based_data_adjustment):
        if (
            time_based_data_adjustment.daylight_saving_adjustment
            == DaylightSavingAdjustmentModel()
        ):
            return
        time_dim = self._config.get_dimension(DimensionType.TIME)
        if time_dim.model.time_type != TimeDimensionType.INDEX:
            msg = f"time_based_data_adjustment.daylight_saving_adjustment does not apply to {time_dim.time_dim.model.time_type=} time type, it applies to INDEX time type only."
            logger.warning(msg)

    def _remove_non_dimension_columns(self, df: DataFrame) -> DataFrame:
        allowed_columns = self._list_dimension_columns(df)
        return df.select(*allowed_columns)
