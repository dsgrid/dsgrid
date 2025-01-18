import logging

import dsgrid.units.energy as energy
from dsgrid.common import VALUE_COLUMN
from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import (
    AggregationModel,
    ColumnType,
    DatasetDimensionsMetadataModel,
)
from dsgrid.query.query_context import QueryContext
from dsgrid.spark.types import DataFrame
from .table_format_handler_base import TableFormatHandlerBase


logger = logging.getLogger(__name__)


class UnpivotedTableHandler(TableFormatHandlerBase):
    """Implements behavior for tables stored in unpivoted format."""

    def process_aggregations(
        self, df: DataFrame, aggregations: list[AggregationModel], context: QueryContext
    ):
        orig_id = id(df)
        df = self.process_stacked_aggregations(df, aggregations, context)
        df = self._remove_invalid_null_timestamps(df, orig_id, context)
        return df

    def process_stacked_aggregations(
        self, df, aggregations: list[AggregationModel], context: QueryContext
    ):
        """Aggregate the stacked dimensional data as specified by aggregations.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        pyspark.sql.DataFrame

        """
        if not aggregations:
            return df

        final_metadata = DatasetDimensionsMetadataModel()
        dim_type_to_query_name = self.project_config.get_base_dimension_to_query_name_mapping()
        column_to_dim_type = {}
        dropped_dimensions = set()
        for agg in aggregations:
            metric_query_name = None
            columns = []
            for dim_type, column in agg.iter_dimensions_to_keep():
                assert dim_type not in dropped_dimensions, dim_type
                columns.append(column)
                self._add_column_to_dim_type(column, dim_type, column_to_dim_type)
                if dim_type == DimensionType.METRIC:
                    metric_query_name = column.dimension_query_name

            if metric_query_name is None:
                raise Exception(f"Bug: A metric dimension is not included in {agg}")

            dropped_dimensions.update(set(agg.list_dropped_dimensions()))
            if not columns:
                continue

            df = self.add_columns(df, columns, context, [VALUE_COLUMN])
            group_by_cols = self._build_group_by_columns(
                columns, context, column_to_dim_type, dim_type_to_query_name, final_metadata
            )
            op = agg.aggregation_function
            df = df.groupBy(*group_by_cols).agg(op(VALUE_COLUMN).alias(VALUE_COLUMN))

            if metric_query_name != dim_type_to_query_name[DimensionType.METRIC]:
                dim_config = self.project_config.get_dimension(metric_query_name)
                mapping_records = self.project_config.get_base_to_supplemental_mapping_records(
                    metric_query_name
                )
                to_unit_records = dim_config.get_records_dataframe()
                df = energy.convert_units_unpivoted(
                    df,
                    _get_metric_column_name(context, metric_query_name),
                    self._project_config.get_base_dimension(
                        DimensionType.METRIC
                    ).get_records_dataframe(),
                    mapping_records,
                    to_unit_records,
                )

            logger.debug(
                "Aggregated dimensions with groupBy %s and operation %s",
                group_by_cols,
                op.__name__,
            )

        for dim_type in DimensionType:
            metadata = final_metadata.get_metadata(dim_type)
            if dim_type in dropped_dimensions and metadata:
                metadata.clear()
            context.replace_dimension_metadata(dim_type, metadata, dataset_id=self.dataset_id)
        return df


def _get_metric_column_name(context: QueryContext, metric_query_name):
    match context.model.result.column_type:
        case ColumnType.DIMENSION_TYPES:
            metric_column = DimensionType.METRIC.value
        case ColumnType.DIMENSION_QUERY_NAMES:
            metric_column = metric_query_name
        case _:
            raise NotImplementedError(f"Bug: unhandled: {context.model.result.column_type}")
    return metric_column
