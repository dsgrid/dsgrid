import logging

import ibis.expr.types as ir

from dsgrid.common import VALUE_COLUMN
from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    ColumnType,
    DatasetDimensionsMetadataModel,
)
from dsgrid.query.query_context import QueryContext
from dsgrid.units.convert import convert_units_unpivoted
from dsgrid.dataset.table_format_handler_base import TableFormatHandlerBase


logger = logging.getLogger(__name__)


class UnpivotedTableHandler(TableFormatHandlerBase):
    """Implements behavior for tables stored in unpivoted format."""

    def process_aggregations(
        self, df: ir.Table, aggregations: list[AggregationModel], context: QueryContext
    ) -> ir.Table:
        orig_id = id(df)
        df = self.process_stacked_aggregations(df, aggregations, context)
        df = self._remove_invalid_null_timestamps(df, orig_id, context)
        return df

    def process_stacked_aggregations(
        self, df: ir.Table, aggregations: list[AggregationModel], context: QueryContext
    ) -> ir.Table:
        """Aggregate the stacked dimensional data as specified by aggregations.

        Parameters
        ----------
        df : ibis.expr.types.Table
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        ibis.expr.types.Table

        """
        table = df
        if not aggregations:
            return table

        final_metadata = DatasetDimensionsMetadataModel()
        dim_type_to_base_query_name = self.project_config.get_dimension_type_to_base_name_mapping()
        column_to_dim_type: dict[str, DimensionType] = {}
        # Initialize current_metric_name based on what's in the table
        current_metric_name = None
        for col in table.columns:
            try:
                dim = self.project_config.get_dimension(col)
                if dim.model.dimension_type == DimensionType.METRIC:
                    current_metric_name = col
                    break
            except Exception:
                continue
        logger.debug(f"Initialized current_metric_name to {current_metric_name}")

        dropped_dimensions = set()
        for agg in aggregations:
            metric_query_name = None
            columns: list[ColumnModel] = []
            for dim_type, column in agg.iter_dimensions_to_keep():
                assert dim_type not in dropped_dimensions, dim_type
                columns.append(column)
                self._add_column_to_dim_type(column, dim_type, column_to_dim_type)
                if dim_type == DimensionType.METRIC:
                    metric_query_name = column.dimension_name

            if metric_query_name is None:
                msg = f"Bug: A metric dimension is not included in {agg}"
                raise Exception(msg)

            dropped_dimensions.update(set(agg.list_dropped_dimensions()))
            if not columns:
                continue

            table = self.add_columns(table, columns, context, [VALUE_COLUMN])
            group_by_cols = self._build_group_by_columns(table, columns, context, final_metadata)
            op = agg.aggregation_function
            table = table.group_by(group_by_cols).aggregate(
                getattr(table[VALUE_COLUMN], op)().name(VALUE_COLUMN)
            )
            try:
                c = table.count().to_pyarrow().as_py()
                print(f"DEBUG: After aggregation {agg.aggregation_function}: {c} rows")
            except Exception as e:
                print(f"DEBUG: count failed after agg: {e}")

            if (
                metric_query_name not in dim_type_to_base_query_name[DimensionType.METRIC]
                and metric_query_name != current_metric_name
            ):
                to_dim = self.project_config.get_dimension_with_records(metric_query_name)
                assert context.base_dimension_names.metric is not None
                mapping = self.project_config.get_base_to_supplemental_config(
                    self.project_config.get_dimension_with_records(
                        context.base_dimension_names.metric
                    ),
                    to_dim,
                )
                from_dim_id = mapping.model.from_dimension.dimension_id
                from_records = self.project_config.get_base_dimension_records_by_id(from_dim_id)
                mapping_records = mapping.get_records_dataframe()
                to_unit_records = to_dim.get_records_dataframe()
                table = convert_units_unpivoted(
                    table,
                    _get_metric_column_name(context, metric_query_name),
                    from_records,
                    mapping_records,
                    to_unit_records,
                )
                current_metric_name = metric_query_name

            if "06037" in str(
                table.to_pyarrow().to_pylist()
            ):  # extensive debug for specific county
                try:
                    debug_filter = (table["county"] == "06037") & (
                        table[metric_query_name] == "electricity_end_uses"
                    )
                    filtered_table = table.filter(debug_filter)
                    debug_count = filtered_table.count().to_pyarrow().as_py()
                    print(f"DEBUG: Pre-agg count for 06037/electricity: {debug_count}")
                    if debug_count > 0:
                        total_val = (
                            filtered_table.aggregate(val=table["value"].sum())
                            .to_pyarrow()
                            .to_pylist()[0]["val"]
                        )
                        print(f"DEBUG: Total value for 06037/electricity: {total_val}")
                except Exception as e:
                    print(f"DEBUG: Failed specific debug: {e}")

            print(f"DEBUG: metric_query_name={metric_query_name}, current={current_metric_name}")

            logger.debug(
                "Aggregated dimensions with groupBy %s and operation %s",
                group_by_cols,
                agg.aggregation_function,
            )

        for dim_type in DimensionType:
            metadata = final_metadata.get_metadata(dim_type)
            if dim_type in dropped_dimensions and metadata:
                metadata.clear()
            context.replace_dimension_metadata(dim_type, metadata, dataset_id=self.dataset_id)
        return table


def _get_metric_column_name(context: QueryContext, metric_query_name):
    match context.model.result.column_type:
        case ColumnType.DIMENSION_TYPES:
            metric_column = DimensionType.METRIC.value
        case ColumnType.DIMENSION_NAMES:
            metric_column = metric_query_name
        case _:
            msg = f"Bug: unhandled: {context.model.result.column_type}"
            raise NotImplementedError(msg)
    return metric_column
