import logging

import ibis

from dsgrid.common import VALUE_COLUMN
from dsgrid.dimension.base_models import DimensionType
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.operations import create_temp_view, handle_column_spaces
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    ColumnType,
    DatasetDimensionsMetadataModel,
)
from dsgrid.query.query_context import QueryContext

from dsgrid.units.convert import convert_units_unpivoted
from dsgrid.dataset.table_format_handler_base import TableFormatHandlerBase
from dsgrid.ibis.session import get_runtime_session


logger = logging.getLogger(__name__)


class UnpivotedTableHandler(TableFormatHandlerBase):
    """Implements behavior for tables stored in unpivoted format."""

    def process_aggregations(
        self, df: ibis.Table, aggregations: list[AggregationModel], context: QueryContext
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
        df : ibis.Table
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        ibis.Table

        """
        if not aggregations:
            return df

        final_metadata = DatasetDimensionsMetadataModel()
        dim_type_to_base_query_name = self.project_config.get_dimension_type_to_base_name_mapping()
        column_to_dim_type: dict[str, DimensionType] = {}
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

            df = self.add_columns(df, columns, context, [VALUE_COLUMN])
            group_by_cols = self._build_group_by_columns(columns, context, final_metadata)
            op = agg.aggregation_function
            df = _aggregate_value(df, group_by_cols, op.name)

            if metric_query_name not in dim_type_to_base_query_name[DimensionType.METRIC]:
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
                df = convert_units_unpivoted(
                    df,
                    _get_metric_column_name(context, metric_query_name),
                    from_records,
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
        case ColumnType.DIMENSION_NAMES:
            metric_column = metric_query_name
        case _:
            msg = f"Bug: unhandled: {context.model.result.column_type}"
            raise NotImplementedError(msg)
    return metric_column


def _aggregate_value(df: ibis.Table, group_by_cols: list[str], op_name: str) -> ibis.Table:
    # Fast path: when all group-by entries are bare column references, keep
    # the chain in native Ibis. The SQL-string branch previously below was
    # called per-aggregation inside process_stacked_aggregations, and each
    # call paid the cost of registering a temp view in the backend catalog;
    # using df.group_by/aggregate keeps the lazy expression tree intact and
    # lets the planner fuse adjacent aggregations across iterations.
    bare_cols = [
        col for col in group_by_cols if _looks_like_bare_column(col, df.columns)
    ]
    if len(bare_cols) == len(group_by_cols):
        ibis_op = "mean" if op_name == "mean" else op_name
        try:
            agg_method = getattr(df[VALUE_COLUMN], ibis_op)
        except AttributeError:
            # Fall through to the SQL-string path below for ops Ibis doesn't
            # expose as a column method (rare; current callers use sum/mean/
            # min/max).
            pass
        else:
            return df.group_by(bare_cols).aggregate(**{VALUE_COLUMN: agg_method()})

    # SQL-string fallback for group-by entries that carry function calls or
    # aliases (e.g. ``year(timestamp) AS year``); these would require parsing
    # the SQL fragment back into Ibis exprs, which the SQL round-trip avoids.
    view = create_temp_view(df)
    select_cols = ", ".join(_select_expr(x) for x in group_by_cols)
    group_cols = ", ".join(_group_by_expr(x) for x in group_by_cols)
    agg_func = "AVG" if op_name == "mean" else op_name.upper()
    query = (
        f"SELECT {select_cols}, {agg_func}({handle_column_spaces(VALUE_COLUMN)}) "
        f"AS {handle_column_spaces(VALUE_COLUMN)} FROM {view}"
    )
    if group_cols:
        query += f" GROUP BY {group_cols}"
    if isinstance(df, ibis.Table):
        return get_runtime_backend().sql(query)
    return get_runtime_session().sql(query)


def _looks_like_bare_column(expr: str, columns: list[str]) -> bool:
    """True if ``expr`` is a plain column reference (no function, alias, or
    quoting) that exists in the table's columns."""
    if "(" in expr or " AS " in expr:
        return False
    if (expr.startswith('"') and expr.endswith('"')) or (
        expr.startswith("`") and expr.endswith("`")
    ):
        return False
    return expr in columns


def _group_by_expr(select_expr: str) -> str:
    marker = " AS "
    if marker in select_expr:
        return select_expr.rsplit(marker, 1)[1]
    return _select_expr(select_expr)


def _select_expr(expr: str) -> str:
    if "(" in expr or " AS " in expr:
        return expr
    if (expr.startswith('"') and expr.endswith('"')) or (
        expr.startswith("`") and expr.endswith("`")
    ):
        return expr
    return handle_column_spaces(expr)
