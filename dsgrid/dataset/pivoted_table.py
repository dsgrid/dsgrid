import copy
import logging
from typing import List

import pyspark.sql.functions as F

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.query.models import AggregationModel
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.dataset import map_and_reduce_pivoted_dimension, remove_invalid_null_timestamps
from dsgrid.utils.spark import get_unique_values
from .table_format_handler_base import TableFormatHandlerBase


logger = logging.getLogger(__name__)


class PivotedTableHandler(TableFormatHandlerBase):
    """Implements behavior for tables stored in pivoted format."""

    def __init__(self, project_config, dataset_id=None):
        super().__init__(project_config, dataset_id=dataset_id)

    def add_columns(self, df, query_columns, context: QueryContext, aggregation_allowed):
        columns = set(df.columns)
        base_query_names = self.project_config.get_base_dimension_query_names()
        for column in query_columns:
            query_name = column.dimension_query_name
            if query_name in base_query_names:
                assert query_name in columns, f"{query_name} {columns}"
                continue
            elif query_name in columns:
                continue
            supp_dim = self._project_config.get_dimension(query_name)
            base_dim = self._project_config.get_base_dimension(supp_dim.model.dimension_type)
            base_query_name = base_dim.model.dimension_query_name
            records = self._project_config.get_base_to_supplemental_mapping_records(query_name)
            if not aggregation_allowed:
                to_ids = records.groupBy("from_id").agg(F.count("to_id").alias("count_to_id"))
                counts_of_to_id = get_unique_values(to_ids, "count_to_id")
                if counts_of_to_id != {1}:
                    raise DSGInvalidParameter(
                        f"Mapping dimension query name {query_name} produced duplicate to_ids for one or more from_ids"
                    )
            from_fractions = get_unique_values(records, "from_fraction")
            if len(from_fractions) != 1 and float(next(iter(from_fractions))) != 1.0:
                # TODO #199: This needs to apply from_fraction to each load value column.
                # Also needs to handle all possible from_id/to_id combinations
                # If aggregation is not allowed then it should raise an error.
                raise DSGInvalidParameter(
                    f"Mapping dimension query name {query_name} produced from_fractions other than 1.0: {from_fractions}"
                )
            else:
                records = records.drop("from_fraction")
                if column.function is not None:
                    # TODO #200: Do we want to allow this?
                    raise Exception(
                        f"Applying a SQL function to added column={query_name} is not supported yet"
                    )
                df = (
                    df.join(records, on=df[base_query_name] == records.from_id)
                    .drop("from_id")
                    .withColumnRenamed("to_id", query_name)
                )
                context.add_dimension_query_name(
                    supp_dim.model.dimension_type, query_name, dataset_id=self.dataset_id
                )

        return df

    def process_aggregations(
        self, df, aggregations: List[AggregationModel], context: QueryContext
    ):
        df = self.process_pivoted_aggregations(df, aggregations, context)
        orig_id = id(df)
        df = self.process_stacked_aggregations(df, aggregations, context)

        if id(df) != orig_id:
            # The table could have NULL timestamps that designate expected-missing data.
            # Those rows could be obsolete after aggregating stacked dimensions.
            # This is an expensive operation, so only do it if the dataframe changed.
            pivoted_columns = context.get_pivoted_columns()
            time_columns = context.get_dimension_query_names(DimensionType.TIME)
            if time_columns:
                stacked_columns = set(df.columns) - pivoted_columns.union(time_columns)
                df = remove_invalid_null_timestamps(df, time_columns, stacked_columns)

        return df

    def process_pivoted_aggregations(
        self, df, aggregations: List[AggregationModel], context: QueryContext
    ):
        """Aggregate the pivoted dimensional data as specified by aggregations.

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

        pivoted_columns = copy.deepcopy(context.get_pivoted_columns(dataset_id=self.dataset_id))
        new_pivoted_columns = []  # Will be the pivoted columns after the last aggregation.
        pivoted_dim_type = context.get_pivoted_dimension_type(dataset_id=self.dataset_id)
        base_query_names = self.project_config.get_base_dimension_query_names()
        for agg in aggregations:
            dimension_query_name = None
            for _, column in agg.iter_dimensions_to_keep():
                query_name = column.dimension_query_name
                # No work is required if the user requested the base dimension for the pivoted
                # dimension or if this pivoted column has already been handled.
                if (
                    query_name not in base_query_names
                    and query_name not in context.get_dimension_query_names(pivoted_dim_type)
                ):
                    if column.function is not None:
                        # TODO: Do we need to support this?
                        raise Exception(f"column function cannot be set on {column}")
                    dim = self.project_config.get_dimension(query_name)
                    if dim.model.dimension_type == pivoted_dim_type:
                        dimension_query_name = query_name
                        break
            if dimension_query_name is None:
                continue
            dim_config = self.project_config.get_dimension(dimension_query_name)
            mapping_records = self.project_config.get_base_to_supplemental_mapping_records(
                dimension_query_name
            )
            df, new_pivoted_columns, dropped_columns = map_and_reduce_pivoted_dimension(
                df,
                mapping_records,
                pivoted_columns,
                agg.aggregation_function.__name__,
                rename=False,
            )
            context.replace_dimension_query_names(
                dim_config.model.dimension_type, {dimension_query_name}, dataset_id=self.dataset_id
            )
            pivoted_columns -= dropped_columns
            logger.info("Replaced dimensions with supplemental records %s", new_pivoted_columns)

        if new_pivoted_columns:
            context.set_pivoted_columns(new_pivoted_columns, dataset_id=self.dataset_id)

        return df

    def process_stacked_aggregations(
        self, df, aggregations: List[AggregationModel], context: QueryContext
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

        group_by_query_names = []
        pivoted_dimension_type = context.get_pivoted_dimension_type(dataset_id=self.dataset_id)
        pivoted_columns = set(context.get_pivoted_columns(dataset_id=self.dataset_id))
        for agg in aggregations:
            columns = []
            for dim_type, column in agg.iter_dimensions_to_keep():
                if dim_type != pivoted_dimension_type:
                    columns.append(column)
            if not columns:
                continue
            df = self.add_columns(df, columns, context, True)
            group_by_cols = []
            for column in columns:
                if column.function is None:
                    expr = column.dimension_query_name
                else:
                    expr = column.function(column.dimension_query_name)
                    if column.alias is not None:
                        expr = expr.alias(column.alias)
                group_by_cols.append(expr)
                group_by_query_names.append(column.dimension_query_name)
            op = agg.aggregation_function
            agg_expr = [op(x).alias(x) for x in df.columns if x in pivoted_columns]
            df = df.groupBy(*group_by_cols).agg(*agg_expr)
            logger.info(
                "Aggregated dimensions with groupBy %s and agg %s", group_by_cols, agg_expr
            )

        final_query_names = {x: set() for x in DimensionType if x != pivoted_dimension_type}
        for column in group_by_query_names:
            dim = self.project_config.get_dimension(column)
            final_query_names[dim.model.dimension_type].add(column)

        for dim_type, columns in final_query_names.items():
            context.replace_dimension_query_names(dim_type, columns, dataset_id=self.dataset_id)
        return df
