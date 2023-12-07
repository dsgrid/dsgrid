import copy
import logging

import dsgrid.units.energy as energy
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.query.models import (
    AggregationModel,
    DatasetDimensionsMetadataModel,
    DimensionMetadataModel,
)
from dsgrid.query.query_context import QueryContext
from dsgrid.utils.dataset import map_and_reduce_pivoted_dimension
from .table_format_handler_base import TableFormatHandlerBase


logger = logging.getLogger(__name__)


class PivotedTableHandler(TableFormatHandlerBase):
    """Implements behavior for tables stored in pivoted format."""

    def __init__(self, project_config, dataset_id=None):
        super().__init__(project_config, dataset_id=dataset_id)

    def process_aggregations(
        self, df, aggregations: list[AggregationModel], context: QueryContext
    ):
        df = self.process_pivoted_aggregations(df, aggregations, context)
        orig_id = id(df)
        df = self.process_stacked_aggregations(df, aggregations, context)
        df = self._remove_invalid_null_timestamps(df, orig_id, context)
        return df

    def process_pivoted_aggregations(
        self, df, aggregations: list[AggregationModel], context: QueryContext
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
        pivoted_dim_type = context.get_pivoted_dimension_type(dataset_id=self.dataset_id)
        base_names = set(self.project_config.list_dimension_query_names(DimensionCategory.BASE))
        supp_names = set(
            self.project_config.list_dimension_query_names(DimensionCategory.SUPPLEMENTAL)
        )
        for agg in aggregations:
            dimension_query_name = None
            for _, column in agg.iter_dimensions_to_keep():
                query_name = column.dimension_query_name
                # No work is required if the user requested the base dimension for the pivoted
                # dimension or if this pivoted column has already been handled.
                if (
                    query_name not in base_names
                    and query_name not in context.get_dimension_column_names(pivoted_dim_type)
                ):
                    if column.function is not None:
                        # TODO: Do we need to support this?
                        raise NotImplementedError(f"column function cannot be set on {column}")
                    dim = self.project_config.get_dimension(query_name)
                    if dim.model.dimension_type == pivoted_dim_type:
                        dimension_query_name = query_name
                        break
            if dimension_query_name is None:
                continue
            dim_config = self.project_config.get_dimension(dimension_query_name)
            dim_type = dim_config.model.dimension_type
            if dimension_query_name not in supp_names:
                raise Exception(f"Bug: {dimension_query_name=} is not a supplemental dimension")
            mapping_records = self.project_config.get_base_to_supplemental_mapping_records(
                dim_config.model.dimension_query_name
            )
            to_unit_records = dim_config.get_records_dataframe()
            df, new_pivoted_columns, dropped_columns = map_and_reduce_pivoted_dimension(
                df,
                mapping_records,
                pivoted_columns,
                agg.aggregation_function.__name__,
                rename=False,
            )
            if dim_type != DimensionType.METRIC:
                raise NotImplementedError(
                    "Aggregation of a pivoted column that is not the metric dimension is not "
                    f"yet supported: {dim_type}"
                )
            df = energy.convert_units_pivoted(
                df,
                new_pivoted_columns,
                self._project_config.get_base_dimension(dim_type).get_records_dataframe(),
                mapping_records,
                to_unit_records,
            )

            context.replace_dimension_metadata(
                dim_type,
                [
                    DimensionMetadataModel(
                        dimension_query_name=dimension_query_name, column_names=new_pivoted_columns
                    )
                ],
                dataset_id=self.dataset_id,
            )
            pivoted_columns -= dropped_columns
            logger.info("Replaced dimensions with supplemental records %s", new_pivoted_columns)

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

        pivoted_dimension_type = context.get_pivoted_dimension_type(dataset_id=self.dataset_id)
        # TODO: This does not handle the scenario where the metric dimension is stacked and
        # needs unit conversion. Raise an exception until we have a dataset that can be used
        # for testing.
        if pivoted_dimension_type != DimensionType.METRIC:
            raise NotImplementedError(f"{pivoted_dimension_type=} is not supported yet")

        pivoted_columns = set(
            context.get_dimension_column_names(pivoted_dimension_type, dataset_id=self.dataset_id)
        )
        final_metadata = DatasetDimensionsMetadataModel()
        dim_type_to_query_name = self.project_config.get_base_dimension_to_query_name_mapping()
        column_to_dim_type = {}
        dropped_dimensions = set()
        for agg in aggregations:
            columns = []
            for dim_type, column in agg.iter_dimensions_to_keep():
                assert dim_type not in dropped_dimensions, dim_type
                if dim_type != pivoted_dimension_type:
                    columns.append(column)
                    self._add_column_to_dim_type(column, dim_type, column_to_dim_type)
            dropped_dimensions.update(set(agg.list_dropped_dimensions()))
            if not columns:
                continue

            df = self.add_columns(df, columns, context, True)
            group_by_cols = self._build_group_by_columns(
                columns, context, column_to_dim_type, dim_type_to_query_name, final_metadata
            )
            op = agg.aggregation_function
            agg_expr = [op(x).alias(x) for x in df.columns if x in pivoted_columns]
            df = df.groupBy(*group_by_cols).agg(*agg_expr)
            logger.info(
                "Aggregated dimensions with groupBy %s and agg %s", group_by_cols, agg_expr
            )

        for dim_type in DimensionType:
            metadata = final_metadata.get_metadata(dim_type)
            if dim_type in dropped_dimensions and metadata:
                metadata.clear()
            context.replace_dimension_metadata(dim_type, metadata, dataset_id=self.dataset_id)
        return df
