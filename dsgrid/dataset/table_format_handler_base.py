import abc
import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import (
    AggregationModel,
    ColumnType,
    DimensionMetadataModel,
)
from dsgrid.utils.spark import get_unique_values
from dsgrid.utils.dataset import remove_invalid_null_timestamps


logger = logging.getLogger(__name__)


class TableFormatHandlerBase(abc.ABC):
    """Base class for table format handers"""

    def __init__(self, project_config: ProjectConfig, dataset_id: str | None = None):
        self._project_config = project_config
        self._dataset_id = dataset_id

    def add_columns(
        self,
        df: DataFrame,
        dimension_query_names: list[str],
        context: QueryContext,
        aggregation_allowed: bool,
    ) -> DataFrame:
        """Add columns to the dataframe.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        dimension_query_names : list
        context : QueryContext
        aggregation_allowed : bool
            Set to False if adding a column is not allowed to change the load values in df.
        """
        columns = set(df.columns)
        dim_type_to_query_name = self.project_config.get_base_dimension_to_query_name_mapping()
        base_query_names = set(dim_type_to_query_name.values())
        for column in dimension_query_names:
            query_name = column.dimension_query_name
            dim = self._project_config.get_dimension(query_name)
            expected_base_dim_cols = context.get_dimension_column_names_by_query_name(
                dim.model.dimension_type, dim_type_to_query_name[dim.model.dimension_type]
            )
            if query_name in base_query_names:
                assert columns.issuperset(
                    expected_base_dim_cols
                ), f"{columns=} {expected_base_dim_cols=}"
                continue
            elif query_name in columns:
                continue
            if dim.model.dimension_type == DimensionType.TIME:
                raise NotImplementedError(
                    "Adding time columns through supplemental mappings is not supported yet."
                )
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
            records = records.drop("from_fraction")
            if column.function is not None:
                # TODO #200: Do we want to allow this?
                raise NotImplementedError(
                    f"Applying a SQL function to added column={query_name} is not supported yet"
                )
            if len(expected_base_dim_cols) > 1:
                raise Exception(
                    "Bug: Non-time dimensions cannot have more than one base dimension column"
                )
            expected_base_dim_col = expected_base_dim_cols[0]
            df = (
                df.join(records, on=df[expected_base_dim_col] == records.from_id)
                .drop("from_id")
                .withColumnRenamed("to_id", query_name)
            )
            if context.model.result.column_type == ColumnType.DIMENSION_QUERY_NAMES:
                if dim.model.dimension_type == DimensionType.TIME:
                    column_names = dim.list_load_data_columns_for_query_name()
                else:
                    column_names = [query_name]
            else:
                column_names = [expected_base_dim_col]
            context.add_dimension_metadata(
                dim.model.dimension_type,
                DimensionMetadataModel(dimension_query_name=query_name, column_names=column_names),
                dataset_id=self.dataset_id,
            )

        return df

    @abc.abstractmethod
    def process_aggregations(self, df, aggregations: AggregationModel, context: QueryContext):
        """Aggregate the dimensional data as specified by aggregations.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @property
    def project_config(self) -> ProjectConfig:
        """Return the project config of the dataset being processed."""
        return self._project_config

    @property
    def dataset_id(self) -> str:
        """Return the ID of the dataset being processed."""
        return self._dataset_id

    def convert_columns_to_query_names(self, df: DataFrame) -> DataFrame:
        """Convert columns from dimension types to dimension query names."""
        base_to_query_name_mapping = self.project_config.get_base_dimension_to_query_name_mapping()
        columns = set(df.columns)
        for dim_type in DimensionType:
            if dim_type == DimensionType.TIME:
                time_dim = self._project_config.get_base_dimension(dim_type)
                df = time_dim.map_timestamp_load_data_columns_for_query_name(df)
            elif dim_type.value in columns:
                existing_col = dim_type.value
                new_col = base_to_query_name_mapping[dim_type]
                if existing_col != new_col:
                    df = df.withColumnRenamed(existing_col, new_col)
                    logger.debug("Converted column from %s to %s", existing_col, new_col)

        return df

    def replace_ids_with_names(self, df: DataFrame) -> DataFrame:
        """Replace dimension record IDs with names."""
        # TODO DT: This doesn't change pivoted column names. Is that OK?
        orig = df
        all_query_names = set(self._project_config.list_dimension_query_names())
        for dimension_query_name in set(df.columns).intersection(all_query_names):
            assert not {"id", "name"}.intersection(df.columns), df.columns
            dim_config = self._project_config.get_dimension(dimension_query_name)
            if dim_config.model.dimension_type == DimensionType.TIME:
                # Time doesn't have records.
                continue
            records = dim_config.get_records_dataframe().select("id", "name")
            df = (
                df.join(records, on=df[dimension_query_name] == records.id)
                .drop("id", dimension_query_name)
                .withColumnRenamed("name", dimension_query_name)
            )
        assert df.count() == orig.count(), f"counts changed {df.count()} {orig.count()}"
        return df

    @staticmethod
    def _add_column_to_dim_type(column, dim_type, column_to_dim_type):
        name = column.get_column_name()
        if name in column_to_dim_type:
            assert dim_type == column_to_dim_type[name], f"{name=} {column_to_dim_type}"
        column_to_dim_type[name] = dim_type

    def _build_group_by_columns(
        self, columns, context, column_to_dim_type, dim_type_to_query_name, final_metadata
    ):
        group_by_cols = []
        for column in columns:
            dim_type = column_to_dim_type[column.get_column_name()]
            match context.model.result.column_type:
                case ColumnType.DIMENSION_TYPES:
                    column_names = context.get_dimension_column_names_by_query_name(
                        dim_type, dim_type_to_query_name[dim_type]
                    )
                    if dim_type == DimensionType.TIME:
                        group_by_cols += column_names
                    else:
                        group_by_cols.append(dim_type.value)
                case ColumnType.DIMENSION_QUERY_NAMES:
                    column_names = [column.get_column_name()]
                    expr = self._make_group_by_column_expr(column)
                    group_by_cols.append(expr)
                    if not isinstance(expr, str) or expr != column.dimension_query_name:
                        # In this case we are replacing any existing query name with an expression
                        # or alias, and so the old name must be removed.
                        final_metadata.remove_metadata(dim_type, column.dimension_query_name)
                case _:
                    raise NotImplementedError(
                        f"Bug: unhandled: {context.model.result.column_type}"
                    )
            final_metadata.add_metadata(
                dim_type,
                DimensionMetadataModel(
                    dimension_query_name=column.dimension_query_name, column_names=column_names
                ),
            )
        return group_by_cols

    @staticmethod
    def _make_group_by_column_expr(column):
        if column.function is None:
            expr = column.dimension_query_name
        else:
            expr = column.function(column.dimension_query_name)
            if column.alias is not None:
                expr = expr.alias(column.alias)
        return expr

    @staticmethod
    def _remove_invalid_null_timestamps(df: DataFrame, orig_id, context: QueryContext):
        if id(df) != orig_id:
            # The table could have NULL timestamps that designate expected-missing data.
            # Those rows could be obsolete after aggregating stacked dimensions.
            # This is an expensive operation, so only do it if the dataframe changed.
            pivoted_columns = context.get_pivoted_columns()
            time_columns = context.get_dimension_column_names(DimensionType.TIME)
            if time_columns:
                stacked_columns = set(df.columns) - pivoted_columns.union(time_columns)
                df = remove_invalid_null_timestamps(df, time_columns, stacked_columns)

        return df
