import abc
import logging
from typing import Iterable

import ibis.expr.types as ir

from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base_models import DimensionCategory, DimensionType
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import (
    AggregationModel,
    ColumnModel,
    ColumnType,
    DatasetDimensionsMetadataModel,
    DimensionMetadataModel,
)
from dsgrid.utils.dataset import map_stacked_dimension, remove_invalid_null_timestamps
from dsgrid.ibis_api import persist_intermediate_query
from dsgrid.utils.timing import track_timing, timer_stats_collector
from dsgrid.common import BackendEngine
import dsgrid
import ibis
import ibis.expr.datatypes as dt


@ibis.udf.scalar.builtin
def timezone(zone: str, ts: dt.timestamp) -> dt.timestamp:
    ...


logger = logging.getLogger(__name__)


class TableFormatHandlerBase(abc.ABC):
    """Base class for table format handers"""

    def __init__(self, project_config: ProjectConfig, dataset_id: str | None = None):
        self._project_config = project_config
        self._dataset_id = dataset_id

    def add_columns(
        self,
        df: ir.Table,
        column_models: list[ColumnModel],
        context: QueryContext,
        value_columns: Iterable[str],
    ) -> ir.Table:
        """Add columns to the dataframe. For example, suppose the geography dimension is at
        county resolution and the user wants to add a column for state.

        Parameters
        ----------
        df : ibis.expr.types.Table
        column_models : list
        context : QueryContext
        value_columns: Iterable[str]
            Columns in the dataframe that contain load values.
        """
        columns = set(df.columns)
        all_base_names = self.project_config.list_dimension_names(category=DimensionCategory.BASE)
        for column in column_models:
            name = column.dimension_name
            if name in all_base_names or name in columns:
                continue
            supp_dim = self._project_config.get_dimension_with_records(name)
            existing_metadata = context.get_dimension_metadata(
                supp_dim.model.dimension_type, dataset_id=self._dataset_id
            )
            existing_base_metadata = [
                x for x in existing_metadata if x.dimension_name in all_base_names
            ]
            if len(existing_base_metadata) != 1:
                msg = (
                    f"Bug: expected one base metadata object for {supp_dim.model.dimension_type}: "
                    "{existing_base_metadata}"
                )
                raise Exception(msg)
            base_dim_name = existing_base_metadata[0].dimension_name
            if base_dim_name not in all_base_names:
                msg = f"Bug: Expected {base_dim_name} to be a base dimension."
                raise Exception(msg)
            base_dim = self._project_config.get_dimension_with_records(base_dim_name)
            records = self._project_config.get_base_to_supplemental_mapping_records(
                base_dim, supp_dim
            )

            if column.function is not None:
                # TODO #200: Do we want to allow this?
                msg = f"Applying a SQL function to added column={name} is not supported yet"
                raise NotImplementedError(msg)
            expected_base_dim_cols = context.get_dimension_column_names_by_name(
                supp_dim.model.dimension_type,
                base_dim.model.name,
                dataset_id=self._dataset_id,
            )
            if len(expected_base_dim_cols) > 1:
                msg = "Bug: Non-time dimensions cannot have more than one base dimension column"
                raise Exception(msg)
            expected_base_dim_col = expected_base_dim_cols[0]
            try:
                c_before = df.count().to_pyarrow().as_py()
            except Exception:
                c_before = "unknown"

            df = map_stacked_dimension(
                df,
                records,
                expected_base_dim_col,
                drop_column=False,
                to_column=name,
            )
            try:
                c_after = df.count().to_pyarrow().as_py()
                print(
                    f"DEBUG: add_columns {self._dataset_id}: {name} <- {expected_base_dim_col}. Rows: {c_before} -> {c_after}"
                )
                if name == "end_uses_by_fuel_type":
                    print(
                        f"DEBUG: Counts by {name}: {df.group_by(name).count().to_pyarrow().to_pylist()}"
                    )
            except Exception as e:
                print(f"DEBUG: add_columns debug failed: {e}")

            if context.model.result.column_type == ColumnType.DIMENSION_NAMES:
                assert supp_dim.model.dimension_type != DimensionType.TIME
                column_names = [name]
            else:
                column_names = [expected_base_dim_col]
            context.add_dimension_metadata(
                supp_dim.model.dimension_type,
                DimensionMetadataModel(dimension_name=name, column_names=column_names),
                dataset_id=self.dataset_id,
            )

        if "fraction" in df.columns:
            mutations = {}
            for col in value_columns:
                mutations[col] = df[col] * df["fraction"]
            df = df.mutate(**mutations).drop("fraction")

        return df

    @abc.abstractmethod
    def process_aggregations(
        self, df: ir.Table, aggregations: list[AggregationModel], context: QueryContext
    ) -> ir.Table:
        """Aggregate the dimensional data as specified by aggregations.

        Parameters
        ----------
        df : ibis.expr.types.Table
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        ibis.expr.types.Table

        """

    @property
    def project_config(self) -> ProjectConfig:
        """Return the project config of the dataset being processed."""
        return self._project_config

    @property
    def dataset_id(self) -> str | None:
        """Return the ID of the dataset being processed."""
        return self._dataset_id

    def convert_columns_to_query_names(
        self, df: ir.Table, dataset_id: str, context: QueryContext
    ) -> ir.Table:
        """Convert columns from dimension types to dimension query names."""
        columns = set(df.columns)
        for dim_type in DimensionType:
            if dim_type == DimensionType.TIME:
                time_dim = self._project_config.get_base_time_dimension()
                df = time_dim.map_timestamp_load_data_columns_for_query_name(df)
            elif dim_type.value in columns:
                existing_col = dim_type.value
                new_cols = context.get_dimension_column_names(dim_type, dataset_id=dataset_id)
                assert len(new_cols) == 1, f"{dim_type=} {new_cols=}"
                new_col = next(iter(new_cols))
                if existing_col != new_col:
                    df = df.rename({new_col: existing_col})
                    logger.debug("Converted column from %s to %s", existing_col, new_col)

        return df

    def replace_ids_with_names(self, df: ir.Table) -> ir.Table:
        """Replace dimension record IDs with names."""
        assert not {"id", "name"}.intersection(df.columns), df.columns
        orig_count = df.count().to_pyarrow().as_py()
        all_query_names = self._project_config.get_dimension_names_mapped_to_type()
        for name in set(df.columns).intersection(all_query_names.keys()):
            if all_query_names[name] != DimensionType.TIME:
                # Time doesn't have records.
                dim_config = self._project_config.get_dimension_with_records(name)
                records = dim_config.get_records_dataframe().select("id", "name")
                df = (
                    df.join(records, df[name] == records["id"])
                    .drop("id", name)
                    .rename({name: "name"})
                )
        new_count = df.count().to_pyarrow().as_py()
        assert new_count == orig_count, f"counts changed {new_count} {orig_count}"
        return df

    @staticmethod
    def _add_column_to_dim_type(
        column: ColumnModel, dim_type: DimensionType, column_to_dim_type: dict[str, DimensionType]
    ) -> None:
        name = column.get_column_name()
        if name in column_to_dim_type:
            assert dim_type == column_to_dim_type[name], f"{name=} {column_to_dim_type}"
        column_to_dim_type[name] = dim_type

    def _build_group_by_columns(
        self,
        table: ir.Table,
        columns: list[ColumnModel],
        context: QueryContext,
        final_metadata: DatasetDimensionsMetadataModel,
    ):
        group_by_cols = []
        for column in columns:
            dim = self._project_config.get_dimension(column.dimension_name)
            dim_type = dim.model.dimension_type
            match context.model.result.column_type:
                case ColumnType.DIMENSION_TYPES:
                    column_names = context.get_dimension_column_names_by_name(
                        dim_type, column.dimension_name, dataset_id=self._dataset_id
                    )
                    if dim_type == DimensionType.TIME:
                        group_by_cols += column_names
                    else:
                        group_by_cols.append(dim_type.value)
                case ColumnType.DIMENSION_NAMES:
                    column_names = [column.get_column_name()]
                    expr = self._make_group_by_column_expr(table, column, context)
                    group_by_cols.append(expr)
                    if not isinstance(expr, str) or expr != column.dimension_name:
                        # In this case we are replacing any existing query name with an expression
                        # or alias, and so the old name must be removed.
                        final_metadata.remove_metadata(dim_type, column.dimension_name)
                case _:
                    msg = f"Bug: unhandled: {context.model.result.column_type}"
                    raise NotImplementedError(msg)
            final_metadata.add_metadata(
                dim_type,
                DimensionMetadataModel(
                    dimension_name=column.dimension_name, column_names=column_names
                ),
            )
        return group_by_cols

    def _make_group_by_column_expr(self, table: ir.Table, column, context: QueryContext):
        print(f"DEBUG: _make_group_by_column_expr calling for {column.dimension_name}")
        if column.function is None:
            expr = column.dimension_name
        else:
            col_expr = table[column.dimension_name]
            # Handle time zone conversion for time dimension functions
            dim = self._project_config.get_dimension(column.dimension_name)
            if dim.model.dimension_type == DimensionType.TIME:
                tz = self._project_config.get_base_time_dimension().get_time_zone()
                if tz:
                    target_type = dt.Timestamp(timezone=tz)
                    col_expr = col_expr.cast(target_type)

                    # Handle time zone conversion to naive local time
                    if dsgrid.ibis_api.get_backend_engine() == BackendEngine.DUCKDB:
                        # DuckDB CAST(timestamptz AS timestamp) converts to UTC naive.
                        # functionality we need is provided by the timezone() function.
                        col_expr = timezone(tz, col_expr)
                    else:
                        # Spark and potentially other backends
                        # Cast to the target timezone, and then to naive timestamp so that valid
                        # local time is used for extraction functions like hour().
                        col_expr = col_expr.cast(dt.Timestamp(timezone=None))

            expr = getattr(col_expr, column.function)()
            if column.alias is not None:
                expr = expr.name(column.alias)

        # DEBUG
        # try:
        #    print(f"DEBUG: SQL for {column.dimension_name}: {table.select(expr).compile()}")
        # except Exception:
        #    pass
        return expr

    @track_timing(timer_stats_collector)
    def _remove_invalid_null_timestamps(self, df: ir.Table, orig_id, context: QueryContext):
        if id(df) != orig_id:
            # The table could have NULL timestamps that designate expected-missing data.
            # Those rows could be obsolete after aggregating stacked dimensions.
            # This is an expensive operation, so only do it if the dataframe changed.
            value_columns = context.get_value_columns()
            if not value_columns:
                msg = "Bug: value_columns cannot be empty"
                raise Exception(msg)
            time_columns = context.get_dimension_column_names(
                DimensionType.TIME, dataset_id=self._dataset_id
            )
            if time_columns:
                # Persist the query up to this point to avoid multiple evaluations.
                df = persist_intermediate_query(df, context.scratch_dir_context)
                stacked_columns = set(df.columns) - value_columns.union(time_columns)
                df = remove_invalid_null_timestamps(df, time_columns, stacked_columns)
                logger.debug("Removed any rows with invalid null timestamps")
        return df
