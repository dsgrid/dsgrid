"""Tests for :meth:`TableFormatHandlerBase._build_group_by_columns`.

The method both builds the group-by expressions and maintains
``final_metadata``, which is shared across every aggregation in
:meth:`UnpivotedTableHandler.process_stacked_aggregations`. A dimension's
recorded metadata must only be dropped when a later column replaces it with a
function/alias expression; a plain reference to the same dimension must leave
earlier entries alone.
"""

from types import SimpleNamespace

from dsgrid.dataset.unpivoted_table import UnpivotedTableHandler
from dsgrid.dimension.base_models import DimensionType
from dsgrid.ibis.types import use_duckdb
from dsgrid.query.models import (
    ColumnModel,
    ColumnType,
    DatasetDimensionsMetadataModel,
)

TIME_DIM = "time_est"


def _make_handler(dimension_types: dict[str, DimensionType]) -> UnpivotedTableHandler:
    """Return a handler whose project config resolves dimension names to types."""

    def get_dimension(name: str):
        return SimpleNamespace(model=SimpleNamespace(dimension_type=dimension_types[name]))

    project_config = SimpleNamespace(get_dimension=get_dimension)
    return UnpivotedTableHandler(project_config, dataset_id="ds")


def _make_context(column_type: ColumnType):
    return SimpleNamespace(model=SimpleNamespace(result=SimpleNamespace(column_type=column_type)))


def _metadata_keys(metadata: DatasetDimensionsMetadataModel, dim_type: DimensionType):
    return [(x.dimension_name, tuple(x.column_names)) for x in metadata.get_metadata(dim_type)]


def test_plain_column_preserves_metadata_from_an_earlier_aggregation():
    """A plain column must not evict an alias entry recorded by an earlier aggregation.

    ``_make_group_by_column_expr`` quotes bare column names, so a check of
    ``expr != column.dimension_name`` is true for every column and would drop
    the ``hour`` entry that the first aggregation added.
    """
    handler = _make_handler({TIME_DIM: DimensionType.TIME})
    context = _make_context(ColumnType.DIMENSION_NAMES)
    metadata = DatasetDimensionsMetadataModel()

    handler._build_group_by_columns(
        [ColumnModel(dimension_name=TIME_DIM, function="hour", alias="hour")], context, metadata
    )
    handler._build_group_by_columns([ColumnModel(dimension_name=TIME_DIM)], context, metadata)

    assert _metadata_keys(metadata, DimensionType.TIME) == [
        (TIME_DIM, ("hour",)),
        (TIME_DIM, (TIME_DIM,)),
    ]


def test_function_column_replaces_metadata_from_an_earlier_aggregation():
    """A function/alias column still evicts the plain entry it replaces."""
    handler = _make_handler({TIME_DIM: DimensionType.TIME})
    context = _make_context(ColumnType.DIMENSION_NAMES)
    metadata = DatasetDimensionsMetadataModel()

    handler._build_group_by_columns([ColumnModel(dimension_name=TIME_DIM)], context, metadata)
    handler._build_group_by_columns(
        [ColumnModel(dimension_name=TIME_DIM, function="hour", alias="hour")], context, metadata
    )

    assert _metadata_keys(metadata, DimensionType.TIME) == [(TIME_DIM, ("hour",))]


def test_repeated_plain_column_is_deduplicated():
    """The same plain column twice records exactly one metadata entry."""
    handler = _make_handler({"county": DimensionType.GEOGRAPHY})
    context = _make_context(ColumnType.DIMENSION_NAMES)
    metadata = DatasetDimensionsMetadataModel()

    for _ in range(2):
        handler._build_group_by_columns([ColumnModel(dimension_name="county")], context, metadata)

    assert _metadata_keys(metadata, DimensionType.GEOGRAPHY) == [("county", ("county",))]


def test_group_by_expressions_are_quoted():
    """Pin the expressions handed to the aggregation builder.

    ``handle_column_spaces`` quotes with the runtime backend's identifier
    character: double quotes on DuckDB, backticks on Spark. The expected strings
    are spelled out per backend rather than built with that helper, so this test
    pins the actual quoting instead of restating the implementation.
    """
    quote = '"' if use_duckdb() else "`"
    handler = _make_handler({"county": DimensionType.GEOGRAPHY, TIME_DIM: DimensionType.TIME})
    context = _make_context(ColumnType.DIMENSION_NAMES)
    metadata = DatasetDimensionsMetadataModel()

    group_by_cols = handler._build_group_by_columns(
        [
            ColumnModel(dimension_name="county"),
            ColumnModel(dimension_name=TIME_DIM, function="hour", alias="hour"),
        ],
        context,
        metadata,
    )

    assert group_by_cols == [
        f"{quote}county{quote}",
        f"hour({quote}{TIME_DIM}{quote}) AS {quote}hour{quote}",
    ]
