import logging
import os
from pathlib import Path
from typing import Any, Iterable, cast
from datetime import tzinfo

import chronify
import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir
import pandas as pd
from chronify.models import TableSchema

import dsgrid
from dsgrid.common import SCALING_FACTOR_COLUMN, TIME_ZONE_COLUMN, VALUE_COLUMN, BackendEngine
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.date_time_dimension_config import DateTimeDimensionConfig
from dsgrid.config.dimension_config import DimensionBaseConfigWithFiles
from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    DaylightSavingFallBackType,
    DaylightSavingSpringForwardType,
    TimeBasedDataAdjustmentModel,
)
from dsgrid.exceptions import (
    DSGFileInputError,
    DSGInvalidField,
    DSGInvalidDimensionMapping,
    DSGInvalidDataset,
    DSGInvalidOperation,
)
from dsgrid.ibis.backend import create_chronify_store, get_runtime_backend
from dsgrid.ibis.io import read_parquet
from dsgrid.ibis.operations import (
    coalesce,
    count_distinct_on_group_by,
    create_temp_view,
    cross_join,
    drop_columns,
    except_all,
    filter_sql,
    handle_column_spaces,
    join,
    join_multiple_columns,
    rename_columns,
    # Aliased because repartition_if_needed_by_mapping has a ``repartition`` parameter
    # that would otherwise shadow this function inside its body.
    repartition as repartition_table,
    union_all,
    unpivot,
    with_literal_column,
)
from dsgrid.ibis.temp import make_temp_view_name
from dsgrid.ibis.table_utils import (
    get_unique_values,
    get_unique_values_per_column,
    is_table_empty,
    table_to_records,
)
from dsgrid.ibis.types import use_duckdb
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.io import persist_table, write_dataframe
from dsgrid.ibis.null_checks import check_for_nulls
from dsgrid.ibis.session import (
    create_dataframe_from_product,
    get_runtime_session,
    get_spark_session,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing
from dsgrid.utils.utilities import sorted_with_nulls

logger = logging.getLogger(__name__)


def _create_shared_chronify_store() -> chronify.Store:
    """Create a chronify Store that uses dsgrid's active runtime session through Ibis."""
    if use_duckdb():
        return create_chronify_store()
    return create_chronify_store(session=get_spark_session())


def _create_runtime_chronify_store() -> chronify.Store:
    """Create a chronify Store using dsgrid's shared Ibis backend."""
    return create_chronify_store()


def _create_chronify_source(store: chronify.Store, df: ibis.Table, schema: TableSchema) -> None:
    """Register ``df`` with ``store`` as a view.

    Ingesting the rows into a chronify table is never necessary: every dsgrid
    table is an Ibis table bound to the runtime backend, so chronify can read
    it in place.
    """
    store.create_view(schema, df)


def _drop_chronify_source(store: chronify.Store, schema: TableSchema) -> None:
    store.drop_view(schema.name, if_exists=True)


def _align_to_table_schema(df: ibis.Table, template: ibis.Table) -> ibis.Table:
    schema = template.schema()
    exprs = {}
    for column in template.columns:
        target_type = schema[column]
        if column in df.columns:
            exprs[column] = df[column].cast(target_type)
        else:
            exprs[column] = ibis.null().cast(target_type)
    return df.select(**exprs)


def _to_duckdb_sql_type(data_type: dt.DataType) -> str:
    if data_type.is_boolean():
        return "BOOLEAN"
    if data_type.is_int8():
        return "TINYINT"
    if data_type.is_int16() or data_type.is_int32():
        return "INTEGER"
    if data_type.is_int64():
        return "BIGINT"
    if data_type.is_float32():
        return "FLOAT"
    if data_type.is_float64():
        return "DOUBLE"
    if data_type.is_string():
        return "VARCHAR"
    if data_type.is_timestamp():
        return "TIMESTAMP"
    msg = f"Unsupported data type for schema alignment: {data_type}"
    raise NotImplementedError(msg)


def _alias_expression(expr, alias: str):
    return expr.name(alias) if isinstance(expr, ir.Value) else expr.alias(alias)


def map_stacked_dimension(
    df: ibis.Table,
    records: ibis.Table,
    column: str,
    drop_column: bool = True,
    to_column: str | None = None,
) -> ibis.Table:
    """Map a stacked (long-format) dimension column through a mapping table.

    Inner-joins ``df`` to ``records`` on ``df[column] == records.from_id``: a df row
    whose ``column`` value has no ``from_id`` is dropped, and a ``from_id`` that fans
    out to several ``to_id`` rows splits the df row into one per ``to_id``. Records
    whose ``to_id`` is NULL are removed first, dropping the df rows that map to them.

    Fractions: ``records`` always carries ``from_fraction`` (default 1.0); ``df``
    carries a running ``fraction`` that accumulates across chained mappings,
    initialized to 1.0 when absent. Output ``fraction = fraction * from_fraction``.

    Parameters
    ----------
    df : ibis.Table
        Long-format table being mapped; contains ``column`` and may already carry
        a ``fraction`` column from a prior mapping.
    records : ibis.Table
        Mapping records with ``from_id``, nullable ``to_id``, and ``from_fraction``.
    column : str
        The df column to map (matched against ``from_id``).
    drop_column : bool, optional
        Drop the source ``column`` after mapping (map in place). Pass False to keep
        it alongside the mapped ``to_column``. False is only allowed when there is
        a distinct ``to_column``.
    to_column : str | None, optional
        Name of the mapped (``to_id``) column; defaults to ``column`` (map in place).

    Returns
    -------
    ibis.Table
        ``df`` with ``column`` mapped and ``fraction`` updated; the ``from_id`` /
        ``from_fraction`` join columns do not leak into the output.

    Raises
    ------
    DSGInvalidOperation
        If ``drop_column`` is False while ``to_column`` resolves to ``column``: the
        mapped column would collide with the preserved source column.
    """
    to_column_ = to_column or column
    if not drop_column and to_column_ == column:
        msg = (
            f"map_stacked_dimension cannot keep the source column {column!r} "
            "(drop_column=False) while mapping it in place; pass a distinct to_column."
        )
        raise DSGInvalidOperation(msg)
    if "fraction" not in df.columns:
        df = with_literal_column(df, "fraction", 1.0)
    # map and consolidate from_fraction only
    records = filter_sql(records, "to_id IS NOT NULL")
    df = drop_columns(join(df, records, column, "from_id", how="inner"), "from_id")
    if drop_column:
        df = drop_columns(df, column)
    df = rename_columns(df, {"to_id": to_column_})
    nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
    df = df.select(
        *nonfraction_cols,
        _alias_expression(df["fraction"] * df["from_fraction"], "fraction"),
    )
    return df


def add_time_zone(
    load_data_df: ibis.Table,
    geography_dim: DimensionBaseConfigWithFiles,
    df_key: str = "geography",
    dim_key: str = "id",
):
    """Add a time_zone column to a load_data dataframe from a geography dimension.

    Parameters
    ----------
    load_data_df : Ibis table
    geography_dim: DimensionConfig

    Returns
    -------
    Ibis table

    """
    geo_records = geography_dim.get_records_dataframe()
    if df_key not in load_data_df.columns:
        msg = f"Cannot locate {df_key=} in load_data_df: {load_data_df.columns}"
        raise ValueError(msg)

    df = add_column_from_records(
        load_data_df, geo_records, TIME_ZONE_COLUMN, df_key, record_key=dim_key
    )
    return df


def add_column_from_records(df, dimension_records, record_column, df_key, record_key: str = "id"):
    df = join(
        df1=df,
        df2=dimension_records.select(
            _alias_expression(dimension_records[record_key], "record_id"),
            record_column,
        ),
        column1=df_key,
        column2="record_id",
        how="inner",
    )
    df = drop_columns(df, "record_id")
    return df


def add_null_rows_from_load_data_lookup(df: ibis.Table, lookup: ibis.Table) -> ibis.Table:
    """Add null rows from the nulled load data lookup table to data table.

    Parameters
    ----------
    df
        load data table
    lookup
        load data lookup table that has been filtered for nulls.
    """
    if not is_table_empty(lookup):
        intersect_cols = set(lookup.columns).intersection(df.columns)
        null_rows_to_add = except_all(lookup.select(*intersect_cols), df.select(*intersect_cols))
        for col in set(df.columns).difference(null_rows_to_add.columns):
            null_rows_to_add = with_literal_column(null_rows_to_add, col, None)
        # union_all (not Ibis default .union) — load_data rows are not unique
        # per dimension combination, so distinct semantics would silently drop
        # legitimate duplicates from the original df. Matches the pre-Ibis
        # Spark .union() semantics this code originally relied on.
        df = union_all(df, _align_to_table_schema(null_rows_to_add, df))

    return df


def apply_scaling_factor(
    df: ibis.Table,
    value_column: str,
    mapping_manager: DatasetMappingManager,
    scaling_factor_column: str = SCALING_FACTOR_COLUMN,
) -> ibis.Table:
    """Apply the scaling factor to all value columns and then drop the scaling factor column."""
    op = mapping_manager.plan.apply_scaling_factor_op
    if mapping_manager.has_completed_operation(op):
        return df

    df = _apply_scaling_factor_sql(df, value_column, scaling_factor_column)
    if mapping_manager.plan.apply_scaling_factor_op.persist:
        df = mapping_manager.persist_table(df, op)
    return df


def _apply_scaling_factor_sql(
    df: ibis.Table,
    value_column: str,
    scaling_factor_column: str,
):
    cols = (x for x in df.columns if x not in (value_column, scaling_factor_column))
    cols_str = ",".join(cols)
    view = create_temp_view(df)
    query = f"""
        SELECT
            {cols_str},
            (
                CASE WHEN {scaling_factor_column} IS NULL THEN {value_column}
                ELSE {value_column} * {scaling_factor_column} END
            ) AS {value_column}
        FROM {view}
    """
    return get_runtime_backend().sql(query)


def check_historical_annual_time_model_year_consistency(
    df: ibis.Table, time_column: str, model_year_column: str
) -> None:
    """Check that the model year values match the time dimension years for a historical
    dataset with an annual time dimension.
    """
    invalid_df = filter_sql(
        filter_sql(
            df.select(time_column, model_year_column),
            f"{time_column} IS NOT NULL",
        ).distinct(),
        f"{time_column} != {model_year_column}",
    )
    if not is_table_empty(invalid_df):
        invalid = table_to_records(invalid_df.limit(100))
        msg = (
            "A historical dataset with annual time must have rows where the time years match the model years. "
            f"{invalid}"
        )
        raise DSGInvalidDataset(msg)


@track_timing(timer_stats_collector)
def check_null_value_in_dimension_rows(dim_table, exclude_columns=None):
    if os.environ.get("__DSGRID_SKIP_CHECK_NULL_DIMENSION__"):
        # This has intermittently caused GC-related timeouts for TEMPO.
        # Leave a backdoor to skip these checks, which may eventually be removed.
        logger.warning("Skip check_null_value_in_dimension_rows")
        return

    try:
        exclude = {"id"}
        if exclude_columns is not None:
            exclude.update(exclude_columns)
        check_for_nulls(dim_table, exclude_columns=exclude)
    except DSGInvalidField as exc:
        msg = (
            "Invalid dimension mapping application. "
            "Combination of remapped dataset dimensions contain NULL value(s) for "
            f"dimension(s): \n{str(exc)}"
        )
        raise DSGInvalidDimensionMapping(msg)


def handle_dimension_association_errors(
    diff: ibis.Table,
    dataset_table: ibis.Table,
    dataset_id: str,
    expected_cardinalities: dict[str, int] | None = None,
) -> None:
    """Record missing dimension record combinations in a Parquet file and log an error."""
    out_file = Path(f"{dataset_id}__missing_dimension_record_combinations.parquet")
    df = write_dataframe(coalesce(diff, 1), out_file, overwrite=True)
    logger.error(
        "Dataset %s is missing required dimension records. Recorded missing records in %s",
        dataset_id,
        out_file,
    )

    # Analyze patterns in missing data to help identify root causes
    try:
        from dsgrid.rust_ext import find_minimal_patterns_from_file

        logger.info("Analyzing missing data patterns for dataset %s...", dataset_id)
        if out_file.is_dir():
            files = list(out_file.glob("*.parquet"))
            assert len(files) == 1, f"Expected 1 file, got {files}"
            filename = files[0]
        else:
            filename = out_file
        patterns = find_minimal_patterns_from_file(
            filename,
            max_depth=0,
            verbose=False,
            expected_cardinalities=expected_cardinalities,
        )

        if patterns:
            logger.error("Found %d minimal closed patterns in missing data:", len(patterns))
            for pattern in patterns[:10]:  # Show top 10 patterns
                logger.error(
                    "  Pattern %d: %s = %s (%d missing rows)",
                    pattern.pattern_id,
                    " | ".join(pattern.columns),
                    " | ".join(pattern.values),
                    pattern.num_rows,
                )
            if len(patterns) > 10:
                logger.error("  ... and %d more patterns", len(patterns) - 10)
        else:
            logger.warning("No closed patterns found in missing data")
    except ImportError:
        logger.warning(
            "Rust pattern analysis not available. Install with: pip install -e . "
            "or build with: maturin develop"
        )
        _look_for_error_contributors(df, dataset_table)
    except Exception as e:
        logger.warning("Failed to analyze missing data patterns: %s", e)

    msg = (
        f"Dataset {dataset_id} is missing required dimension records. "
        "Please look in the log file for more information."
    )
    raise DSGInvalidDataset(msg)


def _look_for_error_contributors(diff: ibis.Table, dataset_table: ibis.Table) -> None:
    # Compute COUNT(DISTINCT col) for every column in a single aggregation
    # query per table. The previous loop issued 2N .execute() calls; this
    # version issues 2 regardless of column count, which matters because
    # this runs on the error path against the full dataset table.
    # Ibis's stub for Table.aggregate types **kwargs against the same Sequence
    # type as the positional `having` parameter, so dict-spread aggregation
    # exprs (which work fine at runtime) trip ty. Suppress per call below.
    cols = list(diff.columns)
    diff_aggs = {col: diff[col].nunique() for col in cols}
    dataset_aggs = {col: dataset_table[col].nunique() for col in cols}
    diff_counts = (
        diff.aggregate(**diff_aggs)  # ty: ignore[invalid-argument-type]
        .execute()
        .iloc[0]
    )
    dataset_counts = (
        dataset_table.aggregate(**dataset_aggs)  # ty: ignore[invalid-argument-type]
        .execute()
        .iloc[0]
    )
    for col in cols:
        if dataset_counts[col] != diff_counts[col]:
            logger.error(
                "Error contributor: column=%s dataset_distinct_count=%s missing_distinct_count=%s",
                col,
                int(dataset_counts[col]),
                int(diff_counts[col]),
            )


def is_noop_mapping(records: ibis.Table) -> bool:
    """Return True if the mapping is a no-op."""
    return is_table_empty(
        filter_sql(
            records,
            "(to_id IS NULL and from_id IS NOT NULL) or "
            "(to_id IS NOT NULL and from_id IS NULL) or "
            "(from_id != to_id) or (from_fraction != 1.0)",
        )
    )


def map_time_dimension_with_chronify_duckdb(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    wrap_time_allowed: bool = False,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
) -> ibis.Table:
    """Create a time-mapped table with chronify and DuckDB.
    All operations are performed in memory.
    """
    # This will only work if the source and destination tables will fit in memory.
    # We could potentially use a file-based DuckDB database for larger-than memory datasets.
    # However, time checks and unpivot operations have failed with out-of-memory errors,
    # and so we have never reached this point.
    # If we solve those problems, this code could be modified.
    src_schema, dst_schema = _get_mapping_schemas(
        df, from_time_dim, to_time_dim, value_column=value_column
    )
    store = _create_runtime_chronify_store()
    _create_chronify_source(store, df, src_schema)
    try:
        store.map_table_time_config(
            src_schema.name,
            dst_schema,
            wrap_time_allowed=wrap_time_allowed,
            data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
        )
        return store.get_table(dst_schema.name)
    finally:
        _drop_chronify_source(store, src_schema)


def convert_time_zone_with_chronify_duckdb(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: tzinfo | None,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
) -> ibis.Table:
    """Create a single time zone-converted table with chronify and DuckDB.
    All operations are performed in memory.
    Time zone conversion converts from tz-aware timestamps to
    tz-naive timestamps with the specified time zone as a new column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=True, value_column=value_column
    )
    store = _create_runtime_chronify_store()
    _create_chronify_source(store, df, src_schema)
    try:
        dst_schema = store.convert_time_zone(
            src_schema.name,
            time_zone,
        )
        return store.get_table(dst_schema.name)
    finally:
        _drop_chronify_source(store, src_schema)


def convert_time_zone_by_column_with_chronify_duckdb(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    time_zone_column: str = TIME_ZONE_COLUMN,
    wrap_time_allowed: bool = False,
) -> ibis.Table:
    """Create a multiple time zone-converted table (based on a time_zone_column)
    using chronify and DuckDB.
    All operations are performed in memory.
    Time zone conversion converts from tz-aware timestamps to
    tz-naive timestamps with time zones specified in the time_zone_column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=True, value_column=value_column
    )
    store = _create_runtime_chronify_store()
    _create_chronify_source(store, df, src_schema)
    try:
        dst_schema = store.convert_time_zone_by_column(
            src_schema.name,
            time_zone_column,
            wrap_time_allowed=wrap_time_allowed,
        )
        return store.get_table(dst_schema.name)
    finally:
        _drop_chronify_source(store, src_schema)


def localize_time_zone_with_chronify_duckdb(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: tzinfo | None,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
) -> ibis.Table:
    """Create a single time zone-localized table with chronify and DuckDB.
    All operations are performed in memory.
    Time zone localization converts from tz-naive timestamps to tz-aware timestamps based on time_zone input.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=False, value_column=value_column
    )

    store = _create_runtime_chronify_store()
    _create_chronify_source(store, df, src_schema)
    try:
        dst_schema = store.localize_time_zone(
            src_schema.name,
            time_zone,
        )
        return store.get_table(dst_schema.name)
    finally:
        _drop_chronify_source(store, src_schema)


def localize_time_zone_by_column_with_chronify_duckdb(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    time_zone_column: str = TIME_ZONE_COLUMN,
) -> ibis.Table:
    """Create a multiple time zone-localized table (based on a time_zone_column)
    using chronify and DuckDB.
    All operations are performed in memory.
    Time zone localization converts from tz-naive timestamps to tz-aware timestamps based on
    the time zones specified in the time_zone_column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=False, value_column=value_column
    )
    store = _create_runtime_chronify_store()
    _create_chronify_source(store, df, src_schema)
    try:
        dst_schema = store.localize_time_zone_by_column(
            src_schema.name,
            time_zone_column,
        )
        return store.get_table(dst_schema.name)
    finally:
        _drop_chronify_source(store, src_schema)


def map_time_dimension_with_chronify_runtime_path(
    df: ibis.Table,
    filename: Path,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    wrap_time_allowed: bool = False,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
) -> ibis.Table:
    """Create a time-mapped table with chronify and the runtime backend using the local filesystem.
    Chronify will store the mapped table in a Parquet file within scratch_dir_context.
    """
    src_schema, dst_schema = _get_mapping_schemas(
        df, from_time_dim, to_time_dim, value_column=value_column
    )
    store = _create_shared_chronify_store()
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    try:
        store.map_table_time_config(
            src_schema.name,
            dst_schema,
            check_mapped_timestamps=False,
            output_file=output_file,
            wrap_time_allowed=wrap_time_allowed,
            data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
        )
    finally:
        # Drop the source view even if the operation raises, mirroring the DuckDB
        # dispatchers. chronify's view is not dsgrid-owned temp state, so the sweep in
        # QueryContext.finalize() is the only thing that would otherwise remove it, and
        # callers without a QueryContext (a direct unit test, say) leave it in the
        # session for good.
        _drop_chronify_source(store, src_schema)
    return read_parquet(output_file)


def convert_time_zone_with_chronify_runtime_path(
    df: ibis.Table,
    filename: Path,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: tzinfo | None,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
) -> ibis.Table:
    """Create a single time zone-converted table with chronify and the runtime backend using the local filesystem.
    Time zone conversion converts from tz-aware timestamps to
    tz-naive timestamps with the specified time zone as a new column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=True, value_column=value_column
    )
    store = _create_shared_chronify_store()
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    try:
        store.convert_time_zone(
            src_schema.name,
            time_zone,
            output_file=output_file,
        )
    finally:
        # See map_time_dimension_with_chronify_runtime_path for why this is explicit.
        _drop_chronify_source(store, src_schema)
    return read_parquet(output_file)


def convert_time_zone_by_column_with_chronify_runtime_path(
    df: ibis.Table,
    filename: Path,
    from_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    time_zone_column: str = TIME_ZONE_COLUMN,
    wrap_time_allowed: bool = False,
) -> ibis.Table:
    """Create a multiple time zone-converted table (based on a time_zone_column)
    using chronify and the runtime backend using the local filesystem.
    Time zone conversion converts from tz-aware timestamps to
    tz-naive timestamps with time zones specified in the time_zone_column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=True, value_column=value_column
    )
    store = _create_shared_chronify_store()
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    try:
        store.convert_time_zone_by_column(
            src_schema.name,
            time_zone_column,
            wrap_time_allowed=wrap_time_allowed,
            output_file=output_file,
        )
    finally:
        # See map_time_dimension_with_chronify_runtime_path for why this is explicit.
        _drop_chronify_source(store, src_schema)
    return read_parquet(output_file)


def localize_time_zone_with_chronify_runtime_path(
    df: ibis.Table,
    filename: Path,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: tzinfo | None,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
) -> ibis.Table:
    """Create a single time zone-localized table with chronify and the runtime backend using the local filesystem.
    Time zone localization converts from tz-naive timestamps to tz-aware timestamps based on time_zone input.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=False, value_column=value_column
    )
    store = _create_shared_chronify_store()
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    try:
        store.localize_time_zone(
            src_schema.name,
            time_zone,
            output_file=output_file,
        )
    finally:
        # See map_time_dimension_with_chronify_runtime_path for why this is explicit.
        _drop_chronify_source(store, src_schema)
    return read_parquet(output_file)


def localize_time_zone_by_column_with_chronify_runtime_path(
    df: ibis.Table,
    filename: Path,
    from_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    value_column: str = VALUE_COLUMN,
    time_zone_column: str = TIME_ZONE_COLUMN,
) -> ibis.Table:
    """Create a multiple time zone-localized table (based on a time_zone_column)
    using chronify and the runtime backend using the local filesystem.
    Time zone localization converts from tz-naive timestamps to tz-aware timestamps based on
    the time zones specified in the time_zone_column.
    """
    src_schema = _get_src_schema(
        df, from_time_dim, data_is_localized=False, value_column=value_column
    )
    store = _create_shared_chronify_store()
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    try:
        store.localize_time_zone_by_column(
            src_schema.name,
            time_zone_column=time_zone_column,
            output_file=output_file,
        )
    finally:
        # See map_time_dimension_with_chronify_runtime_path for why this is explicit.
        _drop_chronify_source(store, src_schema)
    return read_parquet(output_file)


def _to_chronify_time_based_data_adjustment(
    adj: TimeBasedDataAdjustmentModel | None,
) -> chronify.TimeBasedDataAdjustment | None:
    if adj is None:
        return None
    if (
        adj.daylight_saving_adjustment.spring_forward_hour == DaylightSavingSpringForwardType.NONE
        and adj.daylight_saving_adjustment.fall_back_hour == DaylightSavingFallBackType.NONE
    ):
        chronify_dst_adjustment = chronify.time.DaylightSavingAdjustmentType.NONE
    elif (
        adj.daylight_saving_adjustment.spring_forward_hour == DaylightSavingSpringForwardType.DROP
        and adj.daylight_saving_adjustment.fall_back_hour == DaylightSavingFallBackType.DUPLICATE
    ):
        chronify_dst_adjustment = (
            chronify.time.DaylightSavingAdjustmentType.DROP_SPRING_FORWARD_DUPLICATE_FALLBACK
        )
    elif (
        adj.daylight_saving_adjustment.spring_forward_hour == DaylightSavingSpringForwardType.DROP
        and adj.daylight_saving_adjustment.fall_back_hour == DaylightSavingFallBackType.INTERPOLATE
    ):
        chronify_dst_adjustment = (
            chronify.time.DaylightSavingAdjustmentType.DROP_SPRING_FORWARD_INTERPOLATE_FALLBACK
        )
    else:
        msg = f"dsgrid time_based_data_adjustment = {adj}"
        raise NotImplementedError(msg)

    return chronify.TimeBasedDataAdjustment(
        leap_day_adjustment=chronify.time.LeapDayAdjustmentType[adj.leap_day_adjustment.name],
        daylight_saving_adjustment=chronify_dst_adjustment,
    )


def _adjust_time_config_for_post_localization(
    time_config: chronify.TimeBaseModel,
    time_dim: TimeDimensionBaseConfig,
) -> chronify.TimeBaseModel:
    """Return ``time_config`` adjusted to match a time column that was already localized.

    ``to_chronify()`` always reports the original datetime shape, which doesn't match
    the actual data with a ``localize_to_single_tz`` plan: the pre-localization shape
    has an NTZ dtype and a naive start, but registration localizes the data and only
    updates the dimension config in memory, so the persisted record stays stale (#427).
    Rebuild the config as ``TIMESTAMP_TZ`` with a localized start so chronify's mapping
    table lines up with the instants actually stored.

    Callers say whether the data has been localized rather than having this function
    infer it from the column dtype. Only DuckDB can answer that question: Spark's
    ``TimestampType`` is instant-only, so ibis reports ``Timestamp(timezone=None)`` for
    tz-aware and naive data alike. Inferring from the dtype therefore silently skipped
    the adjustment on Spark, and the resulting offset between the mapping table and the
    data dropped rows with no error -- everything for a short range, and ``offset``
    hours' worth for a realistic one.
    """
    if not isinstance(time_dim, DateTimeDimensionConfig):
        return time_config
    if time_dim.get_localization_plan() != "localize_to_single_tz":
        return time_config
    # localize_to_single_tz requires ALIGNED_IN_ABSOLUTE_TIME, whose to_chronify()
    # branch always returns DatetimeRange.
    assert isinstance(time_config, chronify.DatetimeRange), time_config

    if len(time_dim.get_load_data_time_columns()) != 1:
        return time_config

    tz = time_dim.get_chronify_time_zone()
    new_start = pd.Timestamp(time_config.start)
    if new_start.tzinfo is None:
        new_start = new_start.tz_localize(tz)
    return chronify.DatetimeRange(
        dtype=chronify.TimeDataType.TIMESTAMP_TZ,
        time_column=time_config.time_column,
        start=new_start,
        length=time_config.length,
        resolution=time_config.resolution,
        measurement_type=time_config.measurement_type,
        interval_type=time_config.interval_type,
    )


def _get_src_schema(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    *,
    data_is_localized: bool,
    src_name: str | None = None,
    value_column: str = VALUE_COLUMN,
) -> TableSchema:
    """Build the chronify source schema for ``df``.

    Parameters
    ----------
    data_is_localized : bool
        Whether ``df``'s time column has already been localized at this point in the
        pipeline. True for mapping and time zone conversion, whose input is registered
        data that registration localized; a ``localize_to_single_tz`` config then
        describes the pre-localization shape and must be corrected (see
        :func:`_adjust_time_config_for_post_localization`). False for the localization
        operations themselves, whose input is pre-localization by definition and whose
        config is therefore already accurate.
    """
    src = src_name or "src_" + make_temp_view_name()
    time_col_list = from_time_dim.get_load_data_time_columns()
    time_config = from_time_dim.to_chronify()
    if data_is_localized:
        time_config = _adjust_time_config_for_post_localization(time_config, from_time_dim)
    time_array_id_columns = [
        x
        for x in df.columns
        if x in set(df.columns).difference(set(time_col_list)) - {value_column}
    ]
    src_schema = chronify.TableSchema(
        name=src,
        time_config=cast(Any, time_config),
        time_array_id_columns=time_array_id_columns,
        value_column=value_column,
    )
    return src_schema


def _get_dst_schema(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    *,
    data_is_localized: bool,
    value_column: str = VALUE_COLUMN,
) -> TableSchema:
    """Build the chronify destination schema. See :func:`_get_src_schema` for the flag."""
    time_config = to_time_dim.to_chronify()
    if data_is_localized:
        time_config = _adjust_time_config_for_post_localization(time_config, to_time_dim)
    time_col_list = from_time_dim.get_load_data_time_columns()
    time_array_id_columns = [
        x
        for x in df.columns
        if x in set(df.columns).difference(set(time_col_list)) - {value_column}
    ]
    dst_schema = chronify.TableSchema(
        name="dst_" + make_temp_view_name(),
        time_config=cast(Any, time_config),
        time_array_id_columns=time_array_id_columns,
        value_column=value_column,
    )
    return dst_schema


def _get_mapping_schemas(
    df: ibis.Table,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    src_name: str | None = None,
    value_column: str = VALUE_COLUMN,
) -> tuple[TableSchema, TableSchema]:
    """Build both chronify schemas for a time-dimension mapping.

    Mapping only ever runs on registered data, which registration has already localized,
    so both schemas are built with ``data_is_localized=True``.
    """
    src_schema = _get_src_schema(
        df,
        from_time_dim,
        data_is_localized=True,
        src_name=src_name,
        value_column=value_column,
    )
    dst_schema = _get_dst_schema(
        df, from_time_dim, to_time_dim, data_is_localized=True, value_column=value_column
    )
    return src_schema, dst_schema


def ordered_subset_columns(df, subset: set[str]) -> list[str]:
    """Return a list of columns in the dataframe that are present in subset."""
    return [x for x in df.columns if x in subset]


def remove_invalid_null_timestamps(df, time_columns, stacked_columns):
    """Remove rows from the dataframe where the time column is NULL and other rows with the
    same dimensions contain valid data.
    """
    assert len(time_columns) == 1, time_columns
    time_column = next(iter(time_columns))
    orig_columns = df.columns
    stacked = list(stacked_columns)
    joined = join_multiple_columns(
        df,
        count_distinct_on_group_by(df, stacked, time_column, "count_time"),
        stacked,
    )
    return filter_sql(
        joined, f"{handle_column_spaces(time_column)} IS NOT NULL OR count_time = 0"
    ).select(orig_columns)


@track_timing(timer_stats_collector)
def repartition_if_needed_by_mapping(
    df: ibis.Table,
    mapping_type: DimensionMappingType,
    scratch_dir_context: ScratchDirContext,
    repartition: bool | None = None,
) -> tuple[ibis.Table, Path | None]:
    """Repartition the dataframe if the mapping might cause data skew.

    Parameters
    ----------
    df : Ibis table
        The dataframe to repartition.
    mapping_type : DimensionMappingType
    scratch_dir_context : ScratchDirContext
        The scratch directory context to use for temporary files.
    repartition : bool
        If None, repartition based on the mapping type.
        Otherwise, always repartition if True, or never if False.
    """
    if use_duckdb():
        return df, None

    # We experienced an issue with the IEF buildings dataset where the disaggregation of
    # region to county caused a major issue where one Spark executor thread got stuck,
    # seemingly indefinitely. A message like this was repeated continually.
    # UnsafeExternalSorter: Thread 152 spilling sort data of 4.0 GiB to disk (0  time so far)
    # It appears to be caused by data skew, though the imbalance didn't seem too severe.
    # Using a variation of what online sources call a "salting technique" solves the issue.
    # Apply the technique to mappings that will cause an explosion of rows.
    # Note that this probably isn't needed in all cases and we may need to adjust in the
    # future.

    # Note: log messages below are checked in the tests.
    if repartition or (
        repartition is None
        and mapping_type
        in {
            DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
            # These cases might be problematic in the future.
            # DimensionMappingType.ONE_TO_MANY_ASSIGNMENT,
            # DimensionMappingType.ONE_TO_MANY_EXPLICIT_MULTIPLIERS,
            # DimensionMappingType.MANY_TO_MANY_DISAGGREGATION,
            # This is usually happening with scenario and hasn't caused a problem.
            # DimensionMappingType.DUPLICATION,
        }
    ):
        filename = scratch_dir_context.get_temp_filename(suffix=".parquet")
        # Salting techniques online talk about adding or modifying a column with random values.
        # We might be able to use one of our value columns. However, there are cases where there
        # could be many instances of zero or null. So, add a new column with random values.
        logger.info("Repartition after mapping %s", mapping_type)
        salted_column = "salted_key"
        # This is Spark-only code (DuckDB returned above), so spark.sql.shuffle.partitions
        # is the partition count that a bare df.repartition(col) would use.
        num_partitions = int(get_spark_session().conf.get("spark.sql.shuffle.partitions"))
        view = create_temp_view(df)
        salted = get_runtime_session().sql(
            f"SELECT *, CAST(rand() * {num_partitions} AS INTEGER) + 1 "
            f"AS {salted_column} FROM {view}"
        )
        # repartition_table hash-partitions the underlying PySpark DataFrame on the
        # salted column; the shuffle it forces is the entire point of the salting.
        write_dataframe(repartition_table(salted, num_partitions, salted_column), filename)
        df = drop_columns(read_parquet(filename), salted_column)
        logger.info("Completed repartition.")
        return df, filename

    logger.debug("Repartition is not needed for mapping_type %s", mapping_type)
    return df, None


def unpivot_dataframe(
    df: ibis.Table,
    value_columns: Iterable[str],
    variable_column: str,
    time_columns: list[str],
) -> ibis.Table:
    """Unpivot the dataframe, accounting for time columns."""
    values = value_columns if isinstance(value_columns, set) else set(value_columns)
    ids = [x for x in df.columns if x != VALUE_COLUMN and x not in values]
    df = unpivot(df, value_columns, variable_column, VALUE_COLUMN)
    cols = set(df.columns).difference(time_columns)
    new_rows = filter_sql(df, f"{VALUE_COLUMN} IS NULL").select(*cols).distinct()
    for col in time_columns:
        new_rows = with_literal_column(new_rows, col, None)
    new_rows = _align_to_table_schema(new_rows, df)

    non_null_rows = filter_sql(df, f"{VALUE_COLUMN} IS NOT NULL")
    # union_all preserves the pre-Ibis explicit ``SELECT … UNION ALL SELECT …``
    # SQL that this function was migrated from; non_null_rows can have legitimate
    # duplicate value rows (same dimensions, different time slices) and distinct
    # semantics would silently drop them.
    unioned = union_all(non_null_rows, new_rows)
    return unioned.select(*ids, variable_column, VALUE_COLUMN)


def convert_types_if_necessary(df: ibis.Table) -> ibis.Table:
    """Convert the types of the dataframe if necessary."""
    allowed_int_columns = (
        DimensionType.MODEL_YEAR.value,
        DimensionType.WEATHER_YEAR.value,
    )
    existing_columns = set(df.columns)
    columns_to_cast = [column for column in allowed_int_columns if column in existing_columns]
    if not columns_to_cast:
        return df

    return df.mutate(**{col: df[col].cast("string") for col in columns_to_cast})


def merge_expected_associations_tables(
    expected_dfs: dict[str, ibis.Table],
    all_dim_records: dict[str, list[str]],
    context: ScratchDirContext,
) -> ibis.Table:
    """Merge user-provided expected association tables into a single Ibis table.

    Tables are combined according to their column sets:

    - **Identical** column sets are unioned (different subsets of the same
      dimension space).
    - **Disjoint** column sets are cross-joined (each table constrains
      independent dimensions).
    - **Overlapping but not identical** column sets are inner-joined on the
      shared columns (each table further constrains the other).

    After each inner join the function verifies that no values of the shared
    dimension columns were lost.  This catches inconsistent tables early and
    the error message identifies which pair of tables caused the problem.

    After merging, any dimension columns not covered by any table are filled
    in by cross-joining with the full set of records for those dimensions.

    Parameters
    ----------
    expected_dfs
        Dictionary of Ibis tables with expected dimension combinations.
    all_dim_records
        Mapping from dimension column name to the complete list of record
        ids for that dimension (excluding TIME).
    context
        Scratch directory context for temporary files.

    Returns
    -------
    Ibis table
        A single Ibis table with one row per expected dimension combination.

    Raises
    ------
    DSGInvalidDataset
        If a dimension column loses records during the merge.
    """
    # Step 1: Group by column set; union tables with identical columns.
    groups: dict[frozenset[str], ibis.Table] = {}
    for df in expected_dfs.values():
        key = frozenset(df.columns)
        if key in groups:
            # union_all matches the pre-Ibis Spark .union() (UNION ALL) used
            # before this branch was migrated; expected-association tables
            # with identical column sets are supposed to be appended, and
            # any dedup happens later via cross-join expansion.
            groups[key] = union_all(groups[key], df)
        else:
            groups[key] = df

    assert groups, "Bug: expected_dfs is empty"

    # Step 2: Merge groups.
    # - Disjoint column sets   -> cross join
    # - Overlapping column sets -> inner join on the shared columns
    # Each group is checked on entry: every column that corresponds to a
    # known dimension must contain all of that dimension's records.
    # After each inner join, shared columns are re-checked to catch losses.
    merged: ibis.Table | None = None
    covered_columns: set[str] = set()
    merged_label: str = ""
    for col_set, df in groups.items():
        df = df.distinct()
        group_label = "{" + ", ".join(sorted(col_set)) + "}"

        # Validate that this group covers every record for its dimensions.
        df_cols = sorted(col_set)
        for col in df_cols:
            if col not in all_dim_records:
                msg = f"Unexpected dimension type in expected associations table with columns {group_label}: '{col}'"
                raise DSGFileInputError(msg)
        actual_ids_per_col = get_unique_values_per_column(df, df_cols)
        for col in df_cols:
            actual_ids = actual_ids_per_col[col]
            expected_ids = set(all_dim_records[col])
            missing = sorted_with_nulls(expected_ids - actual_ids)
            if missing:
                msg = (
                    f"Expected associations table with columns {group_label} is missing "
                    f"dimension '{col}' records: {missing}. Every record for a dimension "
                    f"must appear in at least one row of each table that contains that "
                    f"dimension column."
                )
                raise DSGInvalidDataset(msg)

        if merged is None:
            merged = df
            covered_columns = set(col_set)
            merged_label = group_label
        else:
            overlap = covered_columns & set(col_set)
            if overlap:
                # Collect pre-join distinct values for each side.
                covered_dim_cols = sorted(c for c in covered_columns if c in all_dim_records)
                set_dim_cols = sorted(c for c in col_set if c in all_dim_records)
                pre_join_values: dict[str, set] = {}
                for col, values in get_unique_values_per_column(merged, covered_dim_cols).items():
                    pre_join_values[col] = values
                for col, values in get_unique_values_per_column(df, set_dim_cols).items():
                    pre_join_values.setdefault(col, set()).update(values)

                merged = join_multiple_columns(merged, df, sorted(overlap), how="inner")

                overlap_dim_cols = sorted(c for c in overlap if c in all_dim_records)
                post_join_values = get_unique_values_per_column(merged, overlap_dim_cols)
                for col in overlap_dim_cols:
                    lost = sorted_with_nulls(
                        pre_join_values.get(col, set()) - post_join_values[col]
                    )
                    if lost:
                        msg = (
                            f"Inner join of expected associations tables with columns "
                            f"{merged_label} and {group_label} on {sorted(overlap)} "
                            f"dropped dimension '{col}' records: {lost}. "
                            f"Both tables must contain every record for shared dimensions."
                        )
                        raise DSGInvalidDataset(msg)
                merged_label = f"({merged_label} ⋈ {group_label})"
            else:
                merged = cross_join(merged, df)
                merged_label = f"({merged_label} × {group_label})"
            covered_columns |= set(col_set)

    assert merged is not None

    # Step 3: Cross-join with full records of any remaining uncovered dimensions.
    all_dim_columns = set(all_dim_records)
    remaining_columns = all_dim_columns - covered_columns
    if remaining_columns:
        remaining_data = {c: all_dim_records[c] for c in remaining_columns}
        remaining_df = create_dataframe_from_product(remaining_data, context)
        merged = cross_join(merged, remaining_df)

    return merged.distinct()


def filter_out_expected_missing_associations(
    main_df: ibis.Table, missing_df: ibis.Table
) -> ibis.Table:
    """Filter out rows that are expected to be missing from the main dataframe."""
    missing_columns = [DimensionType.from_column(x).value for x in missing_df.columns]
    main_view = create_temp_view(main_df)
    assoc_view = create_temp_view(missing_df)
    main_columns = ",".join((f"{main_view}.{x}" for x in main_df.columns))
    join_str = " AND ".join((f"{main_view}.{x} = {assoc_view}.{x}" for x in missing_columns))
    query = f"""
        SELECT {main_columns}
        FROM {main_view}
        ANTI JOIN {assoc_view}
        ON {join_str}
    """
    return get_runtime_backend().sql(query)


def split_expected_missing_rows(
    df: ibis.Table, time_columns: list[str]
) -> tuple[ibis.Table, ibis.Table | None]:
    """Split an Ibis table into two if it contains expected missing data."""
    null_df = filter_sql(df, f"{VALUE_COLUMN} IS NULL")
    if is_table_empty(null_df):
        return df, None

    columns_to_drop = time_columns + [VALUE_COLUMN]
    missing_associations = drop_columns(null_df, *columns_to_drop)
    return filter_sql(df, f"{VALUE_COLUMN} IS NOT NULL"), missing_associations


def _check_time_zones_are_declared(df: ibis.Table, time_dim: DateTimeDimensionConfig) -> None:
    """Reject time zones in the data that the time dimension does not declare.

    chronify localizes each row against the ``time_zones`` list in the time config and
    silently drops rows carrying any other zone, so an undeclared zone is data loss with
    no error. Catch it here instead.

    Raises
    ------
    DSGInvalidOperation
        If the ``time_zone`` column holds a value missing from the config's time zones.
    """
    declared = set(time_dim.get_time_zones())
    found = {tz for tz in get_unique_values(df, TIME_ZONE_COLUMN) if tz is not None}
    undeclared = found - declared
    if undeclared:
        msg = (
            f"The '{TIME_ZONE_COLUMN}' column holds time zone(s) {sorted(undeclared)} that the "
            f"time dimension does not declare. Its 'time_zones' list is {sorted(declared)}, and "
            "rows carrying any other zone would be dropped during localization. Add the missing "
            "zone(s) to 'time_zones', or correct the geography dimension records."
        )
        raise DSGInvalidOperation(msg)


def localize_timestamps_if_necessary(
    df: ibis.Table,
    config: DatasetConfig,
    scratch_dir_context: ScratchDirContext,
) -> tuple[ibis.Table, bool]:
    """Localize tz-naive timestamps to time zone(s) in the dataframe if necessary using Chronify.

    Timestamps will be localized if the time dimension has a localization plan.
    The localization plan will specify whether to localize to a single time zone
    or multiple time zones based on a time zone column for tz-naive timestamps.
    If the time dimension doesn't have a localization plan, the dataframe will be returned unchanged.

    Cross-backend contract
    ----------------------
    The shape of the localized time column differs by backend:

    - **DuckDB**: the time column becomes ``TIMESTAMP WITH TIME ZONE``
      carrying an explicit per-row TZ tag.
    - **Spark**: Spark's ``TIMESTAMP`` type cannot carry a per-row TZ tag.
      The instant is preserved (UTC microseconds), but column extractions
      such as ``.year()`` / ``.hour()`` interpret the timestamp in the
      session TZ. The instant is correct on both backends; the *displayed*
      time-of-day differs.

    Downstream callers that depend on a specific render TZ must either
    pin ``spark.sql.session.timeZone`` (e.g. via
    :func:`~dsgrid.ibis.tz.custom_time_zone`) or cast the column to
    ``timestamp('<tz>')`` before extracting.
    """
    time_dim = config.get_dimension(DimensionType.TIME)
    if not isinstance(time_dim, DateTimeDimensionConfig):
        msg = f"Only DateTimeDimensionConfig allowed for time zone localization. {time_dim.__class__.__name__}"
        raise DSGInvalidOperation(msg)

    localization_plan = time_dim.get_localization_plan()
    if not localization_plan:
        return df, False

    # This is a workaround for pivoted tables to still use Chronify, which only supports stacked tables.
    value_columns = config.get_value_columns()
    assert len(value_columns) > 0, value_columns
    value_column = next(iter(value_columns))

    match localization_plan:
        case "localize_to_single_tz":
            to_time_zone = time_dim.get_chronify_time_zone()
            match dsgrid.runtime_config.backend_engine:
                case BackendEngine.SPARK:
                    filename = persist_table(
                        df,
                        scratch_dir_context,
                        tag="dataset query before time zone localization",
                    )
                    df = localize_time_zone_with_chronify_runtime_path(
                        df=df,
                        filename=filename,
                        from_time_dim=time_dim,
                        time_zone=to_time_zone,
                        scratch_dir_context=scratch_dir_context,
                        value_column=value_column,
                    )
                case BackendEngine.DUCKDB:
                    df = localize_time_zone_with_chronify_duckdb(
                        df=df,
                        from_time_dim=time_dim,
                        time_zone=to_time_zone,
                        scratch_dir_context=scratch_dir_context,
                        value_column=value_column,
                    )
        case "localize_to_multi_tz":
            if TIME_ZONE_COLUMN not in df.columns:
                geo_dim = config.get_dimension(DimensionType.GEOGRAPHY)
                geo_dim = cast(DimensionBaseConfigWithFiles, geo_dim)
                df = add_time_zone(df, geo_dim)

            if is_table_empty(filter_sql(df, f"{TIME_ZONE_COLUMN} IS NOT NULL")):
                msg = (
                    f"The '{TIME_ZONE_COLUMN}' column is all null, or no rows matched "
                    f"the geography dimension records, after joining with them. The "
                    f"geography dimension records file must include a 'time_zone' "
                    f"column with valid IANA time zone values (e.g., 'Etc/GMT+5') for "
                    f"'aligned_in_std_clock_time' localization during registration. "
                    f"Registration always uses the dataset's own geography dimension, "
                    f"because no dimension mapping has been applied yet."
                )
                raise DSGInvalidOperation(msg)

            _check_time_zones_are_declared(df, time_dim)

            match dsgrid.runtime_config.backend_engine:
                case BackendEngine.SPARK:
                    filename = persist_table(
                        df,
                        scratch_dir_context,
                        tag="dataset query before time zone localization",
                    )
                    df = localize_time_zone_by_column_with_chronify_runtime_path(
                        df=df,
                        filename=filename,
                        from_time_dim=time_dim,
                        scratch_dir_context=scratch_dir_context,
                        value_column=value_column,
                    )
                case BackendEngine.DUCKDB:
                    df = localize_time_zone_by_column_with_chronify_duckdb(
                        df=df,
                        from_time_dim=time_dim,
                        scratch_dir_context=scratch_dir_context,
                        value_column=value_column,
                    )
        case _:
            msg = f"Unknown localization plan: {localization_plan}"
            raise DSGInvalidOperation(msg)

    return df, True
