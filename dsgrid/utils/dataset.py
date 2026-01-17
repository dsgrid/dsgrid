import logging
import os
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import ibis
import ibis.expr.types as ir

import chronify
from chronify.models import TableSchema

from dsgrid.chronify import create_store
from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.ibis_api import get_ibis_connection
from dsgrid.config.dimension_config import DimensionConfig
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
    DSGInvalidField,
    DSGInvalidDimensionMapping,
    DSGInvalidDataset,
)
from dsgrid.ibis_api import (
    check_for_nulls,
    is_dataframe_empty,
    make_temp_view_name,
    write_dataframe,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext

from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


def align_column_types(target_df: ir.Table, source_df: ir.Table) -> ir.Table:
    """Cast columns in source_df to match the types in target_df.

    Parameters
    ----------
    target_df : ir.Table
        The reference dataframe whose column types should be matched.
    source_df : ir.Table
        The dataframe whose columns will be cast if needed.

    Returns
    -------
    ir.Table
        The source_df with column types aligned to target_df.
    """
    mutations = {}
    for col in source_df.columns:
        if col in target_df.columns:
            target_type = target_df[col].type()
            source_type = source_df[col].type()
            if target_type != source_type:
                mutations[col] = source_df[col].cast(target_type)
    if mutations:
        return source_df.mutate(**mutations)
    return source_df


def map_stacked_dimension(
    df: ir.Table,
    records: ir.Table,
    column: str,
    drop_column: bool = True,
    to_column: str | None = None,
) -> ir.Table:
    to_column_ = to_column or column

    t_df = df
    t_records = records

    if "fraction" not in t_df.columns:
        t_df = t_df.mutate(fraction=ibis.literal(1.0))

    t_records = t_records.filter(t_records["to_id"].notnull())

    # Ensure compatible types for join: cast from_id to match the column type
    df_col_type = t_df[column].type()
    records_col_type = t_records["from_id"].type()
    if df_col_type != records_col_type:
        t_records = t_records.mutate(from_id=t_records["from_id"].cast(df_col_type))

    joined = t_df.join(t_records, t_df[column] == t_records["from_id"], how="left")
    try:
        total = t_df.count().to_pyarrow().as_py()
        unmatched = joined.filter(t_records["to_id"].isnull()).count().to_pyarrow().as_py()
        if unmatched > 0:
            print(
                f"DEBUG: map_stacked_dimension unmatched rows: {unmatched} / {total} for column {column}"
            )
            # joined.filter(t_records["to_id"].isnull()).select(column).limit(5).show() # Debug only
    except Exception as e:
        print(f"DEBUG: map_stacked_dimension debug failed: {e}")

    selection = []

    for col in t_df.columns:
        if col == "fraction":
            continue
        if drop_column and col == column:
            continue
        selection.append(t_df[col])

    selection.append(t_records["to_id"].name(to_column_))

    new_fraction = (t_df["fraction"] * t_records["from_fraction"]).name("fraction")
    selection.append(new_fraction)

    result = joined.select(selection)

    return result


def add_time_zone(
    load_data_df: ir.Table,
    geography_dim: DimensionConfig,
    df_key: str = "geography",
    dim_key: str = "id",
):
    """Add a time_zone column to a load_data dataframe from a geography dimension.

    Parameters
    ----------
    load_data_df : ir.Table
    geography_dim: DimensionConfig

    Returns
    -------
    ir.Table

    """
    geo_records = geography_dim.get_records_dataframe()
    # Ensure geo_records is an ibis table (it usually returns DataFrame currently, so we might need conversion if caller doesn't handle it, but we want to move towards ibis everywhere)
    # Ideally get_records_dataframe should return ibis table.
    # For now assuming it is or handled by add_column_from_records if we fix it there.
    # Wait, add_column_from_records below expects ibis tables now.

    # We might need to wrap geo_records if it's not a table.
    # But since we removed table_from_dataframe import, we assume inputs are Tables.
    # The caller (DatasetSchemaHandler) needs to ensure this.

    if df_key not in load_data_df.columns:
        msg = f"Cannot locate {df_key=} in load_data_df: {load_data_df.columns}"
        raise ValueError(msg)

    df = add_column_from_records(
        load_data_df, geo_records, "time_zone", df_key, record_key=dim_key
    )
    return df


def add_column_from_records(
    df: ir.Table, dimension_records: ir.Table, record_column, df_key, record_key: str = "id"
):
    t_df = df
    t_dim = dimension_records

    joined = t_df.join(t_dim, t_df[df_key] == t_dim[record_key], how="inner")

    # Select all columns from df and the desired record_column from dimension_records
    selection = [t_df[c] for c in t_df.columns]
    selection.append(t_dim[record_column])

    result = joined.select(selection)

    return result


def add_null_rows_from_load_data_lookup(df: ir.Table, lookup: ir.Table) -> ir.Table:
    """Add null rows from the nulled load data lookup table to data table.

    Parameters
    ----------
    df
        load data table
    lookup
        load data lookup table that has been filtered for nulls.
    """
    if lookup.count().execute() > 0:
        t_df = df
        t_lookup = lookup

        common_cols = list(set(t_lookup.columns).intersection(t_df.columns))

        t_lookup_common = t_lookup.select(common_cols)
        t_df_common = t_df.select(common_cols)

        # Find rows in lookup that are not in df
        null_rows_to_add = t_lookup_common.difference(t_df_common)

        # Add missing columns with null values, cast to correct type
        mutations = {}
        missing_cols = set(t_df.columns).difference(null_rows_to_add.columns)
        for col in missing_cols:
            target_type = t_df[col].type()
            mutations[col] = ibis.null().cast(target_type)

        null_rows_complete = null_rows_to_add.mutate(**mutations)

        # Ensure column order matches t_df for union
        null_rows_ordered = null_rows_complete.select(t_df.columns)

        result = t_df.union(null_rows_ordered)
        return result

    return df


def apply_scaling_factor(
    df: ir.Table,
    value_column: str,
    mapping_manager: DatasetMappingManager,
    scaling_factor_column: str = SCALING_FACTOR_COLUMN,
) -> ir.Table:
    """Apply the scaling factor to all value columns and then drop the scaling factor column."""
    op = mapping_manager.plan.apply_scaling_factor_op
    if mapping_manager.has_completed_operation(op):
        return df

    tbl = df
    val_col = tbl[value_column]
    scale_col = tbl[scaling_factor_column]

    # Logic matches existing: if scaling factor exists, multiply; else keep original value.
    new_val = ibis.ifelse(scale_col.notnull(), val_col * scale_col, val_col)

    tbl = tbl.mutate(**{value_column: new_val}).drop(scaling_factor_column)
    df = tbl

    if mapping_manager.plan.apply_scaling_factor_op.persist:
        df = mapping_manager.persist_table(df, op)
    return df


def check_historical_annual_time_model_year_consistency(
    df: ir.Table, time_column: str, model_year_column: str
) -> None:
    """Check that the model year values match the time dimension years for a historical
    dataset with an annual time dimension.
    """
    t_df = df

    invalid = (
        t_df.select([time_column, model_year_column])
        .filter(t_df[time_column].notnull())
        .distinct()
        .filter(t_df[time_column] != t_df[model_year_column])
    )

    # Check for any invalid rows
    res = invalid.limit(1).execute()

    if not res.empty:
        rows = res.to_dict("records")
        msg = (
            "A historical dataset with annual time must have rows where the time years match the model years. "
            f"{rows}"
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
    diff: ir.Table,
    dataset_table: ir.Table,
    dataset_id: str,
) -> None:
    """Record missing dimension record combinations in a Parquet file and log an error."""
    out_file = Path(f"{dataset_id}__missing_dimension_record_combinations.parquet")
    write_dataframe(diff, out_file, overwrite=True)
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
        _look_for_error_contributors(diff, dataset_table)
    except Exception as e:
        logger.warning("Failed to analyze missing data patterns: %s", e)

    msg = (
        f"Dataset {dataset_id} is missing required dimension records. "
        "Please look in the log file for more information."
    )
    raise DSGInvalidDataset(msg)


def _look_for_error_contributors(diff: ir.Table, dataset_table: ir.Table) -> None:
    diff_counts = {x: diff.select(x).distinct().count().execute() for x in diff.columns}
    for col in diff.columns:
        dataset_count = dataset_table.select(col).distinct().count().execute()
        if dataset_count != diff_counts[col]:
            logger.error(
                "Error contributor: column=%s dataset_distinct_count=%s missing_distinct_count=%s",
                col,
                dataset_count,
                diff_counts[col],
            )


def is_noop_mapping(records: ir.Table) -> bool:
    """Return True if the mapping is a no-op."""
    # Filter for rows that indicate a change (i.e. NOT a no-op row)
    # If any such rows exist, the mapping is NOT a no-op.
    # Note: is_dataframe_empty checks if count == 0.
    # So we want to return True if count of "change rows" is 0.

    # Conditions for a "change row":
    # 1. to_id is NULL but from_id is NOT NULL (deletion)
    # 2. to_id is NOT NULL but from_id is NULL (shouldn't happen in valid mapping but implies change)
    # 3. from_id != to_id (renaming/remapping)
    # 4. from_fraction != 1.0 (splitting)

    # Ibis expression
    cond = (
        (records["to_id"].isnull() & records["from_id"].notnull())
        | (records["to_id"].notnull() & records["from_id"].isnull())
        | (records["from_id"] != records["to_id"])
        | (records["from_fraction"] != 1.0)
    )

    return records.filter(cond).count().execute() == 0


def _create_memtable_in_dsgrid_connection(pandas_df) -> ir.Table:
    """Create a table in dsgrid's shared ibis connection.

    This ensures the table is accessible when used with other dsgrid operations
    that use the same shared connection, including chronify operations.
    """
    con = get_ibis_connection()
    table_name = make_temp_view_name()
    return con.create_table(table_name, pandas_df, overwrite=True)


def map_time_dimension_with_chronify(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
) -> ir.Table:
    """Create a time-mapped table with chronify.

    Uses the appropriate backend (Spark or DuckDB) based on dsgrid's runtime configuration.
    For Spark, uses the existing spark session. For DuckDB, uses dsgrid's shared ibis connection.

    Parameters
    ----------
    df : ir.Table
        Input ibis table to map.
    value_column : str
        Name of the value column.
    from_time_dim : TimeDimensionBaseConfig
        Source time dimension configuration.
    to_time_dim : TimeDimensionBaseConfig
        Target time dimension configuration.
    scratch_dir_context : ScratchDirContext
        Context for temporary file storage.
    wrap_time_allowed : bool, optional
        Whether time wrapping is allowed, by default False.
    time_based_data_adjustment : TimeBasedDataAdjustmentModel | None, optional
        Adjustment settings for time-based data, by default None.

    Returns
    -------
    ir.Table
        The time-mapped ibis table.
    """
    # Materialize input to avoid stale memtable references
    df = _create_memtable_in_dsgrid_connection(df.to_pandas())
    src_schema, dst_schema = _get_mapping_schemas(df, value_column, from_time_dim, to_time_dim)
    store_file = scratch_dir_context.get_temp_filename(suffix=".db")
    with create_store(store_file) as store:
        store.create_view(src_schema, df, bypass_checks=True)
        store.map_table_time_config(
            src_schema.name,
            dst_schema,
            wrap_time_allowed=wrap_time_allowed,
            data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
            scratch_dir=scratch_dir_context.scratch_dir,
        )
        result = store.get_table(dst_schema.name)
        # Force execution to materialize the result before the store context exits
        # Use dsgrid's connection to ensure the memtable is accessible
        result = _create_memtable_in_dsgrid_connection(result.to_pandas())
        store.drop_view(src_schema.name)
        store.drop_table(dst_schema.name)
    return result


def convert_time_zone_with_chronify(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: str,
    scratch_dir_context: ScratchDirContext,
) -> ir.Table:
    """Create a single time zone-converted table with chronify.

    Uses the appropriate backend (Spark or DuckDB) based on dsgrid's runtime configuration.

    Parameters
    ----------
    df : ir.Table
        Input ibis table to convert.
    value_column : str
        Name of the value column.
    from_time_dim : TimeDimensionBaseConfig
        Source time dimension configuration.
    time_zone : str
        Target time zone string (e.g., "America/New_York").
    scratch_dir_context : ScratchDirContext
        Context for temporary file storage.

    Returns
    -------
    ir.Table
        The time zone-converted ibis table.
    """
    # Materialize input to avoid stale memtable references
    df = _create_memtable_in_dsgrid_connection(df.to_pandas())
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store_file = scratch_dir_context.get_temp_filename(suffix=".db")
    with create_store(store_file) as store:
        store.create_view(src_schema, df, bypass_checks=True)
        zone_info_tz = ZoneInfo(time_zone)
        dst_schema = store.convert_time_zone(
            src_schema.name,
            zone_info_tz,
            scratch_dir=scratch_dir_context.scratch_dir,
        )
        result = store.get_table(dst_schema.name)
        # Force execution to materialize the result before the store context exits
        # Use dsgrid's connection to ensure the memtable is accessible
        result = _create_memtable_in_dsgrid_connection(result.to_pandas())
        store.drop_view(src_schema.name)
        store.drop_table(dst_schema.name)
    return result


def convert_time_zone_by_column_with_chronify(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone_column: str,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
) -> ir.Table:
    """Create a multiple time zone-converted table (based on a time_zone_column) using chronify.

    Uses the appropriate backend (Spark or DuckDB) based on dsgrid's runtime configuration.

    Parameters
    ----------
    df : ir.Table
        Input ibis table to convert.
    value_column : str
        Name of the value column.
    from_time_dim : TimeDimensionBaseConfig
        Source time dimension configuration.
    time_zone_column : str
        Name of the column containing time zone information per row.
    scratch_dir_context : ScratchDirContext
        Context for temporary file storage.
    wrap_time_allowed : bool, optional
        Whether time wrapping is allowed, by default False.

    Returns
    -------
    ir.Table
        The time zone-converted ibis table.
    """
    # Materialize input to avoid stale memtable references
    df = _create_memtable_in_dsgrid_connection(df.to_pandas())
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store_file = scratch_dir_context.get_temp_filename(suffix=".db")
    with create_store(store_file) as store:
        store.create_view(src_schema, df, bypass_checks=True)
        dst_schema = store.convert_time_zone_by_column(
            src_schema.name,
            time_zone_column,
            wrap_time_allowed=wrap_time_allowed,
            scratch_dir=scratch_dir_context.scratch_dir,
        )
        result = store.get_table(dst_schema.name)
        # Force execution to materialize the result before the store context exits
        # Use dsgrid's connection to ensure the memtable is accessible
        result = _create_memtable_in_dsgrid_connection(result.to_pandas())
        store.drop_view(src_schema.name)
        store.drop_table(dst_schema.name)
    return result


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
        leap_day_adjustment=adj.leap_day_adjustment.value,
        daylight_saving_adjustment=chronify_dst_adjustment,
    )


def _get_src_schema(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    src_name: str | None = None,
) -> TableSchema:
    src = src_name or "src_" + make_temp_view_name()
    time_col_list = from_time_dim.get_load_data_time_columns()
    time_config = from_time_dim.to_chronify()
    time_array_id_columns = [
        x
        for x in df.columns
        if x in set(df.columns).difference(set(time_col_list)) - {value_column}
    ]
    src_schema = chronify.TableSchema(
        name=src,
        time_config=time_config,
        time_array_id_columns=time_array_id_columns,
        value_column=value_column,
    )
    return src_schema


def _get_dst_schema(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
) -> TableSchema:
    time_config = to_time_dim.to_chronify()
    time_col_list = from_time_dim.get_load_data_time_columns()
    time_array_id_columns = [
        x
        for x in df.columns
        if x in set(df.columns).difference(set(time_col_list)) - {value_column}
    ]
    dst_schema = chronify.TableSchema(
        name="dst_" + make_temp_view_name(),
        time_config=time_config,
        time_array_id_columns=time_array_id_columns,
        value_column=value_column,
    )
    return dst_schema


def _get_mapping_schemas(
    df: ir.Table,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    src_name: str | None = None,
) -> tuple[TableSchema, TableSchema]:
    src_schema = _get_src_schema(df, value_column, from_time_dim, src_name=src_name)
    dst_schema = _get_dst_schema(df, value_column, from_time_dim, to_time_dim)
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
    stacked = list(stacked_columns)

    t_df = df

    # Count distinct time values per group
    counts = t_df.group_by(stacked).aggregate(count_time=t_df[time_column].nunique())

    joined = t_df.join(counts, stacked)

    # Keep rows where time is present OR no time values exist for this group (all nulls)
    filtered = joined.filter(joined[time_column].notnull() | (joined["count_time"] == 0))

    result = filtered.select(t_df.columns)

    return result


@track_timing(timer_stats_collector)
def repartition_if_needed_by_mapping(
    df: ir.Table,
    mapping_type: DimensionMappingType,
    scratch_dir_context: ScratchDirContext,
    repartition: bool | None = None,
) -> tuple[ir.Table, Path | None]:
    """Repartition the dataframe if the mapping might cause data skew.

    Parameters
    ----------
    df : ir.Table
        The dataframe to repartition.
    mapping_type : DimensionMappingType
    scratch_dir_context : ScratchDirContext
        The scratch directory context to use for temporary files.
    repartition : bool
        If None, repartition based on the mapping type.
        Otherwise, always repartition if True, or never if False.
    """
    # Spark-specific repartitioning logic is removed as part of Ibis migration.
    # Return table as is.
    return df, None


def unpivot_dataframe(
    df: ir.Table,
    value_columns: Iterable[str],
    variable_column: str,
    time_columns: list[str],
) -> ir.Table:
    """Unpivot the dataframe, accounting for time columns."""
    t_df = df
    values_list = list(value_columns)

    unpivoted = t_df.pivot_longer(values_list, names_to=variable_column, values_to=VALUE_COLUMN)

    # Handle NULL values: coalesce rows with NULL values into a single row with NULL time
    null_rows = unpivoted.filter(unpivoted[VALUE_COLUMN].isnull())

    # Select distinct dimension columns (excluding time)
    exclude_cols = set(time_columns + [VALUE_COLUMN])
    keep_cols = [c for c in unpivoted.columns if c not in exclude_cols]

    new_rows = null_rows.select(keep_cols).distinct()

    # Add back time columns and value column as NULL
    mutations = {col: ibis.null().cast(unpivoted[col].type()) for col in time_columns}
    mutations[VALUE_COLUMN] = ibis.null().cast(unpivoted[VALUE_COLUMN].type())

    new_rows = new_rows.mutate(**mutations).select(unpivoted.columns)

    valid_rows = unpivoted.filter(unpivoted[VALUE_COLUMN].notnull())

    result = valid_rows.union(new_rows)

    # Reorder columns to match expectations: ids, variable, value
    ids = [c for c in result.columns if c not in (variable_column, VALUE_COLUMN)]
    result = result.select(ids + [variable_column, VALUE_COLUMN])

    return result


def convert_types_if_necessary(df: ir.Table) -> ir.Table:
    """Convert the types of the dataframe if necessary.

    Ensures dimension columns and join keys have consistent types across
    different file formats (e.g., CSV with inferSchema=False produces strings,
    parquet preserves original types).
    """
    t_df = df
    # Columns that should be converted to string if they are integers.
    # This includes dimension columns that may have leading zeros (e.g., geography codes)
    # and join keys like 'id'.
    columns_to_stringify = (
        DimensionType.MODEL_YEAR.value,
        DimensionType.WEATHER_YEAR.value,
        "id",  # Join key used in TWO_TABLE format
    )

    mutations = {}
    schema = t_df.schema()
    for column in columns_to_stringify:
        if column in schema:
            dtype = schema[column]
            # Convert integers and floats to string.
            # Floats need conversion because pandas can convert int columns to float
            # when there are NULL values (since int doesn't support NULL in pandas).
            if dtype.is_integer() or dtype.is_floating():
                mutations[column] = t_df[column].cast("int64").cast("string")

    if mutations:
        t_df = t_df.mutate(**mutations)
        return t_df

    return df


def filter_out_expected_missing_associations(main_df: ir.Table, missing_df: ir.Table) -> ir.Table:
    """Filter out rows that are expected to be missing from the main dataframe."""
    t_main = main_df
    t_missing = missing_df

    # Ensure columns match expected dimension types
    missing_columns = [DimensionType.from_column(x).value for x in t_missing.columns]

    # Ensure compatible types for anti-join
    mutations = {}
    for col in missing_columns:
        if col in t_main.columns and col in t_missing.columns:
            main_type = t_main[col].type()
            missing_type = t_missing[col].type()
            if main_type != missing_type:
                mutations[col] = t_missing[col].cast(main_type)
    if mutations:
        t_missing = t_missing.mutate(**mutations)

    # Anti join to remove rows present in missing_df
    result = t_main.anti_join(t_missing, missing_columns)

    return result


def split_expected_missing_rows(
    df: ir.Table, time_columns: list[str]
) -> tuple[ir.Table, ir.Table | None]:
    """Split a DataFrame into two if it contains expected missing data."""
    null_df = df.filter(df[VALUE_COLUMN].isnull())
    if is_dataframe_empty(null_df):
        return df, None

    drop_columns = time_columns + [VALUE_COLUMN]
    missing_associations = null_df.drop(*drop_columns)
    return df.filter(df[VALUE_COLUMN].notnull()), missing_associations
