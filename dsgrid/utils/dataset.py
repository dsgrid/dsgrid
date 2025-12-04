import logging
import os
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo
import pyspark

import chronify
from chronify.models import TableSchema

import dsgrid
from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    DaylightSavingFallBackType,
    DaylightSavingSpringForwardType,
    TimeBasedDataAdjustmentModel,
    TimeZone,
)
from dsgrid.exceptions import (
    DSGInvalidField,
    DSGInvalidDimensionMapping,
    DSGInvalidDataset,
)
from dsgrid.spark.functions import (
    coalesce,
    count_distinct_on_group_by,
    create_temp_view,
    handle_column_spaces,
    make_temp_view_name,
    read_parquet,
    is_dataframe_empty,
    join,
    join_multiple_columns,
    unpivot,
)
from dsgrid.spark.functions import except_all, get_spark_session
from dsgrid.spark.types import (
    DataFrame,
    F,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    use_duckdb,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    check_for_nulls,
    write_dataframe,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


def map_stacked_dimension(
    df: DataFrame,
    records: DataFrame,
    column: str,
    drop_column: bool = True,
    to_column: str | None = None,
) -> DataFrame:
    to_column_ = to_column or column
    if "fraction" not in df.columns:
        df = df.withColumn("fraction", F.lit(1.0))
    # map and consolidate from_fraction only
    records = records.filter("to_id IS NOT NULL")
    df = join(df, records, column, "from_id", how="inner").drop("from_id")
    if drop_column:
        df = df.drop(column)
    df = df.withColumnRenamed("to_id", to_column_)
    nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
    df = df.select(
        *nonfraction_cols,
        (F.col("fraction") * F.col("from_fraction")).alias("fraction"),
    )
    return df


def add_time_zone(
    load_data_df: pyspark.sql.DataFrame,
    geography_dim: DimensionConfig,
    df_key: str = "geography",
    dim_key: str = "id",
):
    """Add a time_zone column to a load_data dataframe from a geography dimension.

    Parameters
    ----------
    load_data_df : pyspark.sql.DataFrame
    geography_dim: DimensionConfig

    Returns
    -------
    pyspark.sql.DataFrame

    """
    spark = get_spark_session()
    dsg_geo_records = geography_dim.get_records_dataframe()
    tz_map_table = spark.createDataFrame(
        [(x.value, x.tz_name) for x in TimeZone], ("dsgrid_name", "tz_name")
    )
    geo_records = (
        join(dsg_geo_records, tz_map_table, "time_zone", "dsgrid_name")
        .drop("time_zone", "dsgrid_name")
        .withColumnRenamed("tz_name", "time_zone")
    )
    assert dsg_geo_records.count() == geo_records.count()
    if df_key not in load_data_df.columns:
        msg = f"Cannot locate {df_key=} in load_data_df: {load_data_df.columns}"
        raise ValueError(msg)

    df = add_column_from_records(
        load_data_df, geo_records, "time_zone", df_key, record_key=dim_key
    )
    return df


def add_column_from_records(df, dimension_records, record_column, df_key, record_key: str = "id"):
    df = join(
        df1=df,
        df2=dimension_records.select(F.col(record_key).alias("record_id"), record_column),
        column1=df_key,
        column2="record_id",
        how="inner",
    ).drop("record_id")
    return df


def add_null_rows_from_load_data_lookup(df: DataFrame, lookup: DataFrame) -> DataFrame:
    """Add null rows from the nulled load data lookup table to data table.

    Parameters
    ----------
    df
        load data table
    lookup
        load data lookup table that has been filtered for nulls.
    """
    if not is_dataframe_empty(lookup):
        intersect_cols = set(lookup.columns).intersection(df.columns)
        null_rows_to_add = except_all(lookup.select(*intersect_cols), df.select(*intersect_cols))
        for col in set(df.columns).difference(null_rows_to_add.columns):
            null_rows_to_add = null_rows_to_add.withColumn(col, F.lit(None))
        df = df.union(null_rows_to_add.select(*df.columns))

    return df


def apply_scaling_factor(
    df: DataFrame,
    value_column: str,
    mapping_manager: DatasetMappingManager,
    scaling_factor_column: str = SCALING_FACTOR_COLUMN,
) -> DataFrame:
    """Apply the scaling factor to all value columns and then drop the scaling factor column."""
    op = mapping_manager.plan.apply_scaling_factor_op
    if mapping_manager.has_completed_operation(op):
        return df

    func = _apply_scaling_factor_duckdb if use_duckdb() else _apply_scaling_factor_spark
    df = func(df, value_column, scaling_factor_column)
    if mapping_manager.plan.apply_scaling_factor_op.persist:
        df = mapping_manager.persist_table(df, op)
    return df


def _apply_scaling_factor_duckdb(
    df: DataFrame,
    value_column: str,
    scaling_factor_column: str,
):
    # Workaround for the fact that duckdb doesn't support
    # F.col(scaling_factor_column).isNotNull()
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
    spark = get_spark_session()
    return spark.sql(query)


def _apply_scaling_factor_spark(
    df: DataFrame,
    value_column: str,
    scaling_factor_column: str,
):
    return df.withColumn(
        value_column,
        F.when(
            F.col(scaling_factor_column).isNotNull(),
            F.col(value_column) * F.col(scaling_factor_column),
        ).otherwise(F.col(value_column)),
    ).drop(scaling_factor_column)


def check_historical_annual_time_model_year_consistency(
    df: DataFrame, time_column: str, model_year_column: str
) -> None:
    """Check that the model year values match the time dimension years for a historical
    dataset with an annual time dimension.
    """
    invalid = (
        df.select(time_column, model_year_column)
        .filter(f"{time_column} IS NOT NULL")
        .distinct()
        .filter(f"{time_column} != {model_year_column}")
        .collect()
    )
    if invalid:
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
    diff: DataFrame,
    dataset_table: DataFrame,
    dataset_id: str,
) -> None:
    """Record missing dimension record combinations in a Parquet file and log an error."""
    out_file = Path(f"{dataset_id}__missing_dimension_record_combinations.parquet")
    df = diff
    changed = False
    for column in diff.columns:
        if diff.select(column).distinct().count() == 1:
            df = df.drop(column)
            changed = True
    if changed:
        df = df.distinct()
    df = write_dataframe(coalesce(df, 1), out_file, overwrite=True)
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
        _look_for_error_contributors(df, dataset_table)
    except Exception as e:
        logger.warning("Failed to analyze missing data patterns: %s", e)

    msg = (
        f"Dataset {dataset_id} is missing required dimension records. "
        "Please look in the log file for more information."
    )
    raise DSGInvalidDataset(msg)


def _look_for_error_contributors(diff: DataFrame, dataset_table: DataFrame) -> None:
    diff_counts = {x: diff.select(x).distinct().count() for x in diff.columns}
    for col in diff.columns:
        dataset_count = dataset_table.select(col).distinct().count()
        if dataset_count != diff_counts[col]:
            logger.error(
                "Error contributor: column=%s dataset_distinct_count=%s missing_distinct_count=%s",
                col,
                dataset_count,
                diff_counts[col],
            )


def is_noop_mapping(records: DataFrame) -> bool:
    """Return True if the mapping is a no-op."""
    return is_dataframe_empty(
        records.filter(
            "(to_id IS NULL and from_id IS NOT NULL) or "
            "(to_id IS NOT NULL and from_id IS NULL) or "
            "(from_id != to_id) or (from_fraction != 1.0)"
        )
    )


def map_time_dimension_with_chronify_duckdb(
    df: DataFrame,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
) -> DataFrame:
    """Create a time-mapped table with chronify and DuckDB.
    All operations are performed in memory.
    """
    # This will only work if the source and destination tables will fit in memory.
    # We could potentially use a file-based DuckDB database for larger-than memory datasets.
    # However, time checks and unpivot operations have failed with out-of-memory errors,
    # and so we have never reached this point.
    # If we solve those problems, this code could be modified.
    src_schema, dst_schema = _get_mapping_schemas(df, value_column, from_time_dim, to_time_dim)
    store = chronify.Store.create_in_memory_db()
    store.ingest_table(df.relation, src_schema, skip_time_checks=True)
    store.map_table_time_config(
        src_schema.name,
        dst_schema,
        wrap_time_allowed=wrap_time_allowed,
        data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
        scratch_dir=scratch_dir_context.scratch_dir,
    )
    pandas_df = store.read_table(dst_schema.name)
    store.drop_table(dst_schema.name)
    return df.session.createDataFrame(pandas_df)


def convert_time_zone_with_chronify_duckdb(
    df: DataFrame,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: str | TimeZone,
    scratch_dir_context: ScratchDirContext,
) -> DataFrame:
    """Create a single time zone-converted table with chronify and DuckDB.
    All operations are performed in memory.
    """
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_in_memory_db()
    store.ingest_table(df.relation, src_schema, skip_time_checks=True)
    zone_info_tz = time_zone.tz if isinstance(time_zone, TimeZone) else ZoneInfo(time_zone)
    dst_schema = store.convert_time_zone(
        src_schema.name,
        zone_info_tz,
        scratch_dir=scratch_dir_context.scratch_dir,
    )
    pandas_df = store.read_table(dst_schema.name)
    store.drop_table(dst_schema.name)
    return df.session.createDataFrame(pandas_df)


def convert_time_zone_by_column_with_chronify_duckdb(
    df: DataFrame,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone_column: str,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
) -> DataFrame:
    """Create a multiple time zone-converted table (based on a time_zone_column)
    using chronify and DuckDB.
    All operations are performed in memory.
    """
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_in_memory_db()
    store.ingest_table(df.relation, src_schema, skip_time_checks=True)
    dst_schema = store.convert_time_zone_by_column(
        src_schema.name,
        time_zone_column,
        wrap_time_allowed=wrap_time_allowed,
        scratch_dir=scratch_dir_context.scratch_dir,
    )
    pandas_df = store.read_table(dst_schema.name)
    store.drop_table(dst_schema.name)
    return df.session.createDataFrame(pandas_df)


def map_time_dimension_with_chronify_spark_hive(
    df: DataFrame,
    table_name: str,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
    wrap_time_allowed: bool = False,
) -> DataFrame:
    """Create a time-mapped table with chronify and Spark and a Hive Metastore.
    The source data must already be stored in the metastore.
    Chronify will store the mapped table in the metastore.
    """
    src_schema, dst_schema = _get_mapping_schemas(
        df, value_column, from_time_dim, to_time_dim, src_name=table_name
    )
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    with store.engine.begin() as conn:
        # This bypasses checks because the table should already be valid.
        store.schema_manager.add_schema(conn, src_schema)
    try:
        store.map_table_time_config(
            src_schema.name,
            dst_schema,
            check_mapped_timestamps=False,
            scratch_dir=scratch_dir_context.scratch_dir,
            wrap_time_allowed=wrap_time_allowed,
            data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
        )
    finally:
        with store.engine.begin() as conn:
            store.schema_manager.remove_schema(conn, src_schema.name)

    return df.sparkSession.sql(f"SELECT * FROM {dst_schema.name}")


def convert_time_zone_with_chronify_spark_hive(
    df: DataFrame,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: str | TimeZone,
    scratch_dir_context: ScratchDirContext,
) -> DataFrame:
    """Create a single time zone-converted table with chronify and Spark and a Hive Metastore."""
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    with store.engine.begin() as conn:
        # This bypasses checks because the table should already be valid.
        store.schema_manager.add_schema(conn, src_schema)
    zone_info_tz = time_zone.tz if isinstance(time_zone, TimeZone) else ZoneInfo(time_zone)
    try:
        dst_schema = store.convert_time_zone(
            src_schema.name,
            zone_info_tz,
            scratch_dir=scratch_dir_context.scratch_dir,
        )
    finally:
        with store.engine.begin() as conn:
            store.schema_manager.remove_schema(conn, src_schema.name)

    return df.sparkSession.sql(f"SELECT * FROM {dst_schema.name}")


def convert_time_zone_by_column_with_chronify_spark_hive(
    df: DataFrame,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone_column: str,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
) -> DataFrame:
    """Create a multiple time zone-converted table (based on a time_zone_column)
    using chronify and Spark and a Hive Metastore.
    """
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    with store.engine.begin() as conn:
        # This bypasses checks because the table should already be valid.
        store.schema_manager.add_schema(conn, src_schema)
    try:
        dst_schema = store.convert_time_zone_by_column(
            src_schema.name,
            time_zone_column,
            wrap_time_allowed=wrap_time_allowed,
            scratch_dir=scratch_dir_context.scratch_dir,
        )
    finally:
        with store.engine.begin() as conn:
            store.schema_manager.remove_schema(conn, src_schema.name)

    return df.sparkSession.sql(f"SELECT * FROM {dst_schema.name}")


def map_time_dimension_with_chronify_spark_path(
    df: DataFrame,
    filename: Path,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    to_time_dim: TimeDimensionBaseConfig,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
    time_based_data_adjustment: TimeBasedDataAdjustmentModel | None = None,
) -> DataFrame:
    """Create a time-mapped table with chronify and Spark using the local filesystem.
    Chronify will store the mapped table in a Parquet file within scratch_dir_context.
    """
    src_schema, dst_schema = _get_mapping_schemas(df, value_column, from_time_dim, to_time_dim)
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    store.map_table_time_config(
        src_schema.name,
        dst_schema,
        check_mapped_timestamps=False,
        scratch_dir=scratch_dir_context.scratch_dir,
        output_file=output_file,
        wrap_time_allowed=wrap_time_allowed,
        data_adjustment=_to_chronify_time_based_data_adjustment(time_based_data_adjustment),
    )
    return df.sparkSession.read.load(str(output_file))


def convert_time_zone_with_chronify_spark_path(
    df: DataFrame,
    filename: Path,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone: str | TimeZone,
    scratch_dir_context: ScratchDirContext,
) -> DataFrame:
    """Create a single time zone-converted table with chronify and Spark using the local filesystem."""
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    zone_info_tz = time_zone.tz if isinstance(time_zone, TimeZone) else ZoneInfo(time_zone)
    store.convert_time_zone(
        src_schema.name,
        zone_info_tz,
        scratch_dir=scratch_dir_context.scratch_dir,
        output_file=output_file,
    )
    return df.sparkSession.read.load(str(output_file))


def convert_time_zone_by_column_with_chronify_spark_path(
    df: DataFrame,
    filename: Path,
    value_column: str,
    from_time_dim: TimeDimensionBaseConfig,
    time_zone_column: str,
    scratch_dir_context: ScratchDirContext,
    wrap_time_allowed: bool = False,
) -> DataFrame:
    """Create a multiple time zone-converted table (based on a time_zone_column)
    using chronify and Spark using the local filesystem.
    """
    src_schema = _get_src_schema(df, value_column, from_time_dim)
    store = chronify.Store.create_new_hive_store(dsgrid.runtime_config.thrift_server_url)
    store.create_view_from_parquet(filename, src_schema, bypass_checks=True)
    output_file = scratch_dir_context.get_temp_filename(suffix=".parquet")
    store.convert_time_zone_by_column(
        src_schema.name,
        time_zone_column,
        wrap_time_allowed=wrap_time_allowed,
        scratch_dir=scratch_dir_context.scratch_dir,
        output_file=output_file,
    )
    return df.sparkSession.read.load(str(output_file))


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
    df: DataFrame,
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
    df: DataFrame,
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
    df: DataFrame,
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
    orig_columns = df.columns
    stacked = list(stacked_columns)
    return (
        join_multiple_columns(
            df,
            count_distinct_on_group_by(df, stacked, time_column, "count_time"),
            stacked,
        )
        .filter(f"{handle_column_spaces(time_column)} IS NOT NULL OR count_time = 0")
        .select(orig_columns)
    )


@track_timing(timer_stats_collector)
def repartition_if_needed_by_mapping(
    df: DataFrame,
    mapping_type: DimensionMappingType,
    scratch_dir_context: ScratchDirContext,
    repartition: bool | None = None,
) -> tuple[DataFrame, Path | None]:
    """Repartition the dataframe if the mapping might cause data skew.

    Parameters
    ----------
    df : DataFrame
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
        spark = get_spark_session()
        num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
        df.withColumn(
            salted_column, (F.rand() * num_partitions).cast(IntegerType()) + 1
        ).repartition(salted_column).write.parquet(str(filename))
        df = read_parquet(filename).drop(salted_column)
        logger.info("Completed repartition.")
        return df, filename

    logger.debug("Repartition is not needed for mapping_type %s", mapping_type)
    return df, None


def unpivot_dataframe(
    df: DataFrame,
    value_columns: Iterable[str],
    variable_column: str,
    time_columns: list[str],
) -> DataFrame:
    """Unpivot the dataframe, accounting for time columns."""
    values = value_columns if isinstance(value_columns, set) else set(value_columns)
    ids = [x for x in df.columns if x != VALUE_COLUMN and x not in values]
    df = unpivot(df, value_columns, variable_column, VALUE_COLUMN)
    cols = set(df.columns).difference(time_columns)
    new_rows = df.filter(f"{VALUE_COLUMN} IS NULL").select(*cols).distinct()
    for col in time_columns:
        new_rows = new_rows.withColumn(col, F.lit(None))

    return (
        df.filter(f"{VALUE_COLUMN} IS NOT NULL")
        .union(new_rows.select(*df.columns))
        .select(*ids, variable_column, VALUE_COLUMN)
    )


def convert_types_if_necessary(df: DataFrame) -> DataFrame:
    """Convert the types of the dataframe if necessary."""
    allowed_int_columns = (
        DimensionType.MODEL_YEAR.value,
        DimensionType.WEATHER_YEAR.value,
    )
    int_types = {IntegerType(), LongType(), ShortType()}
    existing_columns = set(df.columns)
    for column in allowed_int_columns:
        if column in existing_columns and df.schema[column].dataType in int_types:
            df = df.withColumn(column, F.col(column).cast(StringType()))
    return df


def filter_out_expected_missing_associations(
    main_df: DataFrame, missing_df: DataFrame
) -> DataFrame:
    """Filter out rows that are expected to be missing from the main dataframe."""
    missing_columns = [DimensionType.from_column(x).value for x in missing_df.columns]
    spark = get_spark_session()
    main_view = make_temp_view_name()
    assoc_view = make_temp_view_name()
    main_columns = ",".join((f"{main_view}.{x}" for x in main_df.columns))

    main_df.createOrReplaceTempView(main_view)
    missing_df.createOrReplaceTempView(assoc_view)
    join_str = " AND ".join((f"{main_view}.{x} = {assoc_view}.{x}" for x in missing_columns))
    query = f"""
        SELECT {main_columns}
        FROM {main_view}
        ANTI JOIN {assoc_view}
        ON {join_str}
    """
    res = spark.sql(query)
    return res


def split_expected_missing_rows(
    df: DataFrame, time_columns: list[str]
) -> tuple[DataFrame, DataFrame | None]:
    """Split a DataFrame into two if it contains expected missing data."""
    null_df = df.filter(f"{VALUE_COLUMN} IS NULL")
    if is_dataframe_empty(null_df):
        return df, None

    drop_columns = time_columns + [VALUE_COLUMN]
    missing_associations = null_df.drop(*drop_columns)
    return df.filter(f"{VALUE_COLUMN} IS NOT NULL"), missing_associations
