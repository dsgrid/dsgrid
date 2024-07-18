import logging
import os
from typing import Iterable

import pyspark
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from dsgrid.common import SCALING_FACTOR_COLUMN, VALUE_COLUMN
from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimensionMapping, DSGInvalidDataset
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import check_for_nulls, read_parquet
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


def map_and_reduce_stacked_dimension(df, records, column, drop_column=True, to_column=None):
    to_column_ = to_column or column
    if "fraction" not in df.columns:
        df = df.withColumn("fraction", F.lit(1.0))
    # map and consolidate from_fraction only
    records = records.filter("to_id IS NOT NULL")

    df = df.join(records, on=df[column] == records.from_id, how="inner").drop("from_id")
    if drop_column:
        df = df.drop(column)
    df = df.withColumnRenamed("to_id", to_column_)
    nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
    df = df.fillna(1.0, subset=["from_fraction"]).selectExpr(
        *nonfraction_cols, "fraction*from_fraction AS fraction"
    )
    return df


def add_time_zone(load_data_df, geography_dim):
    """Add a time_zone column to a load_data dataframe from a geography dimension.

    Parameters
    ----------
    load_data_df : pyspark.sql.DataFrame
    geography_dim: DimensionConfig

    Returns
    -------
    pyspark.sql.DataFrame

    """
    geo_records = geography_dim.get_records_dataframe()
    geo_name = geography_dim.model.dimension_type.value
    assert "time_zone" not in load_data_df.columns
    return add_column_from_records(load_data_df, geo_records, geo_name, "time_zone")


def add_column_from_records(df, dimension_records, dimension_name, column_to_add):
    df = df.join(
        dimension_records.select(F.col("id").alias("record_id"), column_to_add),
        on=F.col(dimension_name) == F.col("record_id"),
        how="inner",
    ).drop("record_id")
    return df


def apply_scaling_factor(
    df: DataFrame, value_columns, scaling_factor_column=SCALING_FACTOR_COLUMN
) -> DataFrame:
    """Apply the scaling factor to all value columns and then drop the scaling factor column."""
    for column in value_columns:
        df = df.withColumn(
            column,
            F.when(
                F.col(scaling_factor_column).isNotNull(),
                F.col(column) * F.col(scaling_factor_column),
            ).otherwise(F.col(column)),
        )
    return df.drop(scaling_factor_column)


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
        raise DSGInvalidDimensionMapping(
            "Invalid dimension mapping application. "
            "Combination of remapped dataset dimensions contain NULL value(s) for "
            f"dimension(s): \n{str(exc)}"
        )


def is_noop_mapping(records: pyspark.sql.DataFrame) -> bool:
    """Return True if the mapping is a no-op."""
    return records.filter(
        (records.to_id.isNull() & ~records.from_id.isNull())
        | (~records.to_id.isNull() & records.from_id.isNull())
        | (records.from_id != records.to_id)
        | (records.from_fraction != 1.0)
    ).rdd.isEmpty()


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
        df.join(
            df.groupBy(*stacked).agg(F.count_distinct(time_column).alias("count_time")),
            on=stacked,
        )
        .filter(f"{time_column} IS NOT NULL or count_time == 0")
        .select(orig_columns)
    )


@track_timing(timer_stats_collector)
def repartition_if_needed_by_mapping(
    df: DataFrame,
    mapping_type: DimensionMappingType,
    scratch_dir_context: ScratchDirContext,
) -> DataFrame:
    """Repartition the dataframe if the mapping might cause data skew."""
    # We experienced an issue with the DECARB buildings dataset where the disaggregation of
    # region to county caused a major issue where one Spark executor thread got stuck,
    # seemingly indefinitely. A message like this was repeated continually.
    # UnsafeExternalSorter: Thread 152 spilling sort data of 4.0 GiB to disk (0  time so far)
    # It appears to be caused by data skew, though the imbalance didn't seem too severe.
    # Using a variation of what online sources call a "salting technique" solves the issue.
    # Apply the technique to mappings that will cause an explosion of rows.
    # Note that this probably isn't needed in all cases and we may need to adjust in the
    # future.
    # The case with buildings was particularly severe because it is an unpivoted dataset.

    # Note: log messages below are checked in the tests.
    if mapping_type in {
        DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
        # These cases might be problematic in the future.
        # DimensionMappingType.ONE_TO_MANY_ASSIGNMENT,
        # DimensionMappingType.ONE_TO_MANY_EXPLICIT_MULTIPLIERS,
        # DimensionMappingType.MANY_TO_MANY_DISAGGREGATION,
        # This is usually happening with scenario and hasn't caused a problem.
        # DimensionMappingType.DUPLICATION,
    }:
        if os.environ.get("DSGRID_SKIP_MAPPING_SKEW_REPARTITION", "false").lower() == "true":
            logger.info("DSGRID_SKIP_MAPPING_SKEW_REPARTITION is true; skip repartitions")
            return df

        filename = scratch_dir_context.get_temp_filename(suffix=".parquet")
        # Salting techniques online talk about adding or modifying a column with random values.
        # We might be able to use one of our value columns. However, there are cases where there
        # could be many instances of zero or null. So, add a new column with random values.
        logger.info("Repartition after mapping %s", mapping_type)
        salted_column = "salted_key"
        df.withColumn(salted_column, F.rand()).repartition(salted_column).write.parquet(
            str(filename)
        )
        df = read_parquet(filename).drop(salted_column)
        logger.info("Completed repartition.")
    else:
        logger.debug("Repartition is not needed for mapping_type %s", mapping_type)

    return df


def unpivot_dataframe(
    df: DataFrame, value_columns: Iterable[str], variable_column: str, time_columns: list[str]
) -> DataFrame:
    """Unpivot the dataframe, accounting for time columns."""
    values = value_columns if isinstance(value_columns, set) else set(value_columns)
    ids = [x for x in df.columns if x != VALUE_COLUMN and x not in values]
    df = df.unpivot(
        ids,
        value_columns,
        variable_column,
        VALUE_COLUMN,
    )
    cols = set(df.columns).difference(time_columns)
    new_rows = df.filter(f"{VALUE_COLUMN} IS NULL").select(*cols).distinct()
    for col in time_columns:
        new_rows = new_rows.withColumn(col, F.lit(None))
    return df.filter(f"{VALUE_COLUMN} IS NOT NULL").union(new_rows.select(*df.columns))
