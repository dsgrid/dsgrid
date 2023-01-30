import logging
import os
from collections import defaultdict

# from typing import List

import pyspark.sql.functions as F

from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimensionMapping
from dsgrid.utils.spark import check_for_nulls
from dsgrid.utils.timing import timer_stats_collector, track_timing

logger = logging.getLogger(__name__)


@track_timing(timer_stats_collector)
def map_and_reduce_stacked_dimension(df, records, column):
    if "fraction" not in df.columns:
        df = df.withColumn("fraction", F.lit(1.0))
    # map and consolidate from_fraction only
    # TODO: can remove this if we do it at registration time
    records = records.filter("to_id IS NOT NULL")

    df = (
        df.join(records, on=df[column] == records.from_id, how="inner")
        .drop("from_id")
        .drop(column)
        .withColumnRenamed("to_id", column)
    ).filter(f"{column} IS NOT NULL")
    nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
    df = df.fillna(1.0, subset=["from_fraction"]).selectExpr(
        *nonfraction_cols, "fraction*from_fraction AS fraction"
    )
    return df


@track_timing(timer_stats_collector)
def map_and_reduce_pivoted_dimension(df, records, pivoted_columns, operation, rename=False):
    """Maps the pivoted dimension columns as specified by records and operation. The operation
    is a row-wise aggregation.

    This can be used to:
        1. Map a dataset's dataframe columns per dataset-to-project mapping records.
        2. Map a project's dataframe columns per base-to-supplemental mapping records.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    records : pyspark.sql.DataFrame
        Dimension mapping records
    pivoted_columns : set
        Column names in df that are the pivoted dimension records
    operation : str
        Controls how to aggregate the the mapped columns.
    rename : bool
        Controls whether to rename the aggregated columns to include the operation name.
        For example, if the request is to sum the supplemental dimension 'all_electricity,'
        the column will be renamed to 'all_electricity_sum.'

    Returns
    -------
    tuple
        pyspark.sql.DataFrame, sorted list of pivoted dimension columns in that table, dropped cols

    """
    diff = pivoted_columns.difference(df.columns)
    assert not diff, diff
    nonvalue_cols = list(set(df.columns).difference(pivoted_columns))
    columns = set(df.columns)

    records_dict = defaultdict(dict)
    processed = set()
    for row in records.collect():
        if row.to_id is not None and row.from_id in columns:
            records_dict[row.to_id][row.from_id] = row.from_fraction
            processed.add(row.from_id)

    extra_pivoted_columns_to_keep = set(pivoted_columns).difference(processed)

    if operation == "sum":
        # This is identical to running F.coalesce(col, F.lit(0.0)) on each column.
        df = df.fillna(0.0, subset=list(pivoted_columns))

    to_ids = sorted(records_dict)
    exprs = []
    final_columns = []
    dropped = set()
    for tid in to_ids:
        column = f"{tid}" if rename else tid
        final_columns.append(column)
        expr = [f"{from_id}*{fraction}" for from_id, fraction in records_dict[tid].items()]
        if operation == "sum":
            val = "+".join(expr)
            expr = f"{val} AS {column}"
        elif operation == "max":
            val = ",".join(expr)
            expr = f"greatest({val}) AS {column}"
        elif operation == "min":
            val = ",".join(expr)
            expr = f"least({val}) AS {column}"
        # TODO #208: Need to decide how to handle NULL values. If they should not be included in
        # the mean, the logic below is incorrect
        # elif operation in ("avg", "mean"):
        #    val = "(" + "+".join(expr) + f") / {len(expr)}"
        #    expr = f"{val} AS {column}"
        else:
            raise Exception(f"Unsupported operation: {operation}")
        exprs.append(expr)
        dropped.update({x for x in records_dict[tid]})

    extra_cols = sorted(extra_pivoted_columns_to_keep)
    return df.selectExpr(*nonvalue_cols, *extra_cols, *exprs), sorted(final_columns), dropped


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


@track_timing(timer_stats_collector)
def check_null_value_in_unique_dimension_rows(dim_table, exclude_columns=None):
    if os.environ.get("__DSGRID_SKIP_CHECK_NULL_UNIQUE_DIMENSION__"):
        # This has intermittently caused GC-related timeouts for TEMPO.
        # Leave a backdoor to skip these checks, which may eventually be removed.
        logger.warning("Skip check_null_value_in_unique_dimension_rows")
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


def ordered_subset_columns(df, subset: set[str]) -> list[str]:
    """Return a list of columns in the dataframe that are present in subset."""
    return [x for x in df.columns if x in subset]
