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
        df = df.withColumn("fraction", F.lit(1))
    # map and consolidate from_fraction only
    # TODO: can remove this if we do it at registration time
    records = records.filter("to_id IS NOT NULL")
    df = (
        df.join(records, on=df[column] == records.from_id, how="inner")
        .drop("from_id")
        .drop(column)
        .withColumnRenamed("to_id", column)
    ).filter(f"{column} IS NOT NULL")
    # After remapping, rows in load_data_lookup for standard_handler and rows in load_data for
    # one_table_handler may not be unique;
    # imagine 5 subsectors being remapped/consolidated to 2 subsectors.
    nonfraction_cols = [x for x in df.columns if x not in {"fraction", "from_fraction"}]
    df = df.fillna(1, subset=["from_fraction"]).selectExpr(
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

    to_ids = sorted(records_dict)
    exprs = []
    final_columns = []
    dropped = set()
    for tid in to_ids:
        column = f"{tid}_{operation}" if rename else tid
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
        elif operation in ("avg", "mean"):
            val = "(" + "+".join(expr) + f") / {len(expr)}"
            expr = f"{val} AS {column}"
        else:
            raise Exception(f"Unsupported operation: {operation}")
        exprs.append(expr)
        dropped.update({x for x in records_dict[tid]})

    extra_cols = sorted(extra_pivoted_columns_to_keep)
    return df.selectExpr(*nonvalue_cols, *extra_cols, *exprs), sorted(final_columns), dropped


def add_column_from_records(df, dimension_records, dimension_name, column_to_add):
    df = df.join(
        dimension_records.select(F.col("id").alias("record_id"), column_to_add),
        on=F.col(dimension_name) == F.col("record_id"),
        how="inner",
    ).drop("record_id")
    return df


@track_timing(timer_stats_collector)
def check_null_value_in_unique_dimension_rows(dim_table):
    if os.environ.get("__DSGRID_SKIP_NULL_UNIQUE_DIMENSION_CHECK__"):
        # This has intermittently caused GC-related timeouts for TEMPO.
        # Leave a backdoor to skip these checks, which may eventually be removed.
        logger.warning("Skip check_null_value_in_unique_dimension_rows")
        return

    try:
        check_for_nulls(dim_table, exclude_columns={"id"})
    except DSGInvalidField as exc:
        raise DSGInvalidDimensionMapping(
            "Invalid dimension mapping application. "
            "Combination of remapped dataset dimensions contain NULL value(s) for "
            f"dimension(s): \n{str(exc)}"
        )
