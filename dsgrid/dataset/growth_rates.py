import logging

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ExponentialGrowthDatasetModel
from dsgrid.utils.dataset import ordered_subset_columns
from dsgrid.utils.spark import get_unique_values


logger = logging.getLogger(__name__)


def apply_growth_rate_123(
    dataset: ExponentialGrowthDatasetModel,
    initial_value_df,
    growth_rate_df,
    time_columns,
    model_year_column,
    pivoted_columns,
):
    """Applies the growth rate to the initial_value dataframe.

    Parameters
    ----------
    initial_value_df : pyspark.sql.DataFrame
    growth_rate_df : pyspark.sql.DataFrame
    time_columns : set[str]
    model_year_column : str
    pivoted_columns : set[str]

    Returns
    -------
    pyspark.sql.DataFrame

    """
    initial_value_df, base_year = _check_model_years(
        dataset, initial_value_df, growth_rate_df, model_year_column
    )
    gr_df = growth_rate_df
    for column in pivoted_columns:
        gr_col = column + "__gr"
        gr_df = gr_df.withColumn(
            gr_col,
            F.pow((1 + F.col(column)), F.col(model_year_column).cast(IntegerType()) - base_year),
        ).drop(column)

    dim_columns = set(initial_value_df.columns) - pivoted_columns - time_columns
    # TODO: data_source needs some thought. They are different in these two dfs.
    # And this should be dimension_query_name instead of dimension type
    # What is the data_source of the resulting df?
    if DimensionType.DATA_SOURCE.value in dim_columns:
        dim_columns.remove(DimensionType.DATA_SOURCE.value)
    if DimensionType.DATA_SOURCE.value in gr_df.columns:
        gr_df = gr_df.drop(DimensionType.DATA_SOURCE.value)

    df = initial_value_df.join(gr_df, on=list(dim_columns))
    for column in ordered_subset_columns(df, pivoted_columns):
        tmp_col = column + "_tmp"
        gr_col = column + "__gr"
        df = (
            df.withColumn(tmp_col, F.col(column) * F.col(gr_col))
            .drop(column, gr_col)
            .withColumnRenamed(tmp_col, column)
        )

    return df


def _check_model_years(dataset, initial_value_df, growth_rate_df, model_year_column):
    iv_years = get_unique_values(initial_value_df, model_year_column)
    iv_years_sorted = sorted((int(x) for x in iv_years))

    if dataset.base_year is None:
        base_year = iv_years_sorted[0]
    elif dataset.base_year in iv_years:
        base_year = dataset.base_year
    else:
        raise DSGInvalidQuery(
            f"ExponentialGrowthDatasetModel base_year={dataset.base_year} is not in {iv_years_sorted}"
        )

    if len(iv_years) > 1:
        # TODO: needs test case
        initial_value_df = initial_value_df.filter(f"{model_year_column} == '{base_year}'")

    initial_value_df = initial_value_df.drop(model_year_column).crossJoin(
        growth_rate_df.select(model_year_column).distinct()
    )
    return initial_value_df, base_year
