import logging

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ProjectionDatasetModel
from dsgrid.utils.spark import get_unique_values


logger = logging.getLogger(__name__)


def apply_exponential_growth_rate(
    dataset: ProjectionDatasetModel,
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
    orig_columns = initial_value_df.columns
    initial_value_df, base_year = _check_model_years(
        dataset, initial_value_df, growth_rate_df, model_year_column
    )

    def renamed(col):
        return col + "_gr"

    gr_df = growth_rate_df
    for column in pivoted_columns:
        gr_col = renamed(column)
        gr_df = gr_df.withColumn(
            gr_col,
            F.pow((1 + F.col(column)), F.col(model_year_column).cast(IntegerType()) - base_year),
        ).drop(column)

    dim_columns = set(initial_value_df.columns) - pivoted_columns - time_columns
    df = initial_value_df.join(gr_df, on=list(dim_columns))
    for column in df.columns:
        if column in pivoted_columns:
            gr_column = renamed(column)
            df = df.withColumn(column, df[column] * df[gr_column])

    return df.select(*orig_columns)


def _check_model_years(dataset, initial_value_df, growth_rate_df, model_year_column):
    iv_years = get_unique_values(initial_value_df, model_year_column)
    iv_years_sorted = sorted((int(x) for x in iv_years))

    if dataset.base_year is None:
        base_year = iv_years_sorted[0]
    elif dataset.base_year in iv_years:
        base_year = dataset.base_year
    else:
        raise DSGInvalidQuery(
            f"ProjectionDatasetModel base_year={dataset.base_year} is not in {iv_years_sorted}"
        )

    if len(iv_years) > 1:
        # TODO #198: needs test case
        initial_value_df = initial_value_df.filter(f"{model_year_column} == '{base_year}'")

    initial_value_df = initial_value_df.drop(model_year_column).crossJoin(
        growth_rate_df.select(model_year_column).distinct()
    )
    return initial_value_df, base_year
