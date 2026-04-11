import ibis
import logging

from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ProjectionDatasetModel
from dsgrid.ibis.operations import (
    cross_join,
    drop_columns,
    filter_sql,
    handle_column_spaces,
    join_multiple_columns,
    sql_from_df,
)
from dsgrid.ibis.table_utils import get_unique_values


logger = logging.getLogger(__name__)


def apply_exponential_growth_rate(
    dataset: ProjectionDatasetModel,
    initial_value_df: ibis.Table,
    growth_rate_df: ibis.Table,
    time_columns,
    model_year_column,
    value_columns,
):
    """Applies exponential growth rate to the initial_value dataframe as follows:
    P(t) = P0*(1+r)^(t-t0)
    where:
      P(t): quantity at t
      P0: initial quantity at t0, = P(t0)
      r: growth rate (per time interval)
      t-t0: number of time intervals


    Parameters
    ----------
    dataset : ProjectionDatasetModel
    initial_value_df : ibis.Table
    growth_rate_df : ibis.Table
    time_columns : set[str]
    model_year_column : str
    value_columns : set[str]

    Returns
    -------
    ibis.Table

    """

    initial_value_df, growth_rate_df = _process_exponential_growth_rate(
        dataset,
        initial_value_df,
        growth_rate_df,
        model_year_column,
        value_columns,
    )

    df = apply_annual_multiplier(
        initial_value_df,
        growth_rate_df,
        time_columns,
        value_columns,
    )

    return df


def apply_annual_multiplier(
    initial_value_df: ibis.Table,
    growth_rate_df: ibis.Table,
    time_columns,
    value_columns,
):
    """Applies annual growth rate to the initial_value dataframe as follows:
    P(t) = P0 * r(t)
    where:
      P(t): quantity at year t
      P0: initial quantity
      r(t): growth rate per year t (relative to P0)

    Parameters
    ----------
    dataset : ProjectionDatasetModel
    initial_value_df : ibis.Table
    growth_rate_df : ibis.Table
    time_columns : set[str]
    value_columns : set[str]

    Returns
    -------
    ibis.Table

    """

    def renamed(col):
        return col + "_gr"

    orig_columns = initial_value_df.columns

    dim_columns = set(initial_value_df.columns) - value_columns - time_columns
    df = join_multiple_columns(initial_value_df, growth_rate_df, list(dim_columns))
    select_exprs = []
    for column in orig_columns:
        quoted = handle_column_spaces(column)
        if column in value_columns:
            gr_column = handle_column_spaces(renamed(column))
            select_exprs.append(f"{quoted} * {gr_column} AS {handle_column_spaces(column)}")
        else:
            select_exprs.append(quoted)

    return sql_from_df(df, f"SELECT {', '.join(select_exprs)}")


def _process_exponential_growth_rate(
    dataset,
    initial_value_df,
    growth_rate_df,
    model_year_column,
    value_columns,
):
    def renamed(col):
        return col + "_gr"

    initial_value_df, base_year = _check_model_years(
        dataset, initial_value_df, growth_rate_df, model_year_column
    )

    gr_df = growth_rate_df
    for column in value_columns:
        gr_col = renamed(column)
        cols = ",".join([x for x in gr_df.columns if x not in (column, gr_col)])
        query = f"""
            SELECT
                {cols}
                ,POWER(1 + {column}, CAST({model_year_column} AS INTEGER) - {base_year}) AS {gr_col}
        """
        gr_df = sql_from_df(gr_df, query)

    return initial_value_df, gr_df


def _check_model_years(dataset, initial_value_df, growth_rate_df, model_year_column):
    iv_years = get_unique_values(initial_value_df, model_year_column)
    iv_years_sorted = sorted((int(x) for x in iv_years))

    if dataset.base_year is None:
        base_year = iv_years_sorted[0]
    elif dataset.base_year in iv_years:
        base_year = dataset.base_year
    else:
        msg = f"ProjectionDatasetModel base_year={dataset.base_year} is not in {iv_years_sorted}"
        raise DSGInvalidQuery(msg)

    if len(iv_years) > 1:
        # TODO #198: needs test case
        initial_value_df = filter_sql(initial_value_df, f"{model_year_column} == '{base_year}'")

    initial_value_df = cross_join(
        drop_columns(initial_value_df, model_year_column),
        growth_rate_df.select(model_year_column).distinct(),
    )
    return initial_value_df, base_year
