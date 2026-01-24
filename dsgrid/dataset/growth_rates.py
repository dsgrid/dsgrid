import logging
import ibis
import ibis.expr.types as ir

from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.query.models import ProjectionDatasetModel


logger = logging.getLogger(__name__)


def apply_exponential_growth_rate(
    dataset: ProjectionDatasetModel,
    initial_value_tbl: ir.Table,
    growth_rate_tbl: ir.Table,
    time_columns,
    model_year_column,
    value_columns,
) -> ir.Table:
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
    initial_value_tbl : ir.Table
    growth_rate_tbl : ir.Table
    time_columns : set[str]
    model_year_column : str
    value_columns : set[str]

    Returns
    -------
    ir.Table

    """
    iv_tbl = initial_value_tbl
    gr_tbl = growth_rate_tbl

    try:
        print(f"DEBUG: Growth calc - IV count: {iv_tbl.count().to_pyarrow().as_py()}")
        print(f"DEBUG: Growth calc - GR count: {gr_tbl.count().to_pyarrow().as_py()}")
        if "value" in iv_tbl.columns:
            print(
                f"DEBUG: Growth calc - IV sum: {iv_tbl.select('value').sum().to_pyarrow().as_py()}"
            )
    except Exception as e:
        print(f"DEBUG: Growth calc - Failed to get info: {e}")

    iv_tbl, gr_tbl = _process_exponential_growth_rate(
        dataset,
        iv_tbl,
        gr_tbl,
        model_year_column,
        value_columns,
    )

    result_tbl = apply_annual_multiplier_ibis(
        iv_tbl,
        gr_tbl,
        time_columns,
        value_columns,
    )

    try:
        print(f"DEBUG: Growth calc - Result count: {result_tbl.count().to_pyarrow().as_py()}")
        if "value" in result_tbl.columns:
            print(
                f"DEBUG: Growth calc - Result sum: {result_tbl.select('value').sum().to_pyarrow().as_py()}"
            )
    except Exception as e:
        print(f"DEBUG: Growth calc - Failed to get result info: {e}")

    return result_tbl


def apply_annual_multiplier(
    initial_value_tbl: ir.Table,
    growth_rate_tbl: ir.Table,
    time_columns,
    value_columns,
) -> ir.Table:
    """Applies annual growth rate to the initial_value dataframe as follows:
    P(t) = P0 * r(t)
    where:
      P(t): quantity at year t
      P0: initial quantity
      r(t): growth rate per year t (relative to P0)

    Parameters
    ----------
    dataset : ProjectionDatasetModel
    initial_value_tbl : ir.Table
    growth_rate_tbl : ir.Table
    time_columns : set[str]
    value_columns : set[str]

    Returns
    -------
    ir.Table

    """
    iv_tbl = initial_value_tbl
    gr_tbl = growth_rate_tbl
    result_tbl = apply_annual_multiplier_ibis(iv_tbl, gr_tbl, time_columns, value_columns)
    return result_tbl


def apply_annual_multiplier_ibis(
    iv_tbl: ir.Table,
    gr_tbl: ir.Table,
    time_columns,
    value_columns,
):
    def renamed(col):
        return col + "_gr"

    dim_columns = list(set(iv_tbl.columns) - value_columns - time_columns)

    # Inner join on dimension columns
    joined = iv_tbl.join(gr_tbl, predicates=dim_columns, how="inner")

    # Ibis join result includes columns from both tables.
    # Since we joined on dim_columns, they are unified.
    # We need to explicitly select/compute the output columns.

    projection = []
    # Keep all columns from iv_tbl, updating value columns
    for col in iv_tbl.columns:
        if col in value_columns:
            gr_col_name = renamed(col)
            # Calculate new value: P0 * r(t)
            new_val = joined[col] * joined[gr_col_name]
            projection.append(new_val.name(col))
        else:
            projection.append(joined[col])

    return joined.select(projection)


def _process_exponential_growth_rate(
    dataset,
    initial_value_tbl: ir.Table,
    growth_rate_tbl: ir.Table,
    model_year_column,
    value_columns,
):
    def renamed(col):
        return col + "_gr"

    initial_value_tbl, base_year = _check_model_years(
        dataset, initial_value_tbl, growth_rate_tbl, model_year_column
    )

    gr_tbl = growth_rate_tbl
    mutations = {}

    # Compute growth rate multipliers
    # (1 + r) ** (year - base_year)
    for column in value_columns:
        gr_col = renamed(column)

        # Casting model_year_column to integer if needed
        year_val = gr_tbl[model_year_column].cast("int")
        base_val = ibis.literal(base_year)
        exponent = year_val - base_val

        # Ibis pow: (base).pow(exp) or ibis.pow(base, exp)
        # Note: Ibis expression syntax
        rate = gr_tbl[column]
        factor = (1 + rate).pow(exponent)

        mutations[gr_col] = factor

    # Apply mutations and drop original rate columns
    gr_tbl = gr_tbl.mutate(**mutations).drop(*value_columns)

    return initial_value_tbl, gr_tbl


def _check_model_years(dataset, initial_value_tbl, growth_rate_tbl, model_year_column):
    # We still use spark/duckdb util for unique values for now as it takes a DF.
    # To keep it pure ibis, we should implement get_unique_values with ibis.
    # But get_unique_values is imported.
    # For now, execute a small query to get unique years.

    # iv_years = get_unique_values(initial_value_df, model_year_column)
    # Using ibis:
    iv_years_col = initial_value_tbl.select(model_year_column).distinct()
    # Execute to get values as list
    # Depending on backend, .to_pyarrow() or .execute()
    # ibis 9+ prefers execute() which returns pandas series/df
    iv_years = sorted([int(x) for x in iv_years_col.execute()[model_year_column].tolist()])

    if dataset.base_year is None:
        base_year = iv_years[0]
    elif dataset.base_year in iv_years:
        base_year = dataset.base_year
    else:
        msg = f"ProjectionDatasetModel base_year={dataset.base_year} is not in {iv_years}"
        raise DSGInvalidQuery(msg)

    if len(iv_years) > 1:
        # TODO #198: needs test case
        initial_value_tbl = initial_value_tbl.filter(
            initial_value_tbl[model_year_column] == str(base_year)
        )

    # Cross join iv (without model year) with gr model years
    # distinct model years from growth rate
    gr_years = growth_rate_tbl.select(model_year_column).distinct()

    iv_dropped = initial_value_tbl.drop(model_year_column)
    initial_value_tbl = iv_dropped.cross_join(gr_years)

    return initial_value_tbl, base_year
