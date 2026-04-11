import ibis
import logging

from dsgrid.common import VALUE_COLUMN
from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.operations import create_temp_view, drop_columns, except_all, join, rename_columns
from dsgrid.ibis.table_utils import get_unique_values
from dsgrid.ibis.types import is_table_empty
from dsgrid.units.constants import (
    ENERGY_UNITS,
    POWER_UNITS,
    GW,
    GWH,
    KILO_TO_GIGA,
    KW,
    KWH,
    MBTU,
    MBTU_TO_KWH,
    MEGA_TO_KILO,
    MW,
    MWH,
    TERA_TO_KILO,
    THERM,
    THERM_TO_KWH,
    TW,
    TWH,
)
from dsgrid.ibis.session import get_runtime_session


logger = logging.getLogger(__name__)


def convert_units_unpivoted(
    df: ibis.Table,
    metric_column: str,
    from_records: ibis.Table,
    from_to_records: ibis.Table | None,
    to_unit_records: ibis.Table,
) -> ibis.Table:
    """Convert the value column of the dataframe to the target units.

    Parameters
    ----------
    df : Ibis table
        Load data table
    metric_column : str
        Column in dataframe with metric record IDs
    from_records : Ibis table
        Metric dimension records for the columns being converted
    from_to_records : Ibis table | None
        Records that map the dimension IDs in columns to the target IDs
        If None, mapping is not required and from_records contain the units.
    to_unit_records : Ibis table
        Metric dimension records for the target IDs
    """
    unit_col = "unit"  # must match EnergyEndUse.unit
    tmp1 = rename_columns(from_records.select("id", unit_col), {unit_col: "from_unit"})
    if from_to_records is None:
        unit_df = tmp1.select("id", "from_unit")
    else:
        tmp2 = from_to_records.select("from_id", "to_id")
        joined = join(tmp1, tmp2, "id", "from_id")
        unit_df = joined.select(_alias_column(joined, "to_id", "id"), "from_unit").distinct()
    if is_table_empty(
        except_all(
            unit_df,
            to_unit_records.select("id", _alias_column(to_unit_records, "unit", "from_unit")),
        )
    ):
        logger.debug("Return early because the units match.")
        return df

    df = drop_columns(join(df, unit_df, metric_column, "id"), "id")
    tmp3 = rename_columns(to_unit_records.select("id", "unit"), {unit_col: "to_unit"})
    df = drop_columns(join(df, tmp3, metric_column, "id"), "id")
    logger.debug("Converting units from column %s", metric_column)

    units = get_unique_values(to_unit_records, unit_col)
    if units.issubset(ENERGY_UNITS):
        conversion_expr = _make_conversion_expr(_ENERGY_TO_KWH, "from_unit", "to_unit")
    elif units.issubset(POWER_UNITS):
        conversion_expr = _make_conversion_expr(_POWER_TO_KW, "from_unit", "to_unit")
    else:
        msg = f"Unsupported unit conversion: {units}"
        raise ValueError(msg)

    view = create_temp_view(df)
    cols = ", ".join(x for x in df.columns if x not in {VALUE_COLUMN, "from_unit", "to_unit"})
    query = f"SELECT {cols}, {conversion_expr} AS {VALUE_COLUMN} FROM {view}"
    return _sql(df, query)


def _alias_column(df: ibis.Table, column: str, alias: str):
    return df[column].name(alias) if isinstance(df, ibis.Table) else df[column].alias(alias)


def _make_conversion_expr(unit_to_base: dict[str, float], from_unit_col: str, to_unit_col: str):
    cases = [
        f"WHEN {from_unit_col} = {to_unit_col} THEN {VALUE_COLUMN}",
        f"WHEN {from_unit_col} = '' THEN {VALUE_COLUMN}",
    ]
    for to_unit, to_factor in unit_to_base.items():
        for from_unit, from_factor in unit_to_base.items():
            if from_unit == to_unit:
                continue
            factor = from_factor / to_factor
            cases.append(
                f"WHEN {to_unit_col} = '{to_unit}' AND {from_unit_col} = '{from_unit}' "
                f"THEN {VALUE_COLUMN} * {factor}"
            )
    return "CASE " + " ".join(cases) + " ELSE NULL END"


def _sql(df: ibis.Table, query: str) -> ibis.Table:
    if isinstance(df, ibis.Table):
        return make_runtime_backend().sql(query)
    return get_runtime_session().sql(query)


_ENERGY_TO_KWH = {
    KWH: 1.0,
    MWH: MEGA_TO_KILO,
    GWH: 1 / KILO_TO_GIGA,
    TWH: TERA_TO_KILO,
    THERM: THERM_TO_KWH,
    MBTU: MBTU_TO_KWH,
}

_POWER_TO_KW = {
    KW: 1.0,
    MW: MEGA_TO_KILO,
    GW: 1 / KILO_TO_GIGA,
    TW: TERA_TO_KILO,
}
