import ibis
import logging

from dsgrid.common import VALUE_COLUMN
from dsgrid.ibis.operations import drop_columns, except_all, join, rename_columns
from dsgrid.ibis.table_utils import get_unique_values, is_table_empty
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
        unit_to_base = _ENERGY_TO_KWH
    elif units.issubset(POWER_UNITS):
        unit_to_base = _POWER_TO_KW
    else:
        msg = f"Unsupported unit conversion: {units}"
        raise ValueError(msg)

    converted = _make_conversion_expr(df, unit_to_base, "from_unit", "to_unit")
    keep = [c for c in df.columns if c not in ("from_unit", "to_unit", VALUE_COLUMN)]
    projections = [df[c] for c in keep] + [converted.name(VALUE_COLUMN)]
    return df.select(*projections)


def _alias_column(df: ibis.Table, column: str, alias: str):
    return df[column].name(alias)


def _make_conversion_expr(
    df: ibis.Table, unit_to_base: dict[str, float], from_unit_col: str, to_unit_col: str
):
    from_col = df[from_unit_col]
    to_col = df[to_unit_col]
    value_col = df[VALUE_COLUMN]
    # Factor of each known unit relative to the family base unit. An unknown
    # (non-empty) unit matches no branch, so ibis.cases emits NULL for it; the
    # NULL then propagates through the division below (guarded else).
    from_factor = ibis.cases(
        *[(from_col == unit, factor) for unit, factor in unit_to_base.items()]
    )
    to_factor = ibis.cases(*[(to_col == unit, factor) for unit, factor in unit_to_base.items()])
    # Thread the (double) value column through both operations so every backend
    # keeps the arithmetic in double precision. Do NOT rewrite as
    # ``value_col * (from_factor / to_factor)``: that divides two decimal literals
    # (CASE / CASE) which Spark evaluates in truncated-scale decimal and loses
    # precision, while an unknown unit still yields NULL (guarded else). See PR #414.
    converted = value_col * from_factor / to_factor
    return ibis.cases(
        (from_col == to_col, value_col),  # same unit (incl. unitless == unitless) -> passthrough
        (from_col == "", value_col),  # unitless source -> passthrough
        else_=converted,
    )


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
