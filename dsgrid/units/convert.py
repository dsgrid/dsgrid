import logging

from ibis import Table

import dsgrid.units.energy as energy
import dsgrid.units.power as power
from dsgrid.common import VALUE_COLUMN
from dsgrid.units.constants import ENERGY_UNITS, POWER_UNITS
from dsgrid.ibis_api import get_unique_values


logger = logging.getLogger(__name__)


def convert_units_unpivoted(
    df: Table,
    metric_column: str,
    from_records: Table,
    from_to_records: Table | None,
    to_unit_records: Table,
) -> Table:
    """Convert the value column of the dataframe to the target units.

    Parameters
    ----------
    df : Table
        Load data table
    metric_column : str
        Column in dataframe with metric record IDs
    from_records : Table
        Metric dimension records for the columns being converted
    from_to_records : Table | None
        Records that map the dimension IDs in columns to the target IDs
        If None, mapping is not required and from_records contain the units.
    to_unit_records : Table
        Metric dimension records for the target IDs
    """
    unit_col = "unit"  # must match EnergyEndUse.unit
    tmp1 = from_records.select("id", unit_col).rename(from_unit=unit_col)
    if from_to_records is None:
        unit_df = tmp1.select("id", "from_unit")
    else:
        tmp2 = from_to_records.select("from_id", "to_id")
        unit_df = (
            tmp1.inner_join(tmp2, tmp1["id"] == tmp2["from_id"])
            .select(id="to_id", from_unit="from_unit")
            .distinct()
        )

    diff = unit_df.difference(to_unit_records.select("id", from_unit="unit"))
    if diff.count().execute() == 0:
        logger.debug("Return early because the units match.")
        return df

    logger.debug("Joining df with unit_df...")
    count_before = df.count().to_pyarrow().as_py()
    df = df.inner_join(unit_df, df[metric_column] == unit_df["id"]).drop("id")
    count_after_unit_df = df.count().to_pyarrow().as_py()
    if count_after_unit_df != count_before:
        logger.warning(
            f"Row count changed after joining unit_df: {count_before} -> {count_after_unit_df}"
        )

    tmp3 = to_unit_records.select("id", "unit").rename(to_unit=unit_col)
    df = df.inner_join(tmp3, df[metric_column] == tmp3["id"]).drop("id")
    count_after_to_unit_df = df.count().to_pyarrow().as_py()
    if count_after_to_unit_df != count_after_unit_df:
        logger.warning(
            f"Row count changed after joining to_unit_records: {count_after_unit_df} -> {count_after_to_unit_df}"
        )

    logger.debug("Converting units from column %s", metric_column)

    units = get_unique_values(to_unit_records, unit_col)
    if units.issubset(ENERGY_UNITS):
        func = energy.from_any_to_any
    elif units.issubset(POWER_UNITS):
        func = power.from_any_to_any
    else:
        msg = f"Unsupported unit conversion: {units}"
        raise ValueError(msg)

    df = df.mutate(**{VALUE_COLUMN: func(df["from_unit"], df["to_unit"], df[VALUE_COLUMN])})
    cols = [x for x in df.columns if x not in {"from_unit", "to_unit"}]
    return df.select(cols)
