import logging

import dsgrid.units.energy as energy
import dsgrid.units.power as power
from dsgrid.common import VALUE_COLUMN
from dsgrid.spark.functions import except_all, is_dataframe_empty, join
from dsgrid.spark.types import DataFrame, F
from dsgrid.units.constants import ENERGY_UNITS, POWER_UNITS
from dsgrid.utils.spark import get_unique_values


logger = logging.getLogger(__name__)


def convert_units_unpivoted(
    df: DataFrame,
    metric_column: str,
    from_records: DataFrame,
    from_to_records: DataFrame | None,
    to_unit_records: DataFrame,
) -> DataFrame:
    """Convert the value column of the dataframe to the target units.

    Parameters
    ----------
    df : DataFrame
        Load data table
    metric_column : str
        Column in dataframe with metric record IDs
    from_records : DataFrame
        Metric dimension records for the columns being converted
    from_to_records : DataFrame | None
        Records that map the dimension IDs in columns to the target IDs
        If None, mapping is not required and from_records contain the units.
    to_unit_records : DataFrame
        Metric dimension records for the target IDs
    """
    unit_col = "unit"  # must match EnergyEndUse.unit
    tmp1 = from_records.select("id", unit_col).withColumnRenamed(unit_col, "from_unit")
    if from_to_records is None:
        unit_df = tmp1.select("id", "from_unit")
    else:
        tmp2 = from_to_records.select("from_id", "to_id")
        unit_df = (
            join(tmp1, tmp2, "id", "from_id")
            .select(F.col("to_id").alias("id"), "from_unit")
            .distinct()
        )
    if is_dataframe_empty(
        except_all(unit_df, to_unit_records.select("id", F.col("unit").alias("from_unit")))
    ):
        logger.debug("Return early because the units match.")
        return df

    df = join(df, unit_df, metric_column, "id").drop("id")
    tmp3 = to_unit_records.select("id", "unit").withColumnRenamed(unit_col, "to_unit")
    df = join(df, tmp3, metric_column, "id").drop("id")
    logger.debug("Converting units from column %s", metric_column)

    units = get_unique_values(to_unit_records, unit_col)
    if units.issubset(ENERGY_UNITS):
        func = energy.from_any_to_any
    elif units.issubset(POWER_UNITS):
        func = power.from_any_to_any
    else:
        msg = f"Unsupported unit conversion: {units}"
        raise ValueError(msg)

    return df.withColumn(VALUE_COLUMN, func("from_unit", "to_unit", VALUE_COLUMN)).drop(
        "from_unit", "to_unit"
    )
