"""Contains functions to convert electricity between units."""

import logging

from dsgrid.common import VALUE_COLUMN
from dsgrid.spark.functions import except_all, is_dataframe_empty, join
from dsgrid.spark.types import DataFrame, F


KWH = "kWh"
MWH = "MWh"
GWH = "GWh"
TWH = "TWh"
THERM = "therm"
MBTU = "MBtu"

KILO = 1_000
MEGA = 1_000_000
GIGA = 1_000_000_000
TERA = 1_000_000_000_000

KILO_TO_MEGA = KILO / MEGA
KILO_TO_GIGA = KILO / GIGA
KILO_TO_TERA = KILO / TERA

MEGA_TO_KILO = MEGA / KILO
MEGA_TO_GIGA = MEGA / GIGA
MEGA_TO_TERA = MEGA / TERA

GIGA_TO_KILO = GIGA / KILO
GIGA_TO_MEGA = GIGA / MEGA
GIGA_TO_TERA = GIGA / TERA

TERA_TO_KILO = TERA / KILO
TERA_TO_MEGA = TERA / MEGA
TERA_TO_GIGA = TERA / GIGA

THERM_TO_KWH = 29.307107017222222
THERM_TO_MWH = THERM_TO_KWH * KILO_TO_MEGA
THERM_TO_GWH = THERM_TO_KWH * KILO_TO_GIGA
THERM_TO_TWH = THERM_TO_KWH * KILO_TO_TERA

KWH_TO_THERM = 1 / THERM_TO_KWH
MWH_TO_THERM = 1 / THERM_TO_MWH
GWH_TO_THERM = 1 / THERM_TO_GWH
TWH_TO_THERM = 1 / THERM_TO_TWH

# BTU conversion is based on EIA. This website says 1 kWh = 3,412 BTU.
# https://www.eia.gov/energyexplained/units-and-calculators/energy-conversion-calculators.php
# The more precise number below comes from ResStock at
# https://github.com/NREL/resstock/blob/2e0a82a7bfad0f17ff75a3c66c91a5d72265a847/resources/hpxml-measures/HPXMLtoOpenStudio/resources/unit_conversions.rb
MBTU_TO_KWH = 293.0710701722222
MBTU_TO_MWH = MBTU_TO_KWH * KILO_TO_MEGA
MBTU_TO_GWH = MBTU_TO_KWH * KILO_TO_GIGA
MBTU_TO_TWH = MBTU_TO_KWH * KILO_TO_TERA

KWH_TO_MBTU = 1 / MBTU_TO_KWH
MWH_TO_MBTU = 1 / MBTU_TO_MWH
GWH_TO_MBTU = 1 / MBTU_TO_GWH
TWH_TO_MBTU = 1 / MBTU_TO_TWH

MBTU_TO_THERM = 10.0
THERM_TO_MBTU = 1 / MBTU_TO_THERM

logger = logging.getLogger(__name__)


def to_kwh(unit_col, value_col):
    """Convert a column to kWh."""
    return (
        F.when(F.col(unit_col) == KWH, F.col(value_col))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * MEGA_TO_KILO))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * GIGA_TO_KILO))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * TERA_TO_KILO))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * THERM_TO_KWH))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * MBTU_TO_KWH))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_mwh(unit_col, value_col):
    """Convert a column to mWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) * KILO_TO_MEGA))
        .when(F.col(unit_col) == MWH, F.col(value_col))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * GIGA_TO_MEGA))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * TERA_TO_MEGA))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * THERM_TO_MWH))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * MBTU_TO_MWH))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_gwh(unit_col, value_col):
    """Convert a column to gWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) * KILO_TO_GIGA))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * MEGA_TO_GIGA))
        .when(F.col(unit_col) == GWH, F.col(value_col))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * TERA_TO_GIGA))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * THERM_TO_GWH))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * MBTU_TO_GWH))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_twh(unit_col, value_col):
    """Convert a column to tWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) * KILO_TO_TERA))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * MEGA_TO_TERA))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * GIGA_TO_TERA))
        .when(F.col(unit_col) == TWH, F.col(value_col))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * THERM_TO_TWH))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * MBTU_TO_TWH))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_therm(unit_col, value_col):
    """Convert a column to therm."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) * KWH_TO_THERM))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * MWH_TO_THERM))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * GWH_TO_THERM))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * TWH_TO_THERM))
        .when(F.col(unit_col) == THERM, F.col(value_col))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * MBTU_TO_THERM))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_mbtu(unit_col, value_col):
    """Convert a column to MBtu."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) * KWH_TO_MBTU))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * MWH_TO_MBTU))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * GWH_TO_MBTU))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * TWH_TO_MBTU))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * THERM_TO_MBTU))
        .when(F.col(unit_col) == MBTU, F.col(value_col))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def from_any_to_any(from_unit_col, to_unit_col, value_col):
    """Convert a column based on from/to columns."""
    return (
        F.when(F.col(from_unit_col) == F.col(to_unit_col), F.col(value_col))
        .when(F.col(from_unit_col) == "", F.col(value_col))
        .when(F.col(to_unit_col) == KWH, to_kwh(from_unit_col, value_col))
        .when(F.col(to_unit_col) == MWH, to_mwh(from_unit_col, value_col))
        .when(F.col(to_unit_col) == GWH, to_gwh(from_unit_col, value_col))
        .when(F.col(to_unit_col) == TWH, to_twh(from_unit_col, value_col))
        .when(F.col(to_unit_col) == THERM, to_therm(from_unit_col, value_col))
        .when(F.col(to_unit_col) == MBTU, to_mbtu(from_unit_col, value_col))
        .otherwise(None)
    )


def convert_units_unpivoted(
    df, metric_column, from_records, from_to_records, to_unit_records
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
        logger.info("Return early because the units match.")
        return df

    df = join(df, unit_df, metric_column, "id").drop("id")
    tmp3 = to_unit_records.select("id", "unit").withColumnRenamed(unit_col, "to_unit")
    df = join(df, tmp3, metric_column, "id").drop("id")
    logger.info("Converting units from column %s", metric_column)
    return df.withColumn(VALUE_COLUMN, from_any_to_any("from_unit", "to_unit", VALUE_COLUMN)).drop(
        "from_unit", "to_unit"
    )
