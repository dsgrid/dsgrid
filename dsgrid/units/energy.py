"""Contains functions to convert electricity between units."""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from dsgrid.common import VALUE_COLUMN
from dsgrid.exceptions import DSGInvalidDimension, DSGInvalidParameter


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

KILO_TO_MEGA = 1 / KILO
KILO_TO_GIGA = 1 / MEGA
KILO_TO_TERA = 1 / GIGA

MEGA_TO_KILO = KILO
MEGA_TO_GIGA = 1 / KILO
MEGA_TO_TERA = 1 / MEGA

GIGA_TO_KILO = MEGA
GIGA_TO_MEGA = KILO
GIGA_TO_TERA = 1 / KILO

TERA_TO_KILO = GIGA
TERA_TO_MEGA = MEGA
TERA_TO_GIGA = KILO

THERM_TO_KWH = 29.307107017222222
THERM_TO_MWH = THERM_TO_KWH * KILO_TO_MEGA
THERM_TO_GWH = THERM_TO_KWH * KILO_TO_GIGA
THERM_TO_TWH = THERM_TO_KWH * KILO_TO_TERA

KWH_TO_THERM = 1 / THERM_TO_KWH
MWH_TO_THERM = MEGA_TO_KILO * KWH_TO_THERM
GWH_TO_THERM = 1 / THERM_TO_GWH
TWH_TO_THERM = 1 / THERM_TO_TWH

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
        # From KWh
        .when(
            (F.col(from_unit_col) == KWH) & (F.col(to_unit_col) == MWH),
            F.col(value_col) * KILO_TO_MEGA,
        )
        .when(
            (F.col(from_unit_col) == KWH) & (F.col(to_unit_col) == GWH),
            F.col(value_col) * KILO_TO_GIGA,
        )
        .when(
            (F.col(from_unit_col) == KWH) & (F.col(to_unit_col) == TWH),
            F.col(value_col) * KILO_TO_TERA,
        )
        .when(
            (F.col(from_unit_col) == KWH) & (F.col(to_unit_col) == THERM),
            F.col(value_col) * KWH_TO_THERM,
        )
        .when(
            (F.col(from_unit_col) == KWH) & (F.col(to_unit_col) == MBTU),
            F.col(value_col) * KWH_TO_MBTU,
        )
        # From MWh
        .when(
            (F.col(from_unit_col) == MWH) & (F.col(to_unit_col) == KWH),
            F.col(value_col) * MEGA_TO_KILO,
        )
        .when(
            (F.col(from_unit_col) == MWH) & (F.col(to_unit_col) == GWH),
            F.col(value_col) * MEGA_TO_GIGA,
        )
        .when(
            (F.col(from_unit_col) == MWH) & (F.col(to_unit_col) == TWH),
            F.col(value_col) * MEGA_TO_TERA,
        )
        .when(
            (F.col(from_unit_col) == MWH) & (F.col(to_unit_col) == THERM),
            F.col(value_col) * MWH_TO_THERM,
        )
        .when(
            (F.col(from_unit_col) == MWH) & (F.col(to_unit_col) == MBTU),
            F.col(value_col) * MWH_TO_MBTU,
        )
        # From GWh
        .when(
            (F.col(from_unit_col) == GWH) & (F.col(to_unit_col) == KWH),
            F.col(value_col) * GIGA_TO_KILO,
        )
        .when(
            (F.col(from_unit_col) == GWH) & (F.col(to_unit_col) == MWH),
            F.col(value_col) * GIGA_TO_MEGA,
        )
        .when(
            (F.col(from_unit_col) == GWH) & (F.col(to_unit_col) == TWH),
            F.col(value_col) * GIGA_TO_TERA,
        )
        .when(
            (F.col(from_unit_col) == GWH) & (F.col(to_unit_col) == THERM),
            F.col(value_col) * GWH_TO_THERM,
        )
        .when(
            (F.col(from_unit_col) == GWH) & (F.col(to_unit_col) == MBTU),
            F.col(value_col) * GWH_TO_MBTU,
        )
        # From TWh
        .when(
            (F.col(from_unit_col) == TWH) & (F.col(to_unit_col) == KWH),
            F.col(value_col) * TERA_TO_KILO,
        )
        .when(
            (F.col(from_unit_col) == TWH) & (F.col(to_unit_col) == MWH),
            F.col(value_col) * TERA_TO_MEGA,
        )
        .when(
            (F.col(from_unit_col) == TWH) & (F.col(to_unit_col) == GWH),
            F.col(value_col) * TERA_TO_GIGA,
        )
        .when(
            (F.col(from_unit_col) == TWH) & (F.col(to_unit_col) == THERM),
            F.col(value_col) * TWH_TO_THERM,
        )
        .when(
            (F.col(from_unit_col) == TWH) & (F.col(to_unit_col) == MBTU),
            F.col(value_col) * TWH_TO_MBTU,
        )
        # From Therms
        .when(
            (F.col(from_unit_col) == THERM) & (F.col(to_unit_col) == KWH),
            F.col(value_col) * THERM_TO_KWH,
        )
        .when(
            (F.col(from_unit_col) == THERM) & (F.col(to_unit_col) == MWH),
            F.col(value_col) * THERM_TO_MWH,
        )
        .when(
            (F.col(from_unit_col) == THERM) & (F.col(to_unit_col) == GWH),
            F.col(value_col) * THERM_TO_GWH,
        )
        .when(
            (F.col(from_unit_col) == THERM) & (F.col(to_unit_col) == TWH),
            F.col(value_col) * THERM_TO_TWH,
        )
        .when(
            (F.col(from_unit_col) == THERM) & (F.col(to_unit_col) == MBTU),
            F.col(value_col) * THERM_TO_MBTU,
        )
        # From MBTU
        .when(
            (F.col(from_unit_col) == MBTU) & (F.col(to_unit_col) == KWH),
            F.col(value_col) * MBTU_TO_KWH,
        )
        .when(
            (F.col(from_unit_col) == MBTU) & (F.col(to_unit_col) == MWH),
            F.col(value_col) * MBTU_TO_MWH,
        )
        .when(
            (F.col(from_unit_col) == MBTU) & (F.col(to_unit_col) == GWH),
            F.col(value_col) * MBTU_TO_GWH,
        )
        .when(
            (F.col(from_unit_col) == MBTU) & (F.col(to_unit_col) == TWH),
            F.col(value_col) * MBTU_TO_TWH,
        )
        .when(
            (F.col(from_unit_col) == MBTU) & (F.col(to_unit_col) == THERM),
            F.col(value_col) * MBTU_TO_THERM,
        )
        .otherwise(None)
    )


def convert_units_pivoted(
    df, columns, from_records, from_to_records, to_unit_records
) -> DataFrame:
    """Convert the specified columns of the dataframe to the target units.

    Parameters
    ----------
    df : DataFrame
        Load data table
    columns : list[tuple]
        Columns in dataframe to convert (variable_column, value_column)
    from_records : DataFrame
        Metric dimension records for the columns being converted
    from_to_records : DataFrame | None
        Records that map the dimension IDs in columns to the target IDs
        If None, mapping is not required and from_records contain the units.
    to_unit_records : DataFrame
        Metric dimension records for the target IDs

    Returns
    -------
    DataFrame
    """
    from_unit_mapping = _map_metric_units(from_records, from_to_records)
    unit_col = "unit"  # must match EnergyEndUse.unit
    for column in columns:
        from_unit = from_unit_mapping[column]
        to_unit = _get_metric_unit(to_unit_records, column, unit_col)
        # Some datasets are unitless, such as AEO growth rates.
        if from_unit != "" and from_unit != to_unit:
            logger.info(
                "Converting column=%s units from %s to %s",
                column,
                from_unit,
                to_unit,
            )
            df = (
                df.withColumn(unit_col, F.lit(from_unit))
                .withColumn(column, _get_conversion_function(to_unit)(unit_col, column))
                .drop(unit_col)
            )

    return df


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
            tmp1.join(tmp2, on=tmp1["id"] == tmp2["from_id"])
            .selectExpr("to_id AS id", "from_unit")
            .distinct()
        )
    df = df.join(unit_df, on=df[metric_column] == unit_df["id"]).drop("id")
    tmp3 = to_unit_records.select("id", "unit").withColumnRenamed(unit_col, "to_unit")
    df = df.join(tmp3, on=df[metric_column] == tmp3["id"]).drop("id")
    logger.info("Converting units from column %s", metric_column)
    return df.withColumn(VALUE_COLUMN, from_any_to_any("from_unit", "to_unit", VALUE_COLUMN)).drop(
        "from_unit", "to_unit"
    )


_CONVERSION_FUNCTIONS = {
    KWH: to_kwh,
    MWH: to_mwh,
    GWH: to_gwh,
    TWH: to_twh,
    THERM: to_therm,
    MBTU: to_mbtu,
}


def _get_conversion_function(unit: str):
    func = _CONVERSION_FUNCTIONS.get(unit)
    if func is None:
        raise NotImplementedError(f"There is no conversion function for {unit=}")
    return func


def _get_metric_unit(records: DataFrame, dimension_id: str, unit_col: str) -> str:
    if unit_col not in records.columns:
        raise DSGInvalidDimension(f"{unit_col=} is not in records dataframe")

    vals = records.filter(f"id='{dimension_id}'").select(unit_col).collect()
    if not vals:
        raise DSGInvalidParameter(f"{dimension_id=} is not present in records dataframe")
    if len(vals) > 1:
        raise DSGInvalidParameter(f"{dimension_id=} has {len(vals)} entries in records dataframe")
    return vals[0][unit_col]


def _map_metric_units(
    metric_records: DataFrame, mapping_records: DataFrame | None
) -> dict[str, str]:
    if mapping_records is None:
        df = metric_records
    else:
        mappings = mapping_records.filter("to_id IS NOT NULL").select("from_id", "to_id")
        df = metric_records.join(mappings, on=metric_records.id == mappings.from_id).select(
            F.col("to_id").alias("id"), "unit"
        )
    return {x["id"]: x["unit"] for x in df.collect()}
