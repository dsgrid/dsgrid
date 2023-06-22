"""Contains functions to convert electricity between units."""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from dsgrid.exceptions import DSGInvalidDimension, DSGInvalidParameter


KWH = "kWh"
MWH = "MWh"
GWH = "GWh"
TWH = "TWh"
THERM = "therm"
MBTU = "MBtu"
KWH_PER_THERM = 29.3001111
KWH_PER_MBTU = 0.293071
THERMS_PER_MBTU = 0.01

logger = logging.getLogger(__name__)


def to_kwh(unit_col, value_col):
    """Convert a column to kWh."""
    return (
        F.when(F.col(unit_col) == KWH, F.col(value_col))
        .when(F.col(unit_col) == MWH, (F.col(value_col) * 1_000))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * 1_000_000))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * 1_000_000_000))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * KWH_PER_THERM))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * KWH_PER_MBTU))
        .otherwise(None)
    )


def to_mwh(unit_col, value_col):
    """Convert a column to mWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) / 1_000))
        .when(F.col(unit_col) == MWH, F.col(value_col))
        .when(F.col(unit_col) == GWH, (F.col(value_col) * 1_000))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * 1_000_000))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * KWH_PER_THERM / 1_000))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * KWH_PER_MBTU / 1_000))
        .otherwise(None)
    )


def to_gwh(unit_col, value_col):
    """Convert a column to gWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) / 1_000_000))
        .when(F.col(unit_col) == MWH, (F.col(value_col) / 1_000))
        .when(F.col(unit_col) == GWH, F.col(value_col))
        .when(F.col(unit_col) == TWH, (F.col(value_col) * 1_000))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * KWH_PER_THERM / 1_000_000))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * KWH_PER_MBTU / 1_000_000))
        .otherwise(None)
    )


def to_twh(unit_col, value_col):
    """Convert a column to tWh."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) / 1_000_000_000))
        .when(F.col(unit_col) == MWH, (F.col(value_col) / 1_000_000))
        .when(F.col(unit_col) == GWH, (F.col(value_col) / 1_000))
        .when(F.col(unit_col) == TWH, F.col(value_col))
        .when(F.col(unit_col) == THERM, (F.col(value_col) * KWH_PER_THERM / 1_000_000_000))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * KWH_PER_MBTU / 1_000_000_000))
        .otherwise(None)
    )


def to_therm(unit_col, value_col):
    """Convert a column to therm."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) / KWH_PER_THERM))
        .when(F.col(unit_col) == MWH, (F.col(value_col) / KWH_PER_THERM / 1_000))
        .when(F.col(unit_col) == GWH, (F.col(value_col) / KWH_PER_THERM / 1_000_000))
        .when(F.col(unit_col) == TWH, (F.col(value_col) / KWH_PER_THERM / 1_000_000_000))
        .when(F.col(unit_col) == THERM, F.col(value_col))
        .when(F.col(unit_col) == MBTU, (F.col(value_col) * THERMS_PER_MBTU))
        .otherwise(None)
    )


def to_mbtu(unit_col, value_col):
    """Convert a column to MBtu."""
    return (
        F.when(F.col(unit_col) == KWH, (F.col(value_col) / KWH_PER_MBTU))
        .when(F.col(unit_col) == MWH, (F.col(value_col) / KWH_PER_MBTU / 1_000))
        .when(F.col(unit_col) == GWH, (F.col(value_col) / KWH_PER_MBTU / 1_000_000))
        .when(F.col(unit_col) == TWH, (F.col(value_col) / KWH_PER_MBTU / 1_000_000_000))
        .when(F.col(unit_col) == THERM, (F.col(value_col) / THERMS_PER_MBTU))
        .when(F.col(unit_col) == MBTU, F.col(value_col))
        .otherwise(None)
    )


def convert_units(df, columns, from_records, from_to_records, to_unit_records) -> DataFrame:
    """Convert the specified columns of the dataframe to the target units.

    Parameters
    ----------
    df : DataFrame
        Load data table
    columns : list[str]
        Columns in dataframe to convert
    from_records : DataFrame
        Metric dimension records for the columns being converted
    from_to_records : DataFrame
        Records that map the dimension IDs in columns to the target IDs
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


def _map_metric_units(metric_records: DataFrame, mapping_records: DataFrame) -> dict[str, str]:
    mappings = mapping_records.filter("to_id IS NOT NULL").select("from_id", "to_id")
    df = metric_records.join(mappings, on=metric_records.id == mappings.from_id).select(
        F.col("to_id").alias("id"), "unit"
    )
    return {x["id"]: x["unit"] for x in df.collect()}
