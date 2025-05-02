"""Contains functions to perform unit conversion of energy."""

import logging

from dsgrid.spark.types import DataFrame, F
from dsgrid.units.constants import (
    GIGA_TO_KILO,
    GIGA_TO_MEGA,
    GIGA_TO_TERA,
    GWH,
    GWH_TO_MBTU,
    GWH_TO_THERM,
    KILO_TO_GIGA,
    KILO_TO_MEGA,
    KILO_TO_TERA,
    KWH,
    KWH_TO_MBTU,
    KWH_TO_THERM,
    MBTU,
    MBTU_TO_GWH,
    MBTU_TO_KWH,
    MBTU_TO_MWH,
    MBTU_TO_THERM,
    MBTU_TO_TWH,
    MEGA_TO_GIGA,
    MEGA_TO_KILO,
    MEGA_TO_TERA,
    MWH,
    MWH_TO_MBTU,
    MWH_TO_THERM,
    TERA_TO_GIGA,
    TERA_TO_KILO,
    TERA_TO_MEGA,
    THERM,
    THERM_TO_GWH,
    THERM_TO_KWH,
    THERM_TO_MBTU,
    THERM_TO_MWH,
    THERM_TO_TWH,
    TWH,
    TWH_TO_MBTU,
    TWH_TO_THERM,
)


logger = logging.getLogger(__name__)


def to_kwh(unit_col: str, value_col: str) -> DataFrame:
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


def to_mwh(unit_col: str, value_col: str) -> DataFrame:
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


def to_gwh(unit_col: str, value_col: str) -> DataFrame:
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


def to_twh(unit_col: str, value_col: str) -> DataFrame:
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


def to_therm(unit_col: str, value_col: str) -> DataFrame:
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


def to_mbtu(unit_col: str, value_col: str) -> DataFrame:
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


def from_any_to_any(from_unit_col: str, to_unit_col: str, value_col: str) -> DataFrame:
    """Convert a column of energy based on from/to columns."""
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
