"""Contains functions to perform unit conversion of power."""

import logging

from dsgrid.spark.types import DataFrame, F
from dsgrid.units.constants import (
    GIGA_TO_KILO,
    GIGA_TO_MEGA,
    GIGA_TO_TERA,
    GW,
    KILO_TO_GIGA,
    KILO_TO_MEGA,
    KILO_TO_TERA,
    KW,
    MEGA_TO_GIGA,
    MEGA_TO_KILO,
    MEGA_TO_TERA,
    MW,
    TERA_TO_GIGA,
    TERA_TO_KILO,
    TERA_TO_MEGA,
    TW,
)


logger = logging.getLogger(__name__)


def to_kw(unit_col: str, value_col: str) -> DataFrame:
    """Convert a column to kW."""
    return (
        F.when(F.col(unit_col) == KW, F.col(value_col))
        .when(F.col(unit_col) == MW, (F.col(value_col) * MEGA_TO_KILO))
        .when(F.col(unit_col) == GW, (F.col(value_col) * GIGA_TO_KILO))
        .when(F.col(unit_col) == TW, (F.col(value_col) * TERA_TO_KILO))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_mw(unit_col: str, value_col: str) -> DataFrame:
    """Convert a column to MW."""
    return (
        F.when(F.col(unit_col) == KW, (F.col(value_col) * KILO_TO_MEGA))
        .when(F.col(unit_col) == MW, F.col(value_col))
        .when(F.col(unit_col) == GW, (F.col(value_col) * GIGA_TO_MEGA))
        .when(F.col(unit_col) == TW, (F.col(value_col) * TERA_TO_MEGA))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_gw(unit_col: str, value_col: str) -> DataFrame:
    """Convert a column to GW."""
    return (
        F.when(F.col(unit_col) == KW, (F.col(value_col) * KILO_TO_GIGA))
        .when(F.col(unit_col) == MW, (F.col(value_col) * MEGA_TO_GIGA))
        .when(F.col(unit_col) == GW, F.col(value_col))
        .when(F.col(unit_col) == TW, (F.col(value_col) * TERA_TO_GIGA))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def to_tw(unit_col: str, value_col: str) -> DataFrame:
    """Convert a column to TW."""
    return (
        F.when(F.col(unit_col) == KW, (F.col(value_col) * KILO_TO_TERA))
        .when(F.col(unit_col) == MW, (F.col(value_col) * MEGA_TO_TERA))
        .when(F.col(unit_col) == GW, (F.col(value_col) * GIGA_TO_TERA))
        .when(F.col(unit_col) == TW, F.col(value_col))
        .when(F.col(unit_col) == "", F.col(value_col))
        .otherwise(None)
    )


def from_any_to_any(from_unit_col: str, to_unit_col: str, value_col: str) -> DataFrame:
    """Convert a column of power based on from/to columns."""
    return (
        F.when(F.col(from_unit_col) == F.col(to_unit_col), F.col(value_col))
        .when(F.col(from_unit_col) == "", F.col(value_col))
        .when(F.col(to_unit_col) == KW, to_kw(from_unit_col, value_col))
        .when(F.col(to_unit_col) == MW, to_mw(from_unit_col, value_col))
        .when(F.col(to_unit_col) == GW, to_gw(from_unit_col, value_col))
        .when(F.col(to_unit_col) == TW, to_tw(from_unit_col, value_col))
        .otherwise(None)
    )
