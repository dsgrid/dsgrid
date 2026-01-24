"""Contains functions to perform unit conversion of energy."""

import logging

import ibis

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


def to_kwh(unit_col, value_col):
    """Convert a column to kWh."""
    return ibis.cases(
        (unit_col == KWH, value_col),
        (unit_col == MWH, value_col * MEGA_TO_KILO),
        (unit_col == GWH, value_col * GIGA_TO_KILO),
        (unit_col == TWH, value_col * TERA_TO_KILO),
        (unit_col == THERM, value_col * THERM_TO_KWH),
        (unit_col == MBTU, value_col * MBTU_TO_KWH),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_mwh(unit_col, value_col):
    """Convert a column to mWh."""
    return ibis.cases(
        (unit_col == KWH, value_col * KILO_TO_MEGA),
        (unit_col == MWH, value_col),
        (unit_col == GWH, value_col * GIGA_TO_MEGA),
        (unit_col == TWH, value_col * TERA_TO_MEGA),
        (unit_col == THERM, value_col * THERM_TO_MWH),
        (unit_col == MBTU, value_col * MBTU_TO_MWH),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_gwh(unit_col, value_col):
    """Convert a column to gWh."""
    return ibis.cases(
        (unit_col == KWH, value_col * KILO_TO_GIGA),
        (unit_col == MWH, value_col * MEGA_TO_GIGA),
        (unit_col == GWH, value_col),
        (unit_col == TWH, value_col * TERA_TO_GIGA),
        (unit_col == THERM, value_col * THERM_TO_GWH),
        (unit_col == MBTU, value_col * MBTU_TO_GWH),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_twh(unit_col, value_col):
    """Convert a column to tWh."""
    return ibis.cases(
        (unit_col == KWH, value_col * KILO_TO_TERA),
        (unit_col == MWH, value_col * MEGA_TO_TERA),
        (unit_col == GWH, value_col * GIGA_TO_TERA),
        (unit_col == TWH, value_col),
        (unit_col == THERM, value_col * THERM_TO_TWH),
        (unit_col == MBTU, value_col * MBTU_TO_TWH),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_therm(unit_col, value_col):
    """Convert a column to therm."""
    return ibis.cases(
        (unit_col == KWH, value_col * KWH_TO_THERM),
        (unit_col == MWH, value_col * MWH_TO_THERM),
        (unit_col == GWH, value_col * GWH_TO_THERM),
        (unit_col == TWH, value_col * TWH_TO_THERM),
        (unit_col == THERM, value_col),
        (unit_col == MBTU, value_col * MBTU_TO_THERM),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_mbtu(unit_col, value_col):
    """Convert a column to MBtu."""
    return ibis.cases(
        (unit_col == KWH, value_col * KWH_TO_MBTU),
        (unit_col == MWH, value_col * MWH_TO_MBTU),
        (unit_col == GWH, value_col * GWH_TO_MBTU),
        (unit_col == TWH, value_col * TWH_TO_MBTU),
        (unit_col == THERM, value_col * THERM_TO_MBTU),
        (unit_col == MBTU, value_col),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def from_any_to_any(from_unit_col, to_unit_col, value_col):
    """Convert a column of energy based on from/to columns."""
    return ibis.cases(
        (from_unit_col == to_unit_col, value_col),
        (from_unit_col == "", value_col),
        (to_unit_col == KWH, to_kwh(from_unit_col, value_col)),
        (to_unit_col == MWH, to_mwh(from_unit_col, value_col)),
        (to_unit_col == GWH, to_gwh(from_unit_col, value_col)),
        (to_unit_col == TWH, to_twh(from_unit_col, value_col)),
        (to_unit_col == THERM, to_therm(from_unit_col, value_col)),
        (to_unit_col == MBTU, to_mbtu(from_unit_col, value_col)),
        else_=ibis.null(),
    )
