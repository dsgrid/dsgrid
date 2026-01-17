"""Contains functions to perform unit conversion of power."""

import logging

import ibis

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


def to_kw(unit_col, value_col):
    """Convert a column to kW."""
    return ibis.cases(
        (unit_col == KW, value_col),
        (unit_col == MW, value_col * MEGA_TO_KILO),
        (unit_col == GW, value_col * GIGA_TO_KILO),
        (unit_col == TW, value_col * TERA_TO_KILO),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_mw(unit_col, value_col):
    """Convert a column to MW."""
    return ibis.cases(
        (unit_col == KW, value_col * KILO_TO_MEGA),
        (unit_col == MW, value_col),
        (unit_col == GW, value_col * GIGA_TO_MEGA),
        (unit_col == TW, value_col * TERA_TO_MEGA),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_gw(unit_col, value_col):
    """Convert a column to GW."""
    return ibis.cases(
        (unit_col == KW, value_col * KILO_TO_GIGA),
        (unit_col == MW, value_col * MEGA_TO_GIGA),
        (unit_col == GW, value_col),
        (unit_col == TW, value_col * TERA_TO_GIGA),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def to_tw(unit_col, value_col):
    """Convert a column to TW."""
    return ibis.cases(
        (unit_col == KW, value_col * KILO_TO_TERA),
        (unit_col == MW, value_col * MEGA_TO_TERA),
        (unit_col == GW, value_col * GIGA_TO_TERA),
        (unit_col == TW, value_col),
        (unit_col == "", value_col),
        else_=ibis.null(),
    )


def from_any_to_any(from_unit_col, to_unit_col, value_col):
    """Convert a column of power based on from/to columns."""
    return ibis.cases(
        (from_unit_col == to_unit_col, value_col),
        (from_unit_col == "", value_col),
        (to_unit_col == KW, to_kw(from_unit_col, value_col)),
        (to_unit_col == MW, to_mw(from_unit_col, value_col)),
        (to_unit_col == GW, to_gw(from_unit_col, value_col)),
        (to_unit_col == TW, to_tw(from_unit_col, value_col)),
        else_=ibis.null(),
    )
