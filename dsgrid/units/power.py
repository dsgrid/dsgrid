"""Contains functions to perform unit conversion of power."""

import logging

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


def to_kw(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to kW."""
    return _to_unit(
        unit_col, value_col, {KW: 1.0, MW: MEGA_TO_KILO, GW: GIGA_TO_KILO, TW: TERA_TO_KILO}
    )


def to_mw(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to MW."""
    return _to_unit(
        unit_col, value_col, {KW: KILO_TO_MEGA, MW: 1.0, GW: GIGA_TO_MEGA, TW: TERA_TO_MEGA}
    )


def to_gw(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to GW."""
    return _to_unit(
        unit_col, value_col, {KW: KILO_TO_GIGA, MW: MEGA_TO_GIGA, GW: 1.0, TW: TERA_TO_GIGA}
    )


def to_tw(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to TW."""
    return _to_unit(
        unit_col, value_col, {KW: KILO_TO_TERA, MW: MEGA_TO_TERA, GW: GIGA_TO_TERA, TW: 1.0}
    )


def from_any_to_any(from_unit_col: str, to_unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column of power based on from/to columns."""
    return _from_any_to_any(
        from_unit_col,
        to_unit_col,
        value_col,
        {KW: 1.0, MW: MEGA_TO_KILO, GW: GIGA_TO_KILO, TW: TERA_TO_KILO},
    )


def _to_unit(unit_col: str, value_col: str, factors: dict[str, float]) -> str:
    cases = [
        f"WHEN {unit_col} = '{unit}' THEN {value_col} * {factor}"
        for unit, factor in factors.items()
    ]
    cases.append(f"WHEN {unit_col} = '' THEN {value_col}")
    return "CASE " + " ".join(cases) + " ELSE NULL END"


def _from_any_to_any(
    from_unit_col: str,
    to_unit_col: str,
    value_col: str,
    unit_to_base: dict[str, float],
) -> str:
    cases = [
        f"WHEN {from_unit_col} = {to_unit_col} THEN {value_col}",
        f"WHEN {from_unit_col} = '' THEN {value_col}",
    ]
    for to_unit, to_factor in unit_to_base.items():
        for from_unit, from_factor in unit_to_base.items():
            if from_unit == to_unit:
                continue
            cases.append(
                f"WHEN {to_unit_col} = '{to_unit}' AND {from_unit_col} = '{from_unit}' "
                f"THEN {value_col} * {from_factor / to_factor}"
            )
    return "CASE " + " ".join(cases) + " ELSE NULL END"
