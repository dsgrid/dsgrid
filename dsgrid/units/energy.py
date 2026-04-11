"""Contains functions to perform unit conversion of energy."""

import logging

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


def to_kwh(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to kWh."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: 1.0,
            MWH: MEGA_TO_KILO,
            GWH: GIGA_TO_KILO,
            TWH: TERA_TO_KILO,
            THERM: THERM_TO_KWH,
            MBTU: MBTU_TO_KWH,
        },
    )


def to_mwh(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to MWh."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: KILO_TO_MEGA,
            MWH: 1.0,
            GWH: GIGA_TO_MEGA,
            TWH: TERA_TO_MEGA,
            THERM: THERM_TO_MWH,
            MBTU: MBTU_TO_MWH,
        },
    )


def to_gwh(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to GWh."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: KILO_TO_GIGA,
            MWH: MEGA_TO_GIGA,
            GWH: 1.0,
            TWH: TERA_TO_GIGA,
            THERM: THERM_TO_GWH,
            MBTU: MBTU_TO_GWH,
        },
    )


def to_twh(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to TWh."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: KILO_TO_TERA,
            MWH: MEGA_TO_TERA,
            GWH: GIGA_TO_TERA,
            TWH: 1.0,
            THERM: THERM_TO_TWH,
            MBTU: MBTU_TO_TWH,
        },
    )


def to_therm(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to therm."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: KWH_TO_THERM,
            MWH: MWH_TO_THERM,
            GWH: GWH_TO_THERM,
            TWH: TWH_TO_THERM,
            THERM: 1.0,
            MBTU: MBTU_TO_THERM,
        },
    )


def to_mbtu(unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column to MBtu."""
    return _to_unit(
        unit_col,
        value_col,
        {
            KWH: KWH_TO_MBTU,
            MWH: MWH_TO_MBTU,
            GWH: GWH_TO_MBTU,
            TWH: TWH_TO_MBTU,
            THERM: THERM_TO_MBTU,
            MBTU: 1.0,
        },
    )


def from_any_to_any(from_unit_col: str, to_unit_col: str, value_col: str) -> str:
    """Return a SQL expression that converts a column of energy based on from/to columns."""
    return _from_any_to_any(
        from_unit_col,
        to_unit_col,
        value_col,
        {
            KWH: 1.0,
            MWH: MEGA_TO_KILO,
            GWH: GIGA_TO_KILO,
            TWH: TERA_TO_KILO,
            THERM: THERM_TO_KWH,
            MBTU: MBTU_TO_KWH,
        },
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
