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

KWH = "kWh"
MWH = "MWh"
GWH = "GWh"
TWH = "TWh"
THERM = "therm"
MBTU = "MBtu"

KW = "kW"
MW = "MW"
GW = "GW"
TW = "TW"

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

ENERGY_UNITS = (KWH, MWH, GWH, TWH, THERM, MBTU)
POWER_UNITS = (KW, MW, GW, TW)


# Constants for unit conversions
__all__ = (
    "ENERGY_UNITS",
    "POWER_UNITS",
    "KILO",
    "MEGA",
    "GIGA",
    "TERA",
    "KILO_TO_MEGA",
    "KILO_TO_GIGA",
    "KILO_TO_TERA",
    "MEGA_TO_KILO",
    "MEGA_TO_GIGA",
    "MEGA_TO_TERA",
    "GIGA_TO_KILO",
    "GIGA_TO_MEGA",
    "GIGA_TO_TERA",
    "TERA_TO_KILO",
    "TERA_TO_MEGA",
    "TERA_TO_GIGA",
    "KWH",
    "MWH",
    "GWH",
    "TWH",
    "THERM",
    "MBTU",
    "KW",
    "MW",
    "GW",
    "TW",
    "THERM_TO_KWH",
    "THERM_TO_MWH",
    "THERM_TO_GWH",
    "THERM_TO_TWH",
    "KWH_TO_THERM",
    "MWH_TO_THERM",
    "GWH_TO_THERM",
    "TWH_TO_THERM",
    "MBTU_TO_KWH",
    "MBTU_TO_MWH",
    "MBTU_TO_GWH",
    "MBTU_TO_TWH",
    "KWH_TO_MBTU",
    "MWH_TO_MBTU",
    "GWH_TO_MBTU",
    "TWH_TO_MBTU",
    "MBTU_TO_THERM",
    "THERM_TO_MBTU",
)
