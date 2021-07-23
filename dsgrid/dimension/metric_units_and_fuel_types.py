"""Metric Units and Fuel Types
metric units to be standardized at the project level
"""

from enum import Enum


class EnergyUnit(Enum):
    """Acceptable units for energy demand"""

    MMBTU = "mmbtu"
    BTU = "btu"
    MWH = "mwh"
    KWH = "kwh"
    THERM = "therm"


class EnergyServiceUnit(Enum):
    """Acceptable units for energy service demand"""

    MMBTU = "mmbtu"
    BTU = "btu"
    LUMEN_ = "lumens"
    CFM_YRS = "cfm-years"  # ventilation
    LM_YRS = "lumen-years"


class PopulationUnit(Enum):
    """Acceptable units for population"""

    CAPITA = "capita"


class StockUnit(Enum):
    """Acceptable units for stock"""

    # GDP/Industry
    DOLLAR = "dollars"

    # Building stock
    BLDGS = "bldgs"  # no. of buildings
    BLDG_UNITS = "bldg_units"  # no. of building units
    SQFT = "sqft"

    # Equipment
    UNITS = "units"


class EnergyEfficiencyUnit(Enum):
    """Acceptable units for energy or conversion efficiency"""

    AFUE = "afue"
    BTU_PER_BTU = "btu_per_btu"
    COP = "cop"
    EER = "eer"
    EF = "ef"
    HSPF = "hspf"
    KWH_PER_YR = "kwh_per_yr"
    LM_PER_W = "lm_per_w"
    MEF = "mef"
    SEER = "seer"


class FuelType(Enum):
    """Acceptable fuel types"""

    ELECTRICITY = "electricity"
    NATURAL_GAS = "natural_gas"
    GASOLINE = "gasoline"
    DIESEL = "diesel"
    OTHER_FUELS = "other_fuels"
