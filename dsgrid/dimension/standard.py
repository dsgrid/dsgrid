"""Standard dimension classes for dsgrid"""

from enum import StrEnum

from pydantic import Field

from dsgrid.config.dimensions import (
    DateTimeDimensionModel,
    AnnualTimeDimensionModel,
    NoOpTimeDimensionModel,
)
from dsgrid.dimension.base_models import (
    MetricDimensionBaseModel,
    GeographyDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubsectorDimensionBaseModel,
    WeatherYearDimensionBaseModel,
)


# ---------------------------
# GEOGRAPHIC DIMENSIONS
# ---------------------------


class Geography(GeographyDimensionBaseModel):
    """Generic geography with optional time_zone"""


# TODO: Deprecate. Replace instances with Geography
class CensusDivision(GeographyDimensionBaseModel):
    """Census Region attributes"""


# TODO: Deprecate. Replace instances with Geography
class CensusRegion(GeographyDimensionBaseModel):
    """Census Region attributes"""


class State(GeographyDimensionBaseModel):
    """State attributes"""

    is_conus: bool | None = None
    census_division: str = ""
    census_region: str = ""


class County(GeographyDimensionBaseModel):
    """County attributes"""

    state: str


# ---------------------------
# SECTOR DIMENSIONS
# ---------------------------


class Sector(SectorDimensionBaseModel):
    """Sector attributes"""

    category: str = Field(
        title="sector",
        description="Sector dimension",
        default="",
    )


# ---------------------------
# SUBSECTOR DIMENSIONS
# ---------------------------


class Subsector(SubsectorDimensionBaseModel):
    """Subsector attributes"""

    sector: str = ""
    abbr: str = ""


# ---------------------------
# METRIC DIMENSIONS
# ---------------------------


class FunctionalForm(StrEnum):
    """Functional forms for regression parameters"""

    # y = a0 + a1 * x + a2 * x^2 + ...
    LINEAR = "linear"
    # ln y = a0 + a1 * x + a2 * x^2 + ...
    # y = exp(a0 + a1 * x + a2 * x^2 + ...)
    EXPONENTIAL = "exponential"


class EnergyEndUse(MetricDimensionBaseModel):
    """Energy Demand End Use attributes"""

    fuel_id: str
    unit: str


class EnergyServiceDemand(MetricDimensionBaseModel):
    """Energy Service Demand attributes"""

    unit: str


class EnergyServiceDemandRegression(MetricDimensionBaseModel):
    """Energy Service Demand, can be per floor area, vehicle, etc., regression
    over time or other variables
    """

    regression_type: FunctionalForm = Field(
        default=FunctionalForm.LINEAR,
        description="Specifies the functional form of the regression model",
    )
    unit: str


class EnergyEfficiency(MetricDimensionBaseModel):
    """Energy Efficiency of building stock or equipment"""

    fuel_id: str
    unit: str


class EnergyIntensityRegression(MetricDimensionBaseModel):
    """Energy Intensity per capita, GDP, etc. regression over time or other variables"""

    regression_type: FunctionalForm = Field(
        default=FunctionalForm.LINEAR,
        description="Specifies the functional form of the regression model",
    )
    unit: str


class EnergyIntensity(MetricDimensionBaseModel):
    """Energy Intensity per capita, GDP, etc."""

    unit: str


class Population(MetricDimensionBaseModel):
    """Population attributes"""

    unit: str


class Stock(MetricDimensionBaseModel):
    """Stock attributes - e.g., GDP, building stock, equipment"""

    unit: str


class StockRegression(MetricDimensionBaseModel):
    """Stock, can be per capita, GDP, etc., regression over time or other variables"""

    regression_type: FunctionalForm = Field(
        default=FunctionalForm.LINEAR,
        description="Specifies the functional form of the regression model",
    )
    unit: str


class StockShare(MetricDimensionBaseModel):
    """Stock Share attributes - e.g., market share of a technology

    Generally dimensionless, but a unit string can be provided to assist with
    calculations.
    """

    unit: str


class FractionalIndex(MetricDimensionBaseModel):
    """Fractional Index attributes - e.g., human development index (HDI)

    Generally dimensionless, but a unit string can be provided to assist with
    calculations.
    """

    unit: str
    min_value: float
    max_value: float


class PeggedIndex(MetricDimensionBaseModel):
    """Pegged Index attributes

    Data relative to a base year that is normalized to a value like 1 or 100.

    Generally dimensionless, but a unit string can be provided to assist with
    calculations.
    """

    unit: str
    base_year: int
    base_value: float


# ---------------------------
# TIME DIMENSIONS
# ---------------------------


class Time(DateTimeDimensionModel):
    """Time attributes"""


# It is unclear if we need the next few classes. They would need model definitions in
# order to be used.
#
# class DayType(TimeDimensionModel):
#    """Day Type attributes"""
#
#
# class Season(TimeDimensionModel):
#    """Season attributes"""


class AnnualTime(AnnualTimeDimensionModel):
    """Annual Time attributes"""


class NoOpTime(NoOpTimeDimensionModel):
    """NoOp Time attributes"""


# ---------------------------
# OTHER DIMENSIONS
# ---------------------------


class WeatherYear(WeatherYearDimensionBaseModel):
    """Weather Year attributes"""


class ModelYear(ModelYearDimensionBaseModel):
    """Model Year attributes"""


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""
