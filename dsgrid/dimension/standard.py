"""Standard dimension classes for dsgrid"""

from typing import Optional, Union

from pydantic import Field
from pydantic import validator
from sqlalchemy import (
    Column,
    Boolean,
    String,
    ForeignKey,
    Table,
    select,
    text,  # Integer, Text, DateTime,
)

from dsgrid.config.dimensions import DateTimeDimensionModel, AnnualTimeDimensionModel
from dsgrid.dimension.base_models import (
    MetricDimensionBaseModel,
    GeographyDimensionBaseModel,
    DataSourceDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubsectorDimensionBaseModel,
    WeatherYearDimensionBaseModel,
)


# ---------------------------
# GEOGRAPHIC DIMENSIONS
# ---------------------------
class CensusDivision(GeographyDimensionBaseModel):
    """Census Region attributes"""


class CensusRegion(GeographyDimensionBaseModel):
    """Census Region attributes"""


class State(GeographyDimensionBaseModel):
    """State attributes"""

    is_conus: bool
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

    category: Optional[str] = Field(
        title="sector",
        description="Sector dimension",
        default="",
    )


# ---------------------------
# SUBSECTOR DIMENSIONS
# ---------------------------
class Subsector(SubsectorDimensionBaseModel):
    """Subsector attributes"""

    # NOTE: making sector optional for now, we may remove because it should be
    #   handled in the association tables
    sector: Optional[str] = Field(
        default="",
    )
    abbr: Optional[str] = Field(
        default="",
    )

    @validator("abbr", pre=True)
    def validate_abbr(cls, value: Union[str, None]) -> str:
        return value or ""


# ---------------------------
# METRIC DIMENSIONS
# ---------------------------
class EnergyEndUse(MetricDimensionBaseModel):
    """Energy Demand End Use attributes"""

    fuel_id: str
    unit: str


class EnergyServiceEndUse(MetricDimensionBaseModel):
    """Energy Service Demand End Use attributes"""

    unit: str


class Population(MetricDimensionBaseModel):
    """Population attributes"""

    unit: str


class Stock(MetricDimensionBaseModel):
    """Stock attributes - includes GDP, building stock, equipment"""

    unit: str


class EnergyEfficiency(MetricDimensionBaseModel):
    """Energy Efficiency of building stock or equipment"""

    unit: str


# ---------------------------
# TIME DIMENSIONS
# ---------------------------
class Time(DateTimeDimensionModel):
    """Time attributes"""


# It is unclear if we need the next few classes. They would need model definitions in
# order to be used.

#class Timezone(TimeDimensionBaseModel):
#    """Timezone attributes"""
#
#
#class DayType(TimeDimensionModel):
#    """Day Type attributes"""
#
#
#class Season(TimeDimensionModel):
#    """Season attributes"""


class AnnualTime(AnnualTimeDimensionModel):
    """Annual Time attributes"""


# ---------------------------
# OTHER DIMENSIONS
# ---------------------------
class WeatherYear(WeatherYearDimensionBaseModel):
    """Weather Year attributes"""


class ModelYear(ModelYearDimensionBaseModel):
    """Model Year attributes"""


class DataSource(DataSourceDimensionBaseModel):
    """DataSource attributes"""


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""
