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
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dsgrid.config.dimensions import TimeDimensionModel
from dsgrid.dimension.base_models import (
    MetricDimensionBaseModel,
    # EndUseDimensionBaseModel, # <---- delete
    GeographyDimensionBaseModel,
    DataSourceDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubsectorDimensionBaseModel,
    WeatherYearDimensionBaseModel,
)
from dsgrid.dimension.metric_units_and_fuel_types import (
    FuelType,
)

BaseOrm = declarative_base()


metric_model_association = Table(
    "metric_model",
    BaseOrm.metadata,
    Column("metric", String(255), ForeignKey("Metric.id")),
    Column("data_source", String(255), ForeignKey("DataSource.id")),
)


model_subsector_association = Table(
    "model_subsector",
    BaseOrm.metadata,
    Column("data_source", String(255), ForeignKey("DataSource.id")),
    Column("subsector", String(255), ForeignKey("Subsector.id")),
)


subsector_sector_association = Table(
    "subsector_sector",
    BaseOrm.metadata,
    Column("subsector", String(255), ForeignKey("Subsector.id")),
    Column("sector", String(255), ForeignKey("Sector.id")),
)


# ---------------------------
# GEOGRAPHIC DIMENSIONS
# ---------------------------
class CensusDivision(GeographyDimensionBaseModel):
    """Census Region attributes"""


class CensusDivisionOrm(BaseOrm):
    __tablename__ = "CensusDivision"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)


class CensusRegion(GeographyDimensionBaseModel):
    """Census Region attributes"""


class CensusRegionOrm(BaseOrm):
    __tablename__ = "CensusRegion"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)


class State(GeographyDimensionBaseModel):
    """State attributes"""

    is_conus: bool
    census_division: str = ""
    census_region: str = ""


class StateOrm(BaseOrm):
    __tablename__ = "State"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    is_conus = Column(Boolean, nullable=False)
    census_division = Column(String(255), nullable=False)
    census_region = Column(String(255), nullable=False)

    counties = relationship("CountyOrm", back_populates="state_rel")


class County(GeographyDimensionBaseModel):
    """County attributes"""

    state: str


class CountyOrm(BaseOrm):
    __tablename__ = "County"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    state = Column(String(255), ForeignKey("State.id"), nullable=False)
    timezone = Column(String(255), nullable=False)

    state_rel = relationship("StateOrm", back_populates="counties")


# ---------------------------
# SECTOR DIMENSIONS
# ---------------------------
class Sector(SectorDimensionBaseModel):
    """Sector attributes"""

    category: Optional[str] = Field(
        title="sector",
        description="sector dimension",
        default="",
    )


class SectorOrm(BaseOrm):
    __tablename__ = "Sector"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)

    subsector = relationship(
        "SubsectorOrm",
        secondary=subsector_sector_association,
        back_populates="sector",
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


class SubsectorOrm(BaseOrm):
    __tablename__ = "Subsector"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(255), nullable=False)
    abbr = Column(String(255), nullable=False)

    model = relationship(
        "DataSourceOrm",
        secondary=model_subsector_association,
        back_populates="subsector",
    )
    sector = relationship(
        "SectorOrm",
        secondary=subsector_sector_association,
        back_populates="subsector",
    )


# ---------------------------
# METRIC DIMENSIONS
# ---------------------------
# class EndUse(EndUseDimensionBaseModel): # <---- delete
#     """End Use"""
#     fuel_id: str
#     units: str


class EnergyEndUse(MetricDimensionBaseModel):
    """Energy Demand End Use attributes"""

    fuel_id: FuelType = Field(title="fuel_id", description="fuel type, e.g., electricity")
    unit: str


class EnergyEndUseOrm(BaseOrm):
    __tablename__ = "EnergyEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)
    fuel_id = Column(String(255), nullable=False)

    model_energy = relationship(
        "DataSourceOrm",
        secondary=metric_model_association,
        back_populates="metric_energy",
    )


class EnergyServiceEndUse(MetricDimensionBaseModel):
    """Energy Service Demand End Use attributes"""

    unit: str


class EnergyServiceEndUseOrm(BaseOrm):
    __tablename__ = "EnergyServiceEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)

    model_service = relationship(
        "DataSourceOrm",
        secondary=metric_model_association,
        back_populates="metric_service",
    )


class Population(MetricDimensionBaseModel):
    """Population attributes"""

    unit: str


class PopulationOrm(BaseOrm):
    __tablename__ = "Population"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)

    model_population = relationship(
        "DataSourceOrm",
        secondary=metric_model_association,
        back_populates="metric_population",
    )


class Stock(MetricDimensionBaseModel):
    """Stock attributes - includes GDP, building stock, equipment"""

    unit: str


class StockOrm(BaseOrm):
    __tablename__ = "Stock"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)

    model_stock = relationship(
        "DataSourceOrm",
        secondary=metric_model_association,
        back_populates="metric_stock",
    )


class EnergyEfficiency(MetricDimensionBaseModel):
    """Energy Efficiency of building stock or equipment"""

    unit: str


class EnergyEfficiencyOrm(BaseOrm):
    __tablename__ = "EnergyEfficiency"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)

    model_efficiency = relationship(
        "DataSourceOrm",
        secondary=metric_model_association,
        back_populates="metric_efficiency",
    )


# ---------------------------
# TIME DIMENSIONS
# ---------------------------
class Time(TimeDimensionModel):
    """Time attributes"""


class Timezone(TimeDimensionModel):
    """Timezone attributes"""


class DayType(TimeDimensionModel):
    """Day Type attributes"""


class Season(TimeDimensionModel):
    """Season attributes"""


# ---------------------------
# OTHER DIMENSIONS
# ---------------------------
class WeatherYear(WeatherYearDimensionBaseModel):
    """Weather Year attributes"""


class ModelYear(ModelYearDimensionBaseModel):
    """Model Year attributes"""


class DataSource(DataSourceDimensionBaseModel):
    """DataSource attributes"""


class DataSourceOrm(BaseOrm):
    __tablename__ = "DataSource"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)

    metric_energy = relationship(
        "EnergyEndUseOrm",
        secondary=metric_model_association,
        back_populates="model_energy",
    )
    metric_service = relationship(
        "EnergyServiceEndUseOrm",
        secondary=metric_model_association,
        back_populates="model_service",
    )
    metric_population = relationship(
        "PopulationOrm",
        secondary=metric_model_association,
        back_populates="model_population",
    )
    metric_stock = relationship(
        "StockOrm",
        secondary=metric_model_association,
        back_populates="model_stock",
    )
    metric_efficiency = relationship(
        "EnergyEfficiencyOrm",
        secondary=metric_model_association,
        back_populates="model_efficiency",
    )
    subsector = relationship(
        "SubsectorOrm",
        secondary=model_subsector_association,
        back_populates="model",
    )


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""


class ScenarioOrm(BaseOrm):
    __tablename__ = "Scenario"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
