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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dsgrid.config.dimensions import TimeDimensionModel
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


class EnergyEndUseOrm(BaseOrm):
    __tablename__ = "EnergyEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)
    fuel_id = Column(String(255), nullable=False)


class EnergyServiceEndUse(MetricDimensionBaseModel):
    """Energy Service Demand End Use attributes"""

    unit: str


class EnergyServiceEndUseOrm(BaseOrm):
    __tablename__ = "EnergyServiceEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class Population(MetricDimensionBaseModel):
    """Population attributes"""

    unit: str


class PopulationOrm(BaseOrm):
    __tablename__ = "Population"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class Stock(MetricDimensionBaseModel):
    """Stock attributes - includes GDP, building stock, equipment"""

    unit: str


class StockOrm(BaseOrm):
    __tablename__ = "Stock"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class EnergyEfficiency(MetricDimensionBaseModel):
    """Energy Efficiency of building stock or equipment"""

    unit: str


class EnergyEfficiencyOrm(BaseOrm):
    __tablename__ = "EnergyEfficiency"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


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

    subsector = relationship(
        "SubsectorOrm",
        secondary=model_subsector_association,
        back_populates="model",
    )


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""
