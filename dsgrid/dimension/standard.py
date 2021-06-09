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
    EndUseDimensionBaseModel,
    GeographyDimensionBaseModel,
    DataSourceDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubsectorDimensionBaseModel,
    WeatherYearDimensionBaseModel,
)

BaseOrm = declarative_base()


enduse_model_association = Table(
    "enduse_model",
    BaseOrm.metadata,
    Column("enduse", String(255), ForeignKey("EndUse.id")),
    Column("data_source", String(255), ForeignKey("DataSource.id")),
)
# FIXME: @dtom should this be data_source or datasource? Repeat fix.


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
# ENDUSE DIMENSIONS
# ---------------------------
class EndUse(EndUseDimensionBaseModel):
    """End use attributes"""

    # sector: str  # TODO: the raw data doesn't have this field
    fuel_id: str
    units: str


class EndUseOrm(BaseOrm):
    __tablename__ = "EndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    fuel_id = Column(String(255), nullable=False)
    units = Column(String(255), nullable=False)

    model = relationship(
        "DataSourceOrm",
        secondary=enduse_model_association,
        back_populates="enduse",
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

    enduse = relationship(
        "EndUseOrm",
        secondary=enduse_model_association,
        back_populates="data_source",
    )
    subsector = relationship(
        "SubsectorOrm",
        secondary=model_subsector_association,
        back_populates="data_source",
    )


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""


class ScenarioOrm(BaseOrm):
    __tablename__ = "Scenario"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
