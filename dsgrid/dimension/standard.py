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
    ModelDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubSectorDimensionBaseModel,
    WeatherDimensionBaseModel,
)

BaseOrm = declarative_base()


enduse_model_association = Table(
    "enduse_model",
    BaseOrm.metadata,
    Column("enduse", String(255), ForeignKey("EndUse.id")),
    Column("model", String(255), ForeignKey("Model.id")),
)


model_subsector_association = Table(
    "model_subsector",
    BaseOrm.metadata,
    Column("model", String(255), ForeignKey("Model.id")),
    Column("subsector", String(255), ForeignKey("SubSector.id")),
)


subsector_sector_association = Table(
    "subsector_sector",
    BaseOrm.metadata,
    Column("subsector", String(255), ForeignKey("SubSector.id")),
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
        "SubSectorOrm",
        secondary=subsector_sector_association,
        back_populates="sector",
    )


# ---------------------------
# SUBSECTOR DIMENSIONS
# ---------------------------
class SubSector(SubSectorDimensionBaseModel):
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


class SubSectorOrm(BaseOrm):
    __tablename__ = "SubSector"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(255), nullable=False)
    abbr = Column(String(255), nullable=False)

    model = relationship(
        "ModelOrm",
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
        "ModelOrm",
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
class Weather(WeatherDimensionBaseModel):
    """ attributes"""


class ModelYear(ModelYearDimensionBaseModel):
    """ attributes"""


class Model(ModelDimensionBaseModel):
    """Model attributes"""


class ModelOrm(BaseOrm):
    __tablename__ = "Model"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)

    enduse = relationship(
        "EndUseOrm",
        secondary=enduse_model_association,
        back_populates="model",
    )
    subsector = relationship(
        "SubSectorOrm",
        secondary=model_subsector_association,
        back_populates="model",
    )


class Scenario(ScenarioDimensionBaseModel):
    """Scenario attributes"""


class ScenarioOrm(BaseOrm):
    __tablename__ = "Scenario"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
