"""Standard dimension ORM classes for dsgrid"""

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


BaseOrm = declarative_base()

enduse_model_association = Table(
    "enduse_model",
    BaseOrm.metadata,
    Column("enduse", String(255), ForeignKey("EndUse.id")),
    Column("data_source", String(255), ForeignKey("DataSource.id")),
)
# FIXME: @dtom should this be data_source or datasource? Repeat fix.

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


class CensusDivisionOrm(BaseOrm):
    __tablename__ = "CensusDivision"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)


class CensusRegionOrm(BaseOrm):
    __tablename__ = "CensusRegion"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)


class StateOrm(BaseOrm):
    __tablename__ = "State"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    is_conus = Column(Boolean, nullable=False)
    census_division = Column(String(255), nullable=False)
    census_region = Column(String(255), nullable=False)

    counties = relationship("CountyOrm", back_populates="state_rel")


class CountyOrm(BaseOrm):
    __tablename__ = "County"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    state = Column(String(255), ForeignKey("State.id"), nullable=False)
    timezone = Column(String(255), nullable=False)

    state_rel = relationship("StateOrm", back_populates="counties")


class SectorOrm(BaseOrm):
    __tablename__ = "Sector"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)

    subsector = relationship(
        "SubsectorOrm",
        secondary=subsector_sector_association,
        back_populates="sector",
    )


class SubsectorOrm(BaseOrm):
    __tablename__ = "Subsector"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(255), nullable=False)
    abbr = Column(String(255), nullable=False)

    data_source = relationship(
        "DataSourceOrm",
        secondary=model_subsector_association,
        back_populates="subsector",
    )
    sector = relationship(
        "SectorOrm",
        secondary=subsector_sector_association,
        back_populates="subsector",
    )


class EndUseOrm(BaseOrm):
    __tablename__ = "EndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    fuel_id = Column(String(255), nullable=False)
    units = Column(String(255), nullable=False)

    data_source = relationship(
        "DataSourceOrm",
        secondary=enduse_model_association,
        back_populates="enduse",
    )


# Which DataSourceORM do we keep?
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


class ScenarioOrm(BaseOrm):
    __tablename__ = "Scenario"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)


class EnergyEndUseOrm(BaseOrm):
    __tablename__ = "EnergyEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)
    fuel_id = Column(String(255), nullable=False)


class EnergyServiceEndUseOrm(BaseOrm):
    __tablename__ = "EnergyServiceEndUse"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class PopulationOrm(BaseOrm):
    __tablename__ = "Population"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class StockOrm(BaseOrm):
    __tablename__ = "Stock"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)


class EnergyEfficiencyOrm(BaseOrm):
    __tablename__ = "EnergyEfficiency"

    id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    unit = Column(String(255), nullable=False)
