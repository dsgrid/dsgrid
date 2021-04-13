"""Dimension types for dsgrid"""

from enum import Enum

from pydantic import Field

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.data_models import DSGBaseModel


class DimensionType(Enum):
    """Dimension types"""

    END_USE = "end_use"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"
    WEATHER = "weather"
    MODEL_YEAR = "model_year"
    SCENARIO = "scenario"
    MODEL = "model"


class DimensionRecordBaseModel(DSGBaseModel):
    """Base class for all dsgrid dimension models"""

    id: str = Field(
        title="ID",
        description="unique identifier within a dimension",
    )
    name: str = Field(
        title="name",
        description="user-defined name",
    )


class EndUseDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all end use dimensions"""


class GeographyDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all geography dimensions"""


class ModelDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all load model dimensions"""


class ModelYearDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all model year dimensions"""


class ScenarioDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all scenario dimensions"""


class SectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class SubSectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class TimeDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all time dimensions"""


class WeatherDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for weather dimensions"""


_DIMENSION_TO_MODEL = {
    DimensionType.END_USE: EndUseDimensionBaseModel,
    DimensionType.GEOGRAPHY: GeographyDimensionBaseModel,
    DimensionType.SECTOR: SectorDimensionBaseModel,
    DimensionType.SUBSECTOR: SubSectorDimensionBaseModel,
    DimensionType.TIME: TimeDimensionBaseModel,
    DimensionType.WEATHER: WeatherDimensionBaseModel,
    DimensionType.MODEL_YEAR: ModelYearDimensionBaseModel,
    DimensionType.SCENARIO: ScenarioDimensionBaseModel,
    DimensionType.MODEL: ModelDimensionBaseModel,
}


def get_record_base_model(type_enum):
    """Return the dimension model class for a DimensionType."""
    dim_model = _DIMENSION_TO_MODEL.get(type_enum)
    if dim_model is None:
        raise DSGInvalidDimension(f"no mapping for {type_enum}")
    return dim_model