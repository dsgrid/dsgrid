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


class EndUseDimensionModel(DimensionRecordBaseModel):
    """Base class for all end use dimensions"""


class GeographicDimensionModel(DimensionRecordBaseModel):
    """Base class for all geography dimensions"""


class ModelDimensionModel(DimensionRecordBaseModel):
    """Base class for all load model dimensions"""


class ModelYearDimensionModel(DimensionRecordBaseModel):
    """Base class for all model year dimensions"""


class ScenarioDimensionModel(DimensionRecordBaseModel):
    """Base class for all scenario dimensions"""


class SectorDimensionModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class SubSectorDimensionModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class BaseTimeDimensionModel(DimensionRecordBaseModel):
    """Base class for all time dimensions"""


class WeatherDimensionModel(DimensionRecordBaseModel):
    """Base class for weather dimensions"""


_DIMENSION_TO_MODEL = {
    DimensionType.END_USE: EndUseDimensionModel,
    DimensionType.GEOGRAPHY: GeographicDimensionModel,
    DimensionType.SECTOR: SectorDimensionModel,
    DimensionType.SUBSECTOR: SubSectorDimensionModel,
    DimensionType.TIME: BaseTimeDimensionModel,
    DimensionType.WEATHER: WeatherDimensionModel,
    DimensionType.MODEL_YEAR: ModelYearDimensionModel,
    DimensionType.SCENARIO: ScenarioDimensionModel,
    DimensionType.MODEL: ModelDimensionModel,
}


def get_record_base_model(type_enum):
    """Return the dimension model class for a DimensionType."""
    dim_model = _DIMENSION_TO_MODEL.get(type_enum)
    if dim_model is None:
        raise DSGInvalidDimension(f"no mapping for {type_enum}")
    return dim_model
