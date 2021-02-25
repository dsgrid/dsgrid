"""Dimension types for dsgrid"""

import enum

from pydantic import BaseModel, Field

from dsgrid.exceptions import DSGInvalidDimension


class DSGBaseModel(BaseModel):
    """Base model for all dsgrid models"""

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"  # TODO: consider changing this after we get this working
        use_enum_values = True


class DSGBaseDimensionModel(DSGBaseModel):
    """Base class for all dsgrid dimension models"""
    id: str = Field(
        title="ID",
        description="unique identifier within a dimension",
    )
    name: str = Field(
        title="name",
        description="user-defined name",
    )


class EndUseDimensionModel(DSGBaseDimensionModel):
    """Base class for all end use dimensions"""


class GeographicDimensionModel(DSGBaseDimensionModel):
    """Base class for all geography dimensions"""


class ModelDimensionModel(DSGBaseDimensionModel):
    """Base class for all load model dimensions"""


class ModelYearDimensionModel(DSGBaseDimensionModel):
    """Base class for all model year dimensions"""


class ScenarioDimensionModel(DSGBaseDimensionModel):
    """Base class for all scenario dimensions"""


class SectorDimensionModel(DSGBaseDimensionModel):
    """Base class for all subsector dimensions"""


class SubSectorDimensionModel(DSGBaseDimensionModel):
    """Base class for all subsector dimensions"""


class TimeDimensionModel(DSGBaseDimensionModel):
    """Base class for all time dimensions"""


class WeatherDimensionModel(DSGBaseDimensionModel):
    """Base class for weather dimensions"""


class DimensionType(enum.Enum):
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


_DIMENSION_TO_MODEL = {
    DimensionType.END_USE: EndUseDimensionModel,
    DimensionType.GEOGRAPHY: GeographicDimensionModel,
    DimensionType.SECTOR: SectorDimensionModel,
    DimensionType.SUBSECTOR: SubSectorDimensionModel,
    DimensionType.TIME: TimeDimensionModel,
    DimensionType.WEATHER: WeatherDimensionModel,
    DimensionType.MODEL_YEAR: ModelYearDimensionModel,
    DimensionType.SCENARIO: ScenarioDimensionModel,
    DimensionType.MODEL: ModelDimensionModel,
}


def get_dimension_model(type_enum):
    """Return the dimension model class for a DimensionType."""
    dim_model = _DIMENSION_TO_MODEL.get(type_enum)
    if dim_model is None:
        raise DSGInvalidDimension(f"no mapping for {type_enum}")
    return dim_model


class MappingType(enum.Enum):
    ONE_TO_MANY = "one_to_many_mappings"
    MANY_TO_ONE = "many_to_one_mappings"
    MANY_TO_MANY = "many_to_many_mappings"


class DayType(enum.Enum):
    """Day types"""
    WEEKEND = "weekend"
    WEEKDAY = "weekday"


class Season(enum.Enum):
    """Seasons"""
    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    FALL = "autumn"
