"""Dimension types for dsgrid"""

import enum
import os

from pydantic import BaseModel, Field
from semver import VersionInfo

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.utils.files import load_data


class DSGBaseModel(BaseModel):
    """Base model for all dsgrid models"""

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"
        use_enum_values = False
        arbitrary_types_allowed = True

    @classmethod
    def load(cls, filename):
        """Load a configuration from a file containing file paths
        relative to filename.

        Parameters
        ----------
        filename : str

        """
        base_dir = os.path.dirname(filename)
        orig = os.getcwd()
        os.chdir(base_dir)
        try:
            cfg = cls(**load_data(os.path.basename(filename)))
            return cfg

        finally:
            os.chdir(orig)


class DSGBaseDimensionModel(DSGBaseModel):
    """Base class for all dsgrid dimension models"""

    id: str = Field(
        title="ID", description="unique identifier within a dimension",
    )
    name: str = Field(
        title="name", description="user-defined name",
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


def serialize_model(model):
    """Serialize a model to a dict, converting values as needed."""
    return _serialize_model_data(model.dict())


def _serialize_model_data(data):
    for key, val in data.items():
        data[key] = _serialize_model_item(val)
    return data


def _serialize_model_item(val):
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, VersionInfo):
        return str(val)
    if isinstance(val, dict):
        return _serialize_model_data(val)
    if isinstance(val, list):
        return [_serialize_model_item(x) for x in val]
    return val
