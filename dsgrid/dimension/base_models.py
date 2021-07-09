"""Dimension types for dsgrid"""

from enum import Enum

from pydantic import Field

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.data_models import DSGBaseModel


class DimensionType(Enum):
    """Dimension types"""

    METRIC = "metric"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"
    WEATHER_YEAR = "weather_year"
    MODEL_YEAR = "model_year"
    SCENARIO = "scenario"
    DATA_SOURCE = "data_source"


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


class MetricDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all metric dimensions (e.g. EndUse)"""


class GeographyDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all geography dimensions"""


class DataSourceDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all data source dimensions"""


class ModelYearDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all model year dimensions"""


class ScenarioDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all scenario dimensions"""


class SectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class SubsectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class TimeDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all time dimensions"""


class WeatherYearDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for weather year dimensions"""


_DIMENSION_TO_MODEL = {
    DimensionType.METRIC: MetricDimensionBaseModel,
    DimensionType.GEOGRAPHY: GeographyDimensionBaseModel,
    DimensionType.SECTOR: SectorDimensionBaseModel,
    DimensionType.SUBSECTOR: SubsectorDimensionBaseModel,
    DimensionType.TIME: TimeDimensionBaseModel,
    DimensionType.WEATHER_YEAR: WeatherYearDimensionBaseModel,
    DimensionType.MODEL_YEAR: ModelYearDimensionBaseModel,
    DimensionType.SCENARIO: ScenarioDimensionBaseModel,
    DimensionType.DATA_SOURCE: DataSourceDimensionBaseModel,
}


def get_record_base_model(type_enum):
    """Return the dimension model class for a DimensionType."""
    dim_model = _DIMENSION_TO_MODEL.get(type_enum)
    if dim_model is None:
        raise DSGInvalidDimension(f"no mapping for {type_enum}")
    return dim_model
