"""Dimension types for dsgrid"""

from pydantic import Field

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.data_models import DSGBaseModel, DSGEnum
from dsgrid.utils.utilities import check_uniqueness


class DimensionType(DSGEnum):
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

    @classmethod
    def from_column(cls, column):
        try:
            return cls(column)
        except ValueError:
            raise DSGInvalidDimension(
                f"column={column} is not expected or of a known dimension type."
            )


class DimensionRecordBaseModel(DSGBaseModel):
    """Base class for all dsgrid dimension models"""

    # TODO: add support/links for docs
    id: str = Field(
        title="ID",
        description="Unique identifier within a dimension",
    )
    name: str = Field(
        title="name",
        description="User-defined name",
    )


class MetricDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all metric dimensions (e.g. EnergyEndUse)"""


class GeographyDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all geography dimensions"""


class DataSourceDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all data source dimensions"""


class ModelYearDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all model year dimensions"""


class ScenarioDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all scenario dimensions"""


class SectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all sector dimensions"""


class SubsectorDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for all subsector dimensions"""


class WeatherYearDimensionBaseModel(DimensionRecordBaseModel):
    """Base class for weather year dimensions"""


_DIMENSION_TO_MODEL = {
    DimensionType.METRIC: MetricDimensionBaseModel,
    DimensionType.GEOGRAPHY: GeographyDimensionBaseModel,
    DimensionType.SECTOR: SectorDimensionBaseModel,
    DimensionType.SUBSECTOR: SubsectorDimensionBaseModel,
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


def check_required_dimensions(dimensions, tag):
    """Check that a project or dataset config contains all required dimensions.

    Parameters
    ----------
    dimensions : list
        list of DimensionReferenceModel
    tag : str
        User-defined string to include in exception messages

    Raises
    ------
    ValueError
        Raised if a required dimension is not provided.

    """
    dimension_types = {x.dimension_type for x in dimensions}
    required_dim_types = set(DimensionType)
    missing = required_dim_types.difference(dimension_types)
    if missing:
        raise ValueError(f"Required dimension(s) {missing} are not in {tag}.")
    check_uniqueness((x.dimension_type for x in dimensions), tag)
