"""Dimension types for dsgrid"""

from pydantic import Field

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.data_models import DSGBaseModel, DSGEnum
from dsgrid.utils.utilities import check_uniqueness
from dsgrid.dimension.time import TimeZone


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

    def __lt__(self, other):
        return self.value < other.value

    @classmethod
    def from_column(cls, column):
        try:
            return cls(column)
        except ValueError:
            raise DSGInvalidDimension(
                f"column={column} is not expected or of a known dimension type."
            )


def get_project_dimension_types() -> set[DimensionType]:
    """Return the types that must be present in a dataset for operation with a project."""
    return {x for x in DimensionType if x != DimensionType.DATA_SOURCE}


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

    time_zone: TimeZone = Field(
        title="Local Prevailing Time Zone",
        description="""
        These time zone information are used in reference to project timezone
        to convert between project time and local times as necessary.
        All Prevailing timezones account for daylight savings time.
        If a location does not observe daylight savings, use Standard timezones.
        """,
        default=TimeZone.NONE,
    )


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


def check_timezone_in_geography(dimension, err_msg=None):
    """Check that a geography dimension contains valid time zones in records.

    Parameters
    ----------
    dimension : DimensionModel
    err_msg : str | None
        Optional error message

    Raises
    ------
    DSGInvalidDimension
        Raised if a required dimension is not provided.

    """
    if dimension.dimension_type != DimensionType.GEOGRAPHY:
        raise DSGInvalidDimension(
            f"Dimension has type {dimension.dimension_type}, "
            "Can only check timezone for Geography."
        )

    if not hasattr(dimension.records[0], "time_zone"):
        if err_msg is None:
            err_msg = "These geography dimension records must include a time_zone column."
        raise ValueError(err_msg)

    tz = set(TimeZone)
    record_tz = {rec.time_zone for rec in dimension.records}
    diff = record_tz.difference(tz)
    if diff:
        raise DSGInvalidDimension(
            f"Geography dimension {dimension.dimension_id} has invalid timezone(s) in records: "
            f"{dimension.filename}: {diff}. Use dsgrid TimeZone enum values only ({tz})."
        )
