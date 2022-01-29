from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.files import load_data
from .date_time_dimension_config import DateTimeDimensionConfig
from .annual_time_dimension_config import AnnualTimeDimensionConfig
from .noop_time_dimension_config import NoOpTimeDimensionConfig
from .dimension_config import DimensionConfig
from .representative_period_time_dimension_config import RepresentativePeriodTimeDimensionConfig
from .dimensions import (
    DateTimeDimensionModel,
    DimensionModel,
    DimensionType,
    AnnualTimeDimensionModel,
    RepresentativePeriodTimeDimensionModel,
    NoOpTimeDimensionModel,
)


def get_dimension_config(model, src_dir):
    if isinstance(model, DateTimeDimensionModel):
        return DateTimeDimensionConfig(model)
    if isinstance(model, AnnualTimeDimensionModel):
        return AnnualTimeDimensionConfig(model)
    if isinstance(model, RepresentativePeriodTimeDimensionModel):
        return RepresentativePeriodTimeDimensionConfig(model)
    if isinstance(model, DimensionModel):
        config = DimensionConfig(model)
        config.src_dir = src_dir
        return config
    if isinstance(model, NoOpTimeDimensionModel):
        return NoOpTimeDimensionConfig(model)
    assert False, type(model)


def load_dimension_config(filename):
    """Loads a dimension config file before the exact type is known.

    Parameters
    ----------
    filename : Path

    Returns
    -------
    DimensionBaseConfig

    """
    data = load_data(filename)
    if data["type"] == DimensionType.TIME.value:
        if data["time_type"] == TimeDimensionType.DATETIME.value:
            return DateTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.ANNUAL.value:
            return AnnualTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
            return RepresentativePeriodTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.NOOP.value:
            return NoOpTimeDimensionConfig.load(filename)
        else:
            raise ValueError(f"time_type={data['time_type']} not supported")

    return DimensionConfig.load(filename)
