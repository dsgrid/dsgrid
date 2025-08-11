from dsgrid.dimension.time import TimeDimensionType
from dsgrid.utils.files import load_data
from .date_time_dimension_config import DateTimeDimensionConfig
from .annual_time_dimension_config import AnnualTimeDimensionConfig
from .noop_time_dimension_config import NoOpTimeDimensionConfig
from .index_time_dimension_config import IndexTimeDimensionConfig
from .dimension_config import DimensionConfig
from .representative_period_time_dimension_config import RepresentativePeriodTimeDimensionConfig
from .dimensions import (
    DateTimeDimensionModel,
    DimensionModel,
    DimensionType,
    AnnualTimeDimensionModel,
    RepresentativePeriodTimeDimensionModel,
    NoOpTimeDimensionModel,
    IndexTimeDimensionModel,
)


def get_dimension_config(model):
    if isinstance(model, DateTimeDimensionModel):
        return DateTimeDimensionConfig(model)
    if isinstance(model, AnnualTimeDimensionModel):
        return AnnualTimeDimensionConfig(model)
    if isinstance(model, RepresentativePeriodTimeDimensionModel):
        return RepresentativePeriodTimeDimensionConfig(model)
    if isinstance(model, DimensionModel):
        config = DimensionConfig(model)
        return config
    if isinstance(model, NoOpTimeDimensionModel):
        return NoOpTimeDimensionConfig(model)
    if isinstance(model, IndexTimeDimensionModel):
        return IndexTimeDimensionConfig(model)
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
        elif data["time_type"] == TimeDimensionType.INDEX.value:
            return IndexTimeDimensionConfig.load(filename)
        else:
            msg = f"time_type={data['time_type']} not supported"
            raise ValueError(msg)

    return DimensionConfig.load(filename)
