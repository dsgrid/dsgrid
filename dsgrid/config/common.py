from typing import Any, Iterable

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import MeasurementType, TimeDimensionType, TimeIntervalType
from dsgrid.dimension.standard import (
    EnergyEfficiency,
    EnergyEndUse,
    EnergyIntensity,
    EnergyIntensityRegression,
    EnergyServiceDemand,
    EnergyServiceDemandRegression,
    FractionalIndex,
    PeggedIndex,
    Population,
    Stock,
    StockShare,
    StockRegression,
)
from dsgrid.exceptions import DSGInvalidParameter


SUPPORTED_METRIC_TYPES = {
    x.__name__
    for x in (
        EnergyEfficiency,
        EnergyEndUse,
        EnergyIntensity,
        EnergyIntensityRegression,
        EnergyServiceDemand,
        EnergyServiceDemandRegression,
        FractionalIndex,
        PeggedIndex,
        Population,
        Stock,
        StockShare,
        StockRegression,
    )
}

DIMENSION_CLASS_MAP = {
    DimensionType.GEOGRAPHY: "Geography",
    DimensionType.MODEL_YEAR: "ModelYear",
    DimensionType.SCENARIO: "Scenario",
    DimensionType.SECTOR: "Sector",
    DimensionType.SUBSECTOR: "Subsector",
    DimensionType.TIME: "Time",
    DimensionType.WEATHER_YEAR: "WeatherYear",
}


def make_base_dimension_template(
    metric_types: Iterable[str],
    exclude_dimension_types: set[DimensionType] | None = None,
    time_type: TimeDimensionType | None = None,
) -> list[dict[str, Any]]:
    exclude: set[DimensionType] = exclude_dimension_types or set()
    exclude.update({DimensionType.METRIC, DimensionType.TIME})

    dimensions: list[dict[str, Any]] = []
    for metric_type in metric_types:
        if metric_type not in SUPPORTED_METRIC_TYPES:
            msg = f"{metric_type=} is not one of the {SUPPORTED_METRIC_TYPES=}"
            raise DSGInvalidParameter(msg)
        dim = {
            "type": DimensionType.METRIC.value,
            "class": metric_type,
            "name": DimensionType.METRIC.value,
            "description": DimensionType.METRIC.value,
            "file": f"dimensions/{metric_type}.csv",
            "module": "dsgrid.dimension.standard",
        }
        dimensions.append(dim)

    dimensions += [
        {
            "type": x.value,
            "class": DIMENSION_CLASS_MAP[x],
            "name": x.value,
            "description": x.value,
            "file": f"dimensions/{x.value}.csv",
            "module": "dsgrid.dimension.standard",
        }
        for x in DimensionType
        if x not in exclude
    ]
    if time_type is not None:
        time_dim = make_base_time_dimension_template(time_type)
        dimensions.append(time_dim)

    return dimensions


def make_base_time_dimension_template(time_type: TimeDimensionType) -> dict[str, Any]:
    time_dim = {
        "type": DimensionType.TIME.value,
        "time_type": time_type.value,
        "time_interval_type": TimeIntervalType.PERIOD_BEGINNING.value,
        "name": time_type.value,
        "description": time_type.value,
        "module": "dsgrid.dimension.standard",
    }
    match time_type:
        case TimeDimensionType.DATETIME:
            time_dim["class"] = "Time"
            time_dim["frequency"] = "P0DT1H"
            time_dim["str_format"] = "%Y-%m-%d %H:%M:%S"
            time_dim["datetime_format"] = {
                "format_type": "aligned",
                "timezone": "EasternStandard",
            }
            time_dim["measurement_type"] = MeasurementType.TOTAL.value
            time_dim["ranges"] = [
                {
                    "start": "2018-01-01 00:00:00",
                    "end": "2018-12-31 23:00:00",
                },
            ]
        case TimeDimensionType.ANNUAL:
            time_dim["class"] = "AnnualTime"
            time_dim["include_leap_day"] = True
            time_dim["ranges"] = [
                {
                    "start": "2010",
                    "end": "2024",
                },
            ]
            time_dim["str_format"] = "%Y"
        case TimeDimensionType.INDEX:
            time_dim["class"] = "IndexTime"
            time_dim["frequency"] = "P0DT1H"
            time_dim["starting_timestamps"] = ["2018-01-01 00:00:00"]
            time_dim["str_format"] = "%Y-%m-%d %H:%M:%S"
            time_dim["ranges"] = [
                {
                    "start": 0,
                    "end": 8759,
                },
            ]
    return time_dim
