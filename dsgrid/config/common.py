from typing import Any

from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import MeasurementType, TimeDimensionType, TimeIntervalType


def make_base_dimension_template(
    exclude_dimension_types: set[DimensionType] | None = None,
    time_type: TimeDimensionType = TimeDimensionType.DATETIME,
) -> list[dict[str, Any]]:
    exclude: set[DimensionType] = exclude_dimension_types or set()
    exclude.add(DimensionType.TIME)
    dimensions = [
        {
            "type": x.value,
            "class": x.value.title(),
            "name": "",
            "description": "",
            "file": f"dimensions/{x.value}.csv",
            "module": "dsgrid.dimension.standard",
        }
        for x in DimensionType
        if x not in exclude
    ]
    time_dim = {
        "type": DimensionType.TIME.value,
        "time_type": time_type.value,
        "time_interval_type": TimeIntervalType.PERIOD_BEGINNING.value,
        "name": "",
        "description": "",
        "module": "dsgrid.dimension.standard",
    }
    match time_type:
        case TimeDimensionType.DATETIME:
            time_dim["class"] = "Time"
            time_dim["frequency"] = "P0DT1H"
            time_dim["leap_day_adjustment"] = "none"
            time_dim["str_format"] = "%Y-%m-%d %H:%M:%S"
            time_dim["timezone"] = "EasternStandard"
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

    dimensions.append(time_dim)
    return dimensions
