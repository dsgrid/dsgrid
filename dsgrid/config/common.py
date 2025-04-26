from typing import Any

from dsgrid.dimension.base_models import DimensionType


def make_base_dimension_template(
    exclude_dimension_types: set[DimensionType] | None = None,
) -> list[dict[str, Any]]:
    exclude: set[DimensionType] = exclude_dimension_types or set()
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
    dimensions.append(
        {
            "type": DimensionType.TIME.value,
            "time_type": "",
            "class": DimensionType.TIME.value.title(),
            "name": "",
            "description": "",
        }
    )
    return dimensions
