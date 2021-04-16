"""
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
"""
from typing import List, Optional, Union, Dict
import logging

from pydantic import Field
from pydantic import validator

from .config_base import ConfigBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    handle_dimension_union,
)
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.utilities import check_uniqueness


logger = logging.getLogger(__name__)

"""
VALIDATE:
1. 8 unique types of dimensions (all except time) but each type can have more than one entry.
2. dimension `ids' and records are all unique.
"""


class DimensionConfigModel(DSGBaseModel):
    """Represents dimension model configurations"""

    dimensions: List[Union[DimensionModel, TimeDimensionModel]] = Field(
        title="dimensions",
        description="dimensions shared between project and dataset",
    )
    registration: Optional[Dict] = Field(
        title="registration",
        description="registration information",
    )

    @validator("dimensions")
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files across all dimensions"""
        check_uniqueness(
            (x.filename for x in values if isinstance(x, DimensionModel)),
            "dimension record filename",
        )
        return values

    @validator("dimensions", pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, value):
        return handle_dimension_union(value)


class DimensionConfig(ConfigBase):
    """Provides an interface to a DimensionConfigModel."""

    @staticmethod
    def model_class():
        return DimensionConfigModel
