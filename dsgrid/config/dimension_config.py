"""
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
"""
from typing import List, Optional, Union, Dict
import logging

from pydantic import Field
from pydantic import validator, root_validator

from .config_base import ConfigBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    handle_dimension_union,
)
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.utilities import check_uniqueness
from dsgrid.dimension.base_models import DimensionType

logger = logging.getLogger(__name__)


class DimensionConfigModel(DSGBaseModel):
    """Represents dimension model configurations"""

    dimensions: List[Union[DimensionModel, TimeDimensionModel]] = Field(
        title="dimensions",
        description="dimensions for submission to the dimension registry",
    )

    @validator("dimensions")
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files are unique across all dimensions"""
        check_uniqueness(
            (x.filename for x in values if isinstance(x, DimensionModel)),
            "dimension record filename",
        )
        return values

    @validator("dimensions")
    def check_names(cls, values: dict) -> dict:
        """Validate dimension names are unique across all dimensions and descriptive"""
        check_uniqueness(
            (x.name.lower().replace(" ", "-") for x in values),
            "dimension record name",
        )
        # TODO: improve validation for alloweable dimension record names.
        prohibited_names = [x.value.replace("_", "") for x in DimensionType] + [
            "county",
            "counties",
            "year",
            "hourly",
            "comstock",
            "resstock",
            "tempo",
            "model",
            "source",
            "dimesnion",
        ]
        prohibited_names = prohibited_names + [x + "s" for x in prohibited_names]
        for x in values:
            if x.name.lower().replace(" ", "-") in prohibited_names:
                raise ValueError(
                    f" Dimension name '{x.name}' is not descriptive enough for a dimension record name. Please be more descriptive in your naming. Hint: try adding a vintage, or other distinguishable text that will be this dimension memorable, identifiable, and reusable for other datasets and projects. e.g., 'time-2012-est-houlry-periodending-nodst-noleapdayadjustment-mean' is a good descriptive name."
                )
        return values

    @validator("dimensions", pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, values):
        return handle_dimension_union(values)


class DimensionConfig(ConfigBase):
    """Provides an interface to a DimensionConfigModel."""

    @staticmethod
    def model_class():
        return DimensionConfigModel
