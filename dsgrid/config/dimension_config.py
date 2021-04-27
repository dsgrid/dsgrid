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
from dsgrid.registry.common import make_registry_id
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
            [dim.name for dim in values],
            "dimension record name",
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

    def assign_ids(self):
        """Assign unique IDs to each mapping in the config"""
        logger.info("Dimension record ID assignment:")
        for dim in self.model.dimensions:
            # assign id, made from dimension.name and a UUID
            dim.dimension_id = make_registry_id([dim.name.lower().replace(" ", "_")])
