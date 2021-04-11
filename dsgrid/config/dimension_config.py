"""
Running List of TODO's:


Questions:
- Do diemsnion names need to be distinct from project's? What if the dataset
is using the same dimension as the project?
"""
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union, Dict
import os
import logging

import toml

from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.common import LOCAL_REGISTRY_DATA
from dsgrid.exceptions import DSGBaseException
from dsgrid.dimension.models import (
    TimeDimensionModel,
    DimensionModel,
    DimensionType,
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
    """Represents model dataset configurations"""

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


class DimensionConfig:
    """Provides an interface to a DatasetConfigModel."""

    def __init__(self, model):
        self._model = model

    @classmethod
    def load(cls, config_file):
        model = DimensionConfigModel.load(config_file)
        return cls(model)

    @property
    def model(self):
        return self._model

    # TODO:
    #   - check binning/partitioning / file size requirements?
    #   - check unique names
    #   - check unique files
    #   - add similar validators as project_config Dimensions
    # NOTE: project_config.Dimensions has a lot of the
    #       validators we want here however, its unclear to me how we can
    #       apply them in both classes because they are root valitors and
    #       also because Dimensions includes project_dimension and
    #       supplemental_dimensions which are not required at the dataset
    #       level.
