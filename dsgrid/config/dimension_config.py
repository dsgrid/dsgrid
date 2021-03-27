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
from dsgrid.dimension.base import DimensionType, DSGBaseModel
from dsgrid.utils.aws import sync


from dsgrid.config._config import TimeDimension, Dimension, ConfigRegistrationModel


logger = logging.getLogger(__name__)

"""
VALIDATE:
1. 8 unique types of dimensions (all except time) but each type can have more than one entry.
2. The dimensions that are called in `dataset` and `project` are available.
3. dimension `id` are not duplicated.
"""

class DimensionConfigModel(DSGBaseModel):
    """Represents model dataset configurations"""
    # dimension_id: str = Field(
    #     title="dimension_id",
    #     description="dimension record identifier",
    # )
    dimensions: List[Union[Dimension, TimeDimension]] = Field(
        title="dimensions",
        description="dimensions shared between project and dataset",
    )
    registration: Optional[Dict] = Field(
        title="registration",
        description="registration information",
    )
    # TODO: can we reuse this validator? Its taken from the
    #   project_config Diemnsions model
    def handle_dimension_union(cls, value):
        """
        Validate dimension type work around for pydantic Union bug
        related to: https://github.com/samuelcolvin/pydantic/issues/619
        """
        # NOTE: Errors inside Dimension or TimeDimension will be duplicated
        # by Pydantic
        if value["type"] == DimensionType.TIME.value:
            val = TimeDimension(**value)
        else:
            val = Dimension(**value)
        return val

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
