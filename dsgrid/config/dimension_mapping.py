"""Defines models to map dataset dimensions to a project."""

import logging
import os
from typing import List

from pydantic import Field, validator

from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.data_models import DSGBaseModel


logger = logging.getLogger(__name__)


class DimensionMapByAssocationTableModel(DSGBaseModel):
    """Provides mapping within a dimension type."""

    from_dimension: DimensionReferenceModel
    to_dimension: DimensionReferenceModel
    filename: str = Field(
        title="file",
        alias="file",
        description="filename containing association table",
    )

    @validator("filename")
    def check_file(cls, filename):
        """Validate that association file exists."""
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename


class DimensionMappingsModel(DSGBaseModel):
    # This could change to a List[Union] if we have different mapping types.
    mappings: List[DimensionMapByAssocationTableModel] = Field(
        title="mappings",
        description="list of dimension mappings",
    )
