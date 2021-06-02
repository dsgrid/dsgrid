import logging
import os
from typing import Dict, List, Optional, Union

from pydantic import Field, validator

from .dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.utils.files import compute_file_hash


logger = logging.getLogger(__name__)


class AssociationTableModel(DimensionMappingBaseModel):
    """Attributes for an association table"""

    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing association table records",
    )
    file_hash: Optional[str] = Field(
        title="file_hash",
        description="hash of the contents of the file, computed by dsgrid",
    )

    @validator("filename")
    def check_filename(cls, filename):
        """Validate record file"""
        if not os.path.exists(filename):
            raise ValueError(f"{filename} does not exist")
        return filename

    @validator("file_hash")
    def compute_file_hash(cls, file_hash, values):
        """Compute file hash"""
        return file_hash or compute_file_hash(values["filename"])
