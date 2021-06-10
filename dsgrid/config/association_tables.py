import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import Field, validator

from .config_base import ConfigWithDataFilesBase
from .dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.data_models import serialize_model
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.utils.files import compute_file_hash, dump_data


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
        dsg_internal=True,
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


class AssociationTableConfig(ConfigWithDataFilesBase):
    """Provides an interface to an AssociationTableModel"""

    @staticmethod
    def config_filename():
        return "dimension_mapping.toml"

    @property
    def config_id(self):
        return self.model.mapping_id

    @staticmethod
    def model_class():
        return AssociationTableModel
