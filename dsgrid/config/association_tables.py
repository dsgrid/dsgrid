import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import Field, validator

from .config_base import ConfigBase
from .dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.data_models import serialize_model
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


class AssociationTableConfig(ConfigBase):
    """Provides an interface to an AssociationTableModel"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._src_dir = None

    @classmethod
    def load(cls, config_file):
        config = cls._load(config_file)
        config.src_dir = os.path.dirname(config_file)
        return config

    @staticmethod
    def config_filename():
        return "dimension_mapping.toml"

    @property
    def config_id(self):
        return self.model.mapping_id

    @staticmethod
    def model_class():
        return AssociationTableModel

    def serialize(self, path):
        model_data = serialize_model(self.model)
        orig_file = getattr(self.model, "filename")

        # Leading directories from the original are not relevant in the registry.
        dst_record_file = path / os.path.basename(orig_file)
        shutil.copyfile(self._src_dir / self.model.filename, dst_record_file)
        # We have to make this change in the serialized dict instead of
        # model because Pydantic will fail the assignment due to not being
        # able to find the path.
        model_data["file"] = os.path.basename(self.model.filename)

        filename = path / self.config_filename()
        dump_data(model_data, filename)
        return filename

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self._src_dir = Path(src_dir)
