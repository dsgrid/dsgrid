import logging
from pathlib import Path
from typing import List

from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import make_registry_id, check_config_id_loose
from .mapping_tables import MappingTableModel
from .config_base import ConfigBase


logger = logging.getLogger(__name__)


class DimensionMappingsConfigModel(DSGBaseModel):
    """Represents dimension mapping model configurations"""

    # This may eventually change to a Union if there are more subclasses.
    mappings: List[MappingTableModel] = Field(
        title="mappings",
        description="dimension mappings between and within projects and datasets",
    )


class DimensionMappingsConfig(ConfigBase):
    """Provides an interface to a DimensionMappingsConfigModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._src_dir = None

    @staticmethod
    def config_filename():
        return "dimension_mappings.json5"

    @property
    def config_id(self):
        return self._model.dimension_mapping_id

    @staticmethod
    def model_class():
        return DimensionMappingsConfigModel

    def assign_ids(self):
        """Assign unique IDs to each mapping in the config"""
        for mapping in self.model.mappings:
            from_type = mapping.from_dimension.dimension_id.split("__")[0]
            to_type = mapping.to_dimension.dimension_id.split("__")[0]
            mapping_id = make_registry_id((from_type, to_type))
            check_config_id_loose(mapping_id, "Dimension Mapping")
            mapping.mapping_id = mapping_id

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self._src_dir = src_dir

    @classmethod
    def load(cls, config_filename: Path, *args, **kwargs):
        obj = super().load(config_filename, *args, **kwargs)
        obj.src_dir = config_filename.parent
        return obj

    @classmethod
    def load_from_model(cls, model, src_dir):
        obj = cls(model)
        obj.src_dir = src_dir
        return obj
