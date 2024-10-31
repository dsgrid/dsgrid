import logging
from pathlib import Path

from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from .mapping_tables import MappingTableModel
from .config_base import ConfigBase


logger = logging.getLogger(__name__)


class DimensionMappingsConfigModel(DSGBaseModel):
    """Represents dimension mapping model configurations"""

    # This may eventually change to a Union if there are more subclasses.
    mappings: list[MappingTableModel] = Field(
        title="mappings",
        description="dimension mappings between and within projects and datasets",
    )


class DimensionMappingsConfig(ConfigBase):
    """Provides an interface to a DimensionMappingsConfigModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def config_filename():
        return "dimension_mappings.json5"

    @property
    def config_id(self):
        return self._model.dimension_mapping_id

    @staticmethod
    def model_class():
        return DimensionMappingsConfigModel

    @classmethod
    def load(cls, config_filename: Path, *args, **kwargs):
        return super().load(config_filename, *args, **kwargs)

    @classmethod
    def load_from_model(cls, model):
        return cls(model)
