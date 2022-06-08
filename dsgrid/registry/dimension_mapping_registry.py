import logging
from pathlib import Path

from pydantic import Field

from .registry_base import RegistryBaseModel, RegistryBase

logger = logging.getLogger(__name__)


class DimensionMappingRegistryModel(RegistryBaseModel):
    """Dimension registration class"""

    dimension_mapping_id: str = Field(
        title="dimension_mapping_id",
        description="Dimension mapping identifier",
    )


class DimensionMappingRegistry(RegistryBase):
    """Controls registration for dimension mappings"""

    DIMENSION_MAPPING_REGISTRY_PATH = Path("configs/dimension_mappings")

    @staticmethod
    def config_filename():
        return "dimension_mapping.toml"

    @property
    def config_id(self):
        return self.model.dimension_mapping_id

    @staticmethod
    def model_class():
        return DimensionMappingRegistryModel

    @staticmethod
    def registry_path():
        return DimensionMappingRegistry.DIMENSION_MAPPING_REGISTRY_PATH
