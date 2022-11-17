import logging
from pathlib import Path

from pydantic import Field

from .registry_base import RegistryBaseModel, RegistryBase

logger = logging.getLogger(__name__)


class DimensionRegistryModel(RegistryBaseModel):
    """Dimension registration class"""

    dimension_id: str = Field(
        title="dimension_id",
        description="Dimension identifier",
    )


class DimensionRegistry(RegistryBase):
    """Controls dimension (record) registration from datasets and projects"""

    DIMENSION_REGISTRY_PATH = Path("configs/dimensions")

    @staticmethod
    def config_filename():
        return "dimension.json5"

    @property
    def config_id(self):
        return self.model.dimension_id

    @staticmethod
    def model_class():
        return DimensionRegistryModel

    @staticmethod
    def registry_path():
        return DimensionRegistry.DIMENSION_REGISTRY_PATH
