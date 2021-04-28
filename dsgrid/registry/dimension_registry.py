import logging
from pathlib import Path


from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.factory import make_filesystem_interface
from dsgrid.common import REMOTE_REGISTRY

logger = logging.getLogger(__name__)


class DimensionRegistry(RegistryBase):
    """Controls dimension (record) registration from datasets and projects"""

    # TODO: add validator to prevent duplication of registered dim records

    DIMENSION_REGISTRY_PATH = Path("configs/dimensions")

    @staticmethod
    def config_filename():
        return "dimension.toml"

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return DimensionRegistry.DIMENSION_REGISTRY_PATH
