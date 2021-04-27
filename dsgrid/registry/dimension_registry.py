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
    DIMENSION_REGISTRY_REMOTE_PATH = f"{REMOTE_REGISTRY}/{DIMENSION_REGISTRY_PATH}/"

    @staticmethod
    def cloud_interface(self):
        return make_filesystem_interface(DimensionRegistry.DIMENSION_REGISTRY_REMOTE_PATH)

    @staticmethod
    def config_filename():
        return "dimension.toml"

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return DimensionRegistry.DIMENSION_REGISTRY_PATH

    @staticmethod
    def registry_remote_path():
        return DimensionRegistry.DIMENSION_REGISTRY_REMOTE_PATH

    @staticmethod
    def sync_push(local_registry_path):
        DimensionRegistry.cloud_interface.sync_push(
            local_registry=local_registry_path,
            remote_registry=DimensionRegistry.registry_remote_path(),
        )

    @staticmethod
    def sync_pull(local_registry_path):
        DimensionRegistry.cloud_interface.sync_pull(
            remote_registry=DimensionRegistry.registry_remote_path(),
            local_registry=local_registry_path,
        )
