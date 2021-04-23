import logging
from pathlib import Path


from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.factory import make_filesystem_interface
from dsgrid.common import REMOTE_REGISTRY

logger = logging.getLogger(__name__)


class DimensionMappingRegistry(RegistryBase):
    """Controls registration for dimension mappings"""

    DIMENSION_MAPPING_REGISTRY_PATH = Path("configs/dimension_mappings")
    DIMENSION_MAPPING_REMOTE_REGISTRY_PATH = (
        f"{REMOTE_REGISTRY}/{DIMENSION_MAPPING_REGISTRY_PATH}/"
    )

    @staticmethod
    def cloud_interface(self):
        return make_filesystem_interface(
            DimensionMappingRegistry.DIMENSION_MAPPING_REMOTE_REGISTRY_PATH
        )

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return DimensionMappingRegistry.DIMENSION_MAPPING_REGISTRY_PATH

    @staticmethod
    def registry_remote_path():
        return DimensionMappingRegistry.DIMENSION_MAPPING_REMOTE_REGISTRY_PATH

    @staticmethod
    def sync_push(local_registry_path):
        DimensionMappingRegistry.cloud_interface.sync_push(
            local_registry=local_registry_path / DimensionMappingRegistry.registry_path(),
            remote_registry=DimensionMappingRegistry.registry_remote_path(),
        )

    @staticmethod
    def sync_pull(local_registry_path):
        DimensionMappingRegistry.cloud_interface.sync_pull(
            DimensionMappingRegistry.registry_remote_path(),
            local_registry=local_registry_path / DimensionMappingRegistry.registry_path(),
        )
