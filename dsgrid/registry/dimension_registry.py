import logging
from pathlib import Path


from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.aws import sync
from dsgrid.common import S3_REGISTRY

logger = logging.getLogger(__name__)


class DimensionRegistry(RegistryBase):
    """Controls dimension (record) registration from datasets and projects"""

    # TODO: add validator to prevent duplication of registered dim records

    DIMENSION_REGISTRY_PATH = Path("dimensions")
    DIMENSION_REGISTRY_S3_PATH = f"{S3_REGISTRY}/dimensions"

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return DimensionRegistry.DIMENSION_REGISTRY_PATH

    @staticmethod
    def registry_s3_path():
        return DimensionRegistry.DIMENSION_REGISTRY_S3_PATH

    @staticmethod
    def sync_push(local_registry_path):
        sync(
            local_registry_path / DimensionRegistry.registry_path(),
            DimensionRegistry.registry_s3_path(),
        )

    @staticmethod
    def sync_pull(local_registry_path):
        sync(
            DimensionRegistry.registry_s3_path(),
            local_registry_path / DimensionRegistry.registry_path(),
        )
