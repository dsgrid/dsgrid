import logging
from pathlib import Path


from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.aws import sync
from dsgrid.common import S3_REGISTRY

logger = logging.getLogger(__name__)


class AssociationTableRegistry(RegistryBase):
    """Controls association table registration for mapping dimensions"""

    ASSOCIATION_TABLE_REGISTRY_PATH = Path("association_tables")
    ASSOCIATION_TABLE_S3_REGISTRY_PATH = f"{S3_REGISTRY}/association_tables"

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return AssociationTableRegistry.ASSOCIATION_TABLE_REGISTRY_PATH

    @staticmethod
    def registry_s3_path():
        return AssociationTableRegistry.ASSOCIATION_TABLE_S3_REGISTRY_PATH

    @staticmethod
    def sync_push():
        sync(AssociationTableRegistry.registry_path(), AssociationTableRegistry.registry_s3_path())

    @staticmethod
    def sync_pull():
        sync(AssociationTableRegistry.registry_s3_path(), AssociationTableRegistry.registry_path())
