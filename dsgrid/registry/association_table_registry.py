import logging
from pathlib import Path


from .registry_base import RegistryBaseModel, RegistryBase


logger = logging.getLogger(__name__)


class AssociationTableRegistry(RegistryBase):
    """Controls association table registration for mapping dimensions"""

    ASSOCIATION_TABLE_REGISTRY_PATH = Path("association_tables")

    @staticmethod
    def model_class():
        return RegistryBaseModel

    @staticmethod
    def registry_path():
        return AssociationTableRegistry.ASSOCIATION_TABLE_REGISTRY_PATH
