"""Manages registry for a dataset"""

import logging
from pathlib import Path


from pydantic import Field

from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.aws import sync
from dsgrid.common import S3_REGISTRY

logger = logging.getLogger(__name__)


class DatasetRegistryModel(RegistryBaseModel):
    """Dataset registration class"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset identifier",
    )


class DatasetRegistry(RegistryBase):
    """Controls dataset registration"""

    DATASET_REGISTRY_PATH = Path("datasets")
    DATASET_REGISTRY_S3_PATH = f"{S3_REGISTRY}/datasets"

    @property
    def dataset_id(self):
        return self._model.dataset_id

    @staticmethod
    def model_class():
        return DatasetRegistryModel

    @staticmethod
    def registry_path():
        return DatasetRegistry.DATASET_REGISTRY_PATH

    @staticmethod
    def registry_s3_path():
        return DatasetRegistry.DATASET_REGISTRY_S3_PATH

    @staticmethod
    def sync_push():
        sync(DatasetRegistry.registry_path(), DatasetRegistry.registry_s3_path())

    @staticmethod
    def sync_pull():
        sync(DatasetRegistry.registry_s3_path(), DatasetRegistry.registry_path())
