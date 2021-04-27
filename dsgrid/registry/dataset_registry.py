"""Manages registry for a dataset"""

import logging
from pathlib import Path


from pydantic import Field

from .registry_base import RegistryBaseModel, RegistryBase
from dsgrid.filesytem.factory import make_filesystem_interface
from dsgrid.common import REMOTE_REGISTRY

logger = logging.getLogger(__name__)


class DatasetRegistryModel(RegistryBaseModel):
    """Dataset registration class"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset identifier",
    )


class DatasetRegistry(RegistryBase):
    """Controls dataset registration"""

    DATASET_REGISTRY_PATH = Path("configs/datasets")
    DATASET_REGISTRY_REMOTE_PATH = f"{REMOTE_REGISTRY}/{DATASET_REGISTRY_PATH}/"

    @staticmethod
    def config_filename():
        return "dataset.toml"

    @property
    def dataset_id(self):
        return self._model.dataset_id

    @staticmethod
    def cloud_interface(self):
        return make_filesystem_interface(DatasetRegistry.DATASET_REGISTRY_REMOTE_PATH)

    @staticmethod
    def model_class():
        return DatasetRegistryModel

    @staticmethod
    def registry_path():
        return DatasetRegistry.DATASET_REGISTRY_PATH

    @staticmethod
    def registry_remote_path():
        return DatasetRegistry.DATASET_REGISTRY_REMOTE_PATH

    @staticmethod
    def sync_push(local_registry_path):
        DatasetRegistry.cloud_interface.sync_push(
            local_registry=local_registry_path / DatasetRegistry.registry_path(),
            remote_registry=DatasetRegistry.registry_remote_path(),
        )

    @staticmethod
    def sync_pull(local_registry_path):
        DatasetRegistry.cloud_interface.sync_pull(
            DatasetRegistry.registry_remote_path(),
            local_registry=local_registry_path / DatasetRegistry.registry_path(),
        )
