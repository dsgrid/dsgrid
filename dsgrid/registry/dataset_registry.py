"""Manages registry for a dataset"""

import logging
from pathlib import Path


from pydantic import Field

from .registry_base import RegistryBaseModel, RegistryBase

logger = logging.getLogger(__name__)


class DatasetRegistryModel(RegistryBaseModel):
    """Dataset registration class"""

    dataset_id: str = Field(
        title="dataset_id",
        description="Dataset identifier",
    )


class DatasetRegistry(RegistryBase):
    """Controls dataset registration"""

    DATASET_REGISTRY_PATH = Path("configs/datasets")

    @staticmethod
    def config_filename():
        return "dataset.json5"

    @property
    def config_id(self):
        return self._model.dataset_id

    @staticmethod
    def model_class():
        return DatasetRegistryModel

    @staticmethod
    def registry_path():
        return DatasetRegistry.DATASET_REGISTRY_PATH
