import logging
import os
from typing import Dict, List, Optional, Union

from pydantic import Field, validator
from semver import VersionInfo

from .association_tables import AssociationTableModel
from .config_base import ConfigBase
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import make_registry_id, check_config_id_1


logger = logging.getLogger(__name__)


class DimensionMappingsConfigModel(DSGBaseModel):
    """Represents dimension mapping model configurations"""

    # This may eventually change to a Union if there are more subclasses.
    mappings: List[AssociationTableModel] = Field(
        title="mappings",
        description="dimension mappings between and within projects and datasets",
    )


class DimensionMappingsConfig(ConfigBase):
    """Provides an interface to a DimensionMappingsConfigModel."""

    @staticmethod
    def config_filename():
        return "dimension_mappings.toml"

    @property
    def config_id(self):
        return self._model.dimension_mapping_id

    @staticmethod
    def model_class():
        return DimensionMappingsConfigModel

    def assign_ids(self):
        """Assign unique IDs to each mapping in the config"""
        for mapping in self.model.mappings:
            from_type = mapping.from_dimension.dimension_id.split("__")[0]
            to_type = mapping.to_dimension.dimension_id.split("__")[0]
            mapping_id = make_registry_id((from_type, to_type))
            check_config_id_1(mapping_id, "Dimension Mapping")
            mapping.mapping_id = mapping_id
