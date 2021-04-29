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


class DimensionMappingConfigModel(DSGBaseModel):
    """Represents dimension mapping model configurations"""

    # This may eventually change to a Union if there are more subclasses.
    mappings: List[AssociationTableModel] = Field(
        title="mappings",
        description="dimension mappings between and within projects and datasets",
    )
    registration: Optional[Dict] = Field(
        title="registration",
        description="registration information",
    )


class DimensionMappingConfig(ConfigBase):
    """Provides an interface to a DimensionMappingConfigModel."""

    @staticmethod
    def model_class():
        return DimensionMappingConfigModel

    def assign_ids(self):
        """Assign unique IDs to each mapping in the config"""
        for mapping in self.model.mappings:
            from_type = mapping.from_dimension.dimension_type
            to_type = mapping.to_dimension.dimension_type
            mapping_id = make_registry_id((from_type.value, to_type.value))
            check_config_id_1(mapping_id, "Dimension Mapping")
            mapping.mapping_id = mapping_id
