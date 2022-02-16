import logging
import os
from typing import Dict, List, Optional, Union

from pydantic import Field, validator
from semver import VersionInfo

from .dimensions import DimensionReferenceModel
from dsgrid.data_models import DSGBaseModel, DSGEnum
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.versioning import handle_version_or_str
from dsgrid.exceptions import DSGInvalidDimensionMapping


logger = logging.getLogger(__name__)


class DimensionMappingType(DSGEnum):
    # default from_fraction col, FRACTION_SUM_EQ1
    ONE_TO_ONE = "one_to_one"  # includes rename, down-selection
    MANY_TO_ONE_AGGREGATION = "many_to_one_aggregation"

    # default from_fraction col, FRACTION_SUM_NOT_EQ1
    DUPLICATION = "duplication"

    # input from_fraction col, FRACTION_SUM_EQ1
    MANY_TO_MANY_AGGREGATION = "many_to_many_aggregation"
    ONE_TO_MANY_DISAGGREGATION = "one_to_many_disaggregation"
    MANY_TO_MANY_DISAGGREGATION = "many_to_many_disaggregation"

    # input from_fraction col, FRACTION_SUM_NOT_EQ1
    ONE_TO_ONE_MULTIPLICATION = "one_to_one_multiplication"
    ONE_TO_MANY_MULTIPLICATION = "one_to_many_multiplication"
    MANY_TO_ONE_MULTIPLICATION = "many_to_one_multiplication"
    MANY_TO_MANY_MULTIPLICATION = "many_to_many_multiplication"


class DimensionMappingArchetype(DSGEnum):
    """Dimension mapping archetype, used to check:
    - whether duplicates are allowed in from and to dimension;
    - whether sum of from_fraction should be =, <, or > 1 when group by from_id
    """

    ONE_TO_ONE_MAP_FRACTION_SUM_EQ1 = "one_to_one_map_fraction_sum_eq1"  # unique from and to
    ONE_TO_MANY_MAP_FRACTION_SUM_EQ1 = "one_to_many_map_fraction_sum_eq1"  # dup from, unique to
    MANY_TO_ONE_MAP_FRACTION_SUM_EQ1 = "many_to_one_map_fraction_sum_eq1"  # unique from, dup to
    MANY_TO_MANY_MAP_FRACTION_SUM_EQ1 = "many_to_many_map_fraction_sum_eq1"  # dup from and to

    ONE_TO_ONE_MAP_FRACTION_SUM_NOT_EQ1 = "one_to_one_map_fraction_sum_not_eq1"
    ONE_TO_MANY_MAP_FRACTION_SUM_NOT_EQ1 = "one_to_many_map_fraction_sum_not_eq1"
    MANY_TO_ONE_MAP_FRACTION_SUM_NOT_EQ1 = "many_to_one_map_fraction_sum_not_eq1"
    MANY_TO_MANY_MAP_FRACTION_SUM_NOT_EQ1 = "many_to_many_map_fraction_sum_not_eq1"


class DimensionMappingBaseModel(DSGBaseModel):
    """Base class for mapping dimensions"""

    mapping_type: DimensionMappingType = Field(
        title="mapping_type",
        description="Type/purpose of the dimension mapping",
        default="many_to_one_aggregation",
        options=DimensionMappingType.format_for_docs(),
    )
    archetype: Optional[DimensionMappingArchetype] = Field(
        title="archetype",
        description="Dimension mapping archetype, determined based on mapping_type",
        options=DimensionMappingArchetype.format_for_docs(),
    )
    from_dimension: DimensionReferenceModel = Field(
        title="from_dimension",
        description="From dimension",
    )
    to_dimension: DimensionReferenceModel = Field(
        title="to_dimension",
        description="To dimension",
    )
    description: str = Field(
        title="description",
        description="Description of dimension mapping",
    )
    mapping_id: Optional[str] = Field(
        title="mapping_id",
        alias="id",
        description="Unique dimension mapping identifier, generated by dsgrid",
        dsg_internal=True,
        updateable=False,
    )

    @validator("archetype")
    def check_archetype(cls, archetype, values):
        # default from_fraction col, FRACTION_SUM_EQ1
        if values["mapping_type"] == DimensionMappingType.ONE_TO_ONE:
            assigned_archetype = DimensionMappingArchetype.ONE_TO_ONE_MAP_FRACTION_SUM_EQ1
        elif values["mapping_type"] == DimensionMappingType.MANY_TO_ONE_AGGREGATION:
            assigned_archetype = DimensionMappingArchetype.MANY_TO_ONE_MAP_FRACTION_SUM_EQ1

        # default from_fraction col, FRACTION_SUM_NOT_EQ1
        elif values["mapping_type"] == DimensionMappingType.DUPLICATION:
            assigned_archetype = DimensionMappingArchetype.ONE_TO_MANY_MAP_FRACTION_SUM_NOT_EQ1

        # from_fraction col, FRACTION_SUM_EQ1
        elif values["mapping_type"] == DimensionMappingType.MANY_TO_MANY_AGGREGATION:
            assigned_archetype = DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_EQ1
        elif values["mapping_type"] == DimensionMappingType.ONE_TO_MANY_DISAGGREGATION:
            assigned_archetype = DimensionMappingArchetype.ONE_TO_MANY_MAP_FRACTION_SUM_EQ1
        elif values["mapping_type"] == DimensionMappingType.MANY_TO_MANY_DISAGGREGATION:
            assigned_archetype = DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_EQ1

        # input from_fraction col, FRACTION_SUM_NOT_EQ1
        elif values["mapping_type"] == DimensionMappingType.ONE_TO_ONE_MULTIPLICATION:
            assigned_archetype = DimensionMappingArchetype.ONE_TO_ONE_MAP_FRACTION_SUM_NOT_EQ1
        elif values["mapping_type"] == DimensionMappingType.ONE_TO_MANY_MULTIPLICATION:
            assigned_archetype = DimensionMappingArchetype.ONE_TO_MANY_MAP_FRACTION_SUM_NOT_EQ1
        elif values["mapping_type"] == DimensionMappingType.MANY_TO_ONE_MULTIPLICATION:
            assigned_archetype = DimensionMappingArchetype.MANY_TO_ONE_MAP_FRACTION_SUM_NOT_EQ1
        elif values["mapping_type"] == DimensionMappingType.MANY_TO_MANY_MULTIPLICATION:
            assigned_archetype = DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_NOT_EQ1

        if archetype == None:
            archetype = assigned_archetype
        else:
            if archetype != assigned_archetype:
                raise DSGInvalidDimensionMapping(
                    '"mapping_type" and "archetype" are both defined. '
                    'To assign archetype based on mapping_type, remove "archetype" from config. '
                    f'Otherwise, mapping_type={values["mapping_type"]} should have archetype={assigned_archetype} '
                )
        return archetype


class DimensionMappingReferenceModel(DSGBaseModel):
    """Reference to a dimension mapping stored in the registry.

    The DimensionMappingReferenceModel is utilized by the project configuration (project.toml) as well as by the dimension mapping reference configuration (dimension_mapping_references.toml) that may be required when submitting a dataset to a project.
    """

    from_dimension_type: DimensionType = Field(
        title="from_dimension_type",
        description="Dimension Type",
        options=DimensionType.format_for_docs(),
    )
    to_dimension_type: DimensionType = Field(
        title="to_dimension_type",
        description="Dimension Type",
        options=DimensionType.format_for_docs(),
    )
    mapping_id: str = Field(
        title="mapping_id",
        description="Unique ID of the dimension mapping",
        updateable=False,
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="Version of the dimension",
        # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
    )
    required_for_validation: Optional[bool] = Field(
        title="version",
        description="Set to False if a given base-to-base dimension mapping is NOT required for input dataset validation; default is True",  # outdated, FIXME
        default=True,
        # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
    )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)

    # @validator("required_for_validation")
    # def check_required_for_validation_field(cls, value):
    #     # TODO if base_to_supplemental, raise error
    #     return value


class DimensionMappingReferenceListModel(DSGBaseModel):
    """List of dimension mapping references used by the dimensions_mappings.toml config"""

    references: List[DimensionMappingReferenceModel] = Field(
        title="references",
        description="List of dimension mapping references",
    )
