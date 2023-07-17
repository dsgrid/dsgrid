import logging
from typing import Optional

from pydantic import Field, validator

from dsgrid.data_models import DSGBaseModel, DSGEnum, EnumValue
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping
from .dimensions import DimensionReferenceModel, DimensionReferenceByNameModel

logger = logging.getLogger(__name__)


class DimensionMappingType(DSGEnum):
    """Defines the operation dsgrid will apply to the data during a mapping."""

    # optional from_fraction col, FRACTION_SUM_EQ1 when grouped by from_id
    ONE_TO_ONE = "one_to_one"  # includes rename, down-selection
    MANY_TO_ONE_AGGREGATION = "many_to_one_aggregation"
    MANY_TO_ONE_REASSIGNMENT = "many_to_one_reassignment"

    # optional from_fraction col, no FRACTION_SUM check
    DUPLICATION = "duplication"

    # required from_fraction col, FRACTION_SUM_EQ1 when grouped by from_id
    ONE_TO_MANY_DISAGGREGATION = "one_to_many_disaggregation"
    MANY_TO_MANY_AGGREGATION = "many_to_many_aggregation"
    MANY_TO_MANY_DISAGGREGATION = "many_to_many_disaggregation"

    # required from_fraction col, FRACTION_SUM_EQ1 when grouped by to_id
    MANY_TO_ONE_ASSIGNMENT = "many_to_one_assignment"
    ONE_TO_MANY_ASSIGNMENT = "one_to_many_assignment"
    MANY_TO_MANY_ASSIGNMENT = "many_to_many_assignment"

    # required from_fraction col, no FRACTION_SUM check
    ONE_TO_ONE_EXPLICIT_MULTIPLIERS = "one_to_one_explicit_multipliers"
    ONE_TO_MANY_EXPLICIT_MULTIPLIERS = "one_to_many_explicit_multipliers"
    MANY_TO_ONE_EXPLICIT_MULTIPLIERS = "many_to_one_explicit_multipliers"
    MANY_TO_MANY_EXPLICIT_MULTIPLIERS = "many_to_many_explicit_multipliers"


class DimensionMappingArchetype(DSGEnum):
    """Dimension mapping archetype, used to check whether duplicates are allowed in from/to
    dimensions and apply rules about the sum of the from_fraction column.
    """

    ONE_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1 = EnumValue(
        value="one_to_one_map_fraction_sum_from_id_eq1",
        description="One-to-one dimension mapping with sum of from_fraction = 1 when grouped by from_id",
        allow_dup_from_records=False,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=True,
        check_fraction_sum_eq1_to_id=False,
    )
    ONE_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1 = EnumValue(
        value="one_to_many_map_fraction_sum_from_id_eq1",
        description="One-to-many dimension mapping with sum of from_fraction = 1 when grouped by from_id",
        allow_dup_from_records=True,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=True,
        check_fraction_sum_eq1_to_id=False,
    )
    MANY_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1 = EnumValue(
        value="many_to_one_map_fraction_sum_from_id_eq1",
        description="Many-to-one dimension mapping with sum of from_fraction = 1 when grouped by from_id",
        allow_dup_from_records=False,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=True,
        check_fraction_sum_eq1_to_id=False,
    )
    MANY_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1 = EnumValue(
        value="many_to_many_map_fraction_sum_from_id_eq1",
        description="Many-to-many dimension mapping with sum of from_fraction = 1 when grouped by from_id",
        allow_dup_from_records=True,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=True,
        check_fraction_sum_eq1_to_id=False,
    )

    ONE_TO_ONE_MAP_FRACTION_SUM_TO_ID_EQ1 = EnumValue(
        value="one_to_one_map_fraction_sum_to_id_eq1",
        description="One-to-one dimension mapping with sum of from_fraction = 1 when grouped by to_id",
        allow_dup_from_records=False,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=True,
    )
    ONE_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1 = EnumValue(
        value="one_to_many_map_fraction_sum_to_id_eq1",
        description="One-to-many dimension mapping with sum of from_fraction = 1 when grouped by to_id",
        allow_dup_from_records=True,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=True,
    )
    MANY_TO_ONE_MAP_FRACTION_SUM_TO_ID_EQ1 = EnumValue(
        value="many_to_one_map_fraction_sum_to_id_eq1",
        description="Many-to-one dimension mapping with sum of from_fraction = 1 when grouped by to_id",
        allow_dup_from_records=False,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=True,
    )
    MANY_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1 = EnumValue(
        value="many_to_many_map_fraction_sum_to_id_eq1",
        description="Many-to-many dimension mapping with sum of from_fraction = 1 when grouped by to_id",
        allow_dup_from_records=True,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=True,
    )

    ONE_TO_ONE_MAP = EnumValue(
        value="one_to_one_map",
        description="One-to-one dimension mapping with no from_fraction sum check",
        allow_dup_from_records=False,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=False,
    )
    ONE_TO_MANY_MAP = EnumValue(
        value="one_to_many_map",
        description="One-to-many dimension mapping with no from_fraction sum check",
        allow_dup_from_records=True,
        allow_dup_to_records=False,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=False,
    )
    MANY_TO_ONE_MAP = EnumValue(
        value="many_to_one_map",
        description="Many-to-one dimension mapping with no from_fraction sum check",
        allow_dup_from_records=False,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=False,
    )
    MANY_TO_MANY_MAP = EnumValue(
        value="many_to_many_map",
        description="Many-to-many dimension mapping with no from_fraction sum check",
        allow_dup_from_records=True,
        allow_dup_to_records=True,
        check_fraction_sum_eq1_from_id=False,
        check_fraction_sum_eq1_to_id=False,
    )


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
        dsg_internal=True,
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
    from_fraction_tolerance: float = Field(
        title="from_fraction_tolerance",
        description="Tolerance to apply when checking from_fraction column sums",
        default=1e-9,
    )
    to_fraction_tolerance: float = Field(
        title="to_fraction_tolerance",
        description="Tolerance to apply when checking to_fraction column sums",
        default=1e-9,
    )
    description: str = Field(
        title="description",
        description="Description of dimension mapping",
    )
    mapping_id: Optional[str] = Field(
        title="mapping_id",
        description="Unique dimension mapping identifier, generated by dsgrid",
        dsg_internal=True,
        updateable=False,
    )
    version: Optional[str] = Field(
        title="version",
        description="Version, generated by dsgrid",
        dsg_internal=True,
        updateable=False,
    )
    id: Optional[str] = Field(
        alias="_id",
        description="Registry database ID",
        dsgrid_internal=True,
    )
    key: Optional[str] = Field(
        alias="_key",
        description="Registry database key",
        dsgrid_internal=True,
    )
    rev: Optional[str] = Field(
        alias="_rev",
        description="Registry database revision",
        dsgrid_internal=True,
    )

    @validator("archetype")
    def check_archetype(cls, archetype, values):

        archetype_assignment = {
            # optional from_fraction col, FRACTION_SUM_EQ1 when grouped by from_id
            DimensionMappingType.ONE_TO_ONE: DimensionMappingArchetype.ONE_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1,
            DimensionMappingType.MANY_TO_ONE_AGGREGATION: DimensionMappingArchetype.MANY_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1,
            DimensionMappingType.MANY_TO_ONE_REASSIGNMENT: DimensionMappingArchetype.MANY_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1,
            # optional from_fraction col, no FRACTION_SUM check
            DimensionMappingType.DUPLICATION: DimensionMappingArchetype.ONE_TO_MANY_MAP,
            # required from_fraction col, FRACTION_SUM_EQ1 when grouped by from_id
            DimensionMappingType.ONE_TO_MANY_DISAGGREGATION: DimensionMappingArchetype.ONE_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1,
            DimensionMappingType.MANY_TO_MANY_AGGREGATION: DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1,
            DimensionMappingType.MANY_TO_MANY_DISAGGREGATION: DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1,
            # required from_fraction col, FRACTION_SUM_EQ1 when grouped by to_id
            DimensionMappingType.ONE_TO_MANY_ASSIGNMENT: DimensionMappingArchetype.ONE_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1,
            DimensionMappingType.MANY_TO_ONE_ASSIGNMENT: DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1,
            DimensionMappingType.MANY_TO_MANY_ASSIGNMENT: DimensionMappingArchetype.MANY_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1,
            # required from_fraction col, no FRACTION_SUM check
            DimensionMappingType.ONE_TO_ONE_EXPLICIT_MULTIPLIERS: DimensionMappingArchetype.ONE_TO_ONE_MAP,
            DimensionMappingType.ONE_TO_MANY_EXPLICIT_MULTIPLIERS: DimensionMappingArchetype.ONE_TO_MANY_MAP,
            DimensionMappingType.MANY_TO_ONE_EXPLICIT_MULTIPLIERS: DimensionMappingArchetype.MANY_TO_ONE_MAP,
            DimensionMappingType.MANY_TO_MANY_EXPLICIT_MULTIPLIERS: DimensionMappingArchetype.MANY_TO_MANY_MAP,
        }

        if "mapping_type" in values:
            assigned_archetype = archetype_assignment[values["mapping_type"]]
            if archetype is None:
                archetype = assigned_archetype
            else:
                if archetype != assigned_archetype:
                    raise DSGInvalidDimensionMapping(
                        '"mapping_type" and "archetype" are both defined AND they DO NOT correspond to each other. '
                        "archetype can be removed from config so that it can be assigned automatically based on mapping_type. "
                        f'Otherwise, mapping_type={values["mapping_type"]} should have archetype={assigned_archetype} '
                    )
        return archetype


class DimensionMappingPreRegisteredBaseModel(DSGBaseModel):
    """Base class for mapping soon-to-be registered dimensions. As soon as the dimensions
    are registered this will be converted to a DimensionMappingBaseModel and then registered.
    """

    mapping_type: DimensionMappingType = Field(
        title="mapping_type",
        description="Type/purpose of the dimension mapping",
        default="many_to_one_aggregation",
        options=DimensionMappingType.format_for_docs(),
    )
    archetype: Optional[DimensionMappingArchetype] = Field(
        title="archetype",
        description="Dimension mapping archetype, determined based on mapping_type",
        dsg_internal=True,
        options=DimensionMappingArchetype.format_for_docs(),
    )
    description: str = Field(
        title="description",
        description="Description of dimension mapping",
    )
    from_fraction_tolerance: Optional[float] = Field(
        title="from_fraction_tolerance",
        description="Tolerance value to apply to the from_fraction column",
        default=1e-9,
    )
    to_fraction_tolerance: Optional[float] = Field(
        title="to_fraction_tolerance",
        description="Tolerance value to apply to the to_fraction column",
        default=1e-9,
    )


class DimensionMappingByNameBaseModel(DimensionMappingPreRegisteredBaseModel):
    """Base class for mapping soon-to-be registered dimensions by name. Used when automatically
    registering a project's dimensions and mappings along with the project.
    """

    from_dimension: DimensionReferenceByNameModel = Field(
        title="from_dimension", description="Reference to soon-to-be-registered from dimension"
    )
    to_dimension: DimensionReferenceByNameModel = Field(
        title="to_dimension", description="Reference to soon-to-be-registered to dimension"
    )


class DimensionMappingDatasetToProjectBaseModel(DimensionMappingPreRegisteredBaseModel):
    """Base class for mapping soon-to-be registered dimensions for a dataset. Used when
    automatically registering mappings while submitting a dataset to a project.
    """

    dimension_type: DimensionType = Field(
        title="dimension_type",
        description="Dimension types that will be mapped",
    )


class DimensionMappingReferenceModel(DSGBaseModel):
    """Reference to a dimension mapping stored in the registry.

    The DimensionMappingReferenceModel is utilized by the project configuration (project.json5) as well as by the
    dimension mapping reference configuration (dimension_mapping_references.json5) that may be required when submitting a dataset to a project.
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
    version: str = Field(
        title="version",
        description="Version of the dimension",
        # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
    )
    required_for_validation: Optional[bool] = Field(
        title="version",
        description="Set to False if a given dimension association is NOT required for input dataset validation; default is True",
        default=True,
        # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
    )

    # @validator("required_for_validation")
    # def check_required_for_validation_field(cls, value):
    #     # TODO if base_to_supplemental, raise error
    #     return value


class DimensionMappingReferenceListModel(DSGBaseModel):
    """List of dimension mapping references used by the dimensions_mappings.json5 config"""

    references: list[DimensionMappingReferenceModel] = Field(
        title="references",
        description="List of dimension mapping references",
    )
