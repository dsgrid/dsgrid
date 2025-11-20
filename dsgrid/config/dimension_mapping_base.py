import logging


from pydantic import Field, ValidationInfo, field_validator, model_validator

from dsgrid.data_models import DSGBaseDatabaseModel, DSGBaseModel, DSGEnum, EnumValue
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionMapping
from .dimensions import DimensionReferenceModel

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


class DimensionMappingBaseModel(DSGBaseDatabaseModel):
    """Base class for mapping dimensions"""

    mapping_type: DimensionMappingType = Field(
        title="mapping_type",
        description="Type/purpose of the dimension mapping",
        default="many_to_one_aggregation",
        json_schema_extra={
            "options": DimensionMappingType.format_for_docs(),
        },
    )
    archetype: DimensionMappingArchetype | None = Field(
        default=None,
        title="archetype",
        description="Dimension mapping archetype, determined based on mapping_type",
        json_schema_extra={
            "dsgrid_internal": True,
            "options": DimensionMappingArchetype.format_for_docs(),
        },
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
        default=1e-6,
    )
    to_fraction_tolerance: float = Field(
        title="to_fraction_tolerance",
        description="Tolerance to apply when checking to_fraction column sums",
        default=1e-6,
    )
    description: str | None = Field(
        default=None,
        title="description",
        description="Description of dimension mapping",
    )
    mapping_id: str | None = Field(
        default=None,
        title="mapping_id",
        description="Unique dimension mapping identifier, generated by dsgrid",
        json_schema_extra={
            "dsgrid_internal": True,
            "updateable": False,
        },
    )

    @field_validator("archetype")
    @classmethod
    def check_archetype(cls, archetype, info: ValidationInfo):
        if "mapping_type" not in info.data:
            return archetype

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

        mapping_type = info.data["mapping_type"]
        assigned_archetype = archetype_assignment[mapping_type]
        if archetype is None:
            archetype = assigned_archetype
        elif archetype != assigned_archetype:
            msg = (
                '"mapping_type" and "archetype" are both defined AND they DO NOT correspond to each other. '
                "archetype can be removed from config so that it can be assigned automatically based on mapping_type. "
                f"Otherwise, {mapping_type=} should have archetype={assigned_archetype} "
            )
            raise DSGInvalidDimensionMapping(msg)
        return archetype


class DimensionMappingPreRegisteredBaseModel(DSGBaseModel):
    """Base class for mapping soon-to-be registered dimensions. As soon as the dimensions
    are registered this will be converted to a DimensionMappingBaseModel and then registered.
    """

    mapping_type: DimensionMappingType = Field(
        title="mapping_type",
        description="Type/purpose of the dimension mapping",
        default="many_to_one_aggregation",
        json_schema_extra={
            "options": DimensionMappingType.format_for_docs(),
        },
    )
    archetype: DimensionMappingArchetype | None = Field(
        default=None,
        title="archetype",
        description="Dimension mapping archetype, determined based on mapping_type",
        json_schema_extra={
            "dsgrid_internal": True,
            "options": DimensionMappingArchetype.format_for_docs(),
        },
    )
    description: str | None = Field(
        default=None,
        title="description",
        description="Description of dimension mapping",
    )
    from_fraction_tolerance: float = Field(
        title="from_fraction_tolerance",
        description="Tolerance value to apply to the from_fraction column",
        default=1e-6,
    )
    to_fraction_tolerance: float = Field(
        title="to_fraction_tolerance",
        description="Tolerance value to apply to the to_fraction column",
        default=1e-6,
    )
    project_base_dimension_name: str | None = Field(
        default=None,
        description="Name of the base dimension for which the mapping is being registered. "
        "This is required in cases where the project has multiple base dimensions of the same "
        "type. If None, there must only be one base dimension of this type in the project.",
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
        json_schema_extra={
            "options": DimensionType.format_for_docs(),
        },
    )
    to_dimension_type: DimensionType = Field(
        title="to_dimension_type",
        description="Dimension Type",
        json_schema_extra={
            "options": DimensionType.format_for_docs(),
        },
    )
    mapping_id: str = Field(
        title="mapping_id",
        description="Unique ID of the dimension mapping",
        json_schema_extra={
            "updateable": False,
        },
    )
    version: str = Field(
        title="version",
        description="Version of the dimension",
    )

    # This function can be deleted once all dataset repositories have been updated.
    @model_validator(mode="before")
    @classmethod
    def handle_legacy_fields(cls, values):
        if "required_for_validation" in values:
            logger.warning(
                "Removing deprecated required_for_validation field from a dimension mapping reference."
            )
            values.pop("required_for_validation")

        return values


class DimensionMappingReferenceListModel(DSGBaseModel):
    """List of dimension mapping references used by the dimensions_mappings.json5 config"""

    references: list[DimensionMappingReferenceModel] = Field(
        title="references",
        description="List of dimension mapping references",
    )
