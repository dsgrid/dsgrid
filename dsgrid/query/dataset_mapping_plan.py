import logging

from pydantic import Field, field_validator

from dsgrid.data_models import DSGBaseModel

from dsgrid.utils.utilities import check_uniqueness
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel


logger = logging.getLogger(__name__)


class MapDimensionOperation(DSGBaseModel):

    dimension_name: str = Field(
        description="Name of the project base dimension to which the dataset will be mapped.",
    )
    handle_data_skew: bool | None = Field(
        default=None,
        description="Use a salting technique to handle data skew in this mapping "
        "operation. Skew can happen when some partitions have significantly more data than "
        "others, resulting in unbalanced task execution times. "
        "If this value is None, dsgrid will make its own determination of whether this "
        "should be done based on the characteristics of the mapping operation. Setting it "
        "to True or False will override that behavior and inform dsgrid of what to do. "
        "This will automatically trigger a persist to the filesystem (implicitly setting "
        "persist to True).",
    )
    persist: bool = Field(
        default=False,
        description="Persist the intermediate dataset to the filesystem after mapping "
        "this dimension. This can be useful to prevent the query from becoming too "
        "large. It can also be useful for benchmarking and debugging purposes.",
    )
    mapping_reference: DimensionMappingReferenceModel | None = Field(
        default=None,
        description="Reference to the model used to map the dimension. "
        "Set at runtime by dsgrid.",
    )


class DatasetMappingPlan(DSGBaseModel):
    """Defines how to map a dataset to a list of dimensions."""

    dataset_id: str
    mappings: list[MapDimensionOperation]

    @field_validator("mappings")
    @classmethod
    def check_dimension_names(
        cls, mappings: list[MapDimensionOperation]
    ) -> list[MapDimensionOperation]:
        check_uniqueness((x.dimension_name for x in mappings), "dimension_name")
        return mappings

    # TODO: add checkpointing: issue #355
