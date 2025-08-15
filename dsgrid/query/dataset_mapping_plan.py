import logging
from datetime import datetime
from pathlib import Path
from typing import Self

from pydantic import Field, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import compute_hash
from dsgrid.utils.utilities import check_uniqueness
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel


logger = logging.getLogger(__name__)


class MapOperation(DSGBaseModel):
    """Defines one mapping operation for a dataset."""

    name: str = Field(
        description="Identifier for the mapping operation. This must be a unique name.",
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
        description="Reference to the model used to map the dimension. Set at runtime by dsgrid.",
    )


class MapOperationCheckpoint(DSGBaseModel):
    """Defines a completed mapping operation that has been persisted to the filesystem."""

    dataset_id: str
    completed_operation_names: list[str] = Field(
        description="Names of the completed mapping operations."
    )
    persisted_table_filename: Path = Field(description="Path to a persisted file.")
    mapping_plan_hash: str = Field(
        description="Hash of the mapping plan. This is used to ensure that the mapping plan "
        "hasn't changed."
    )
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp of when the operation was completed.",
    )


class DatasetMappingPlan(DSGBaseModel):
    """Defines how to map a dataset to a list of dimensions."""

    dataset_id: str = Field(
        description="ID of the dataset to be mapped.",
    )
    mappings: list[MapOperation] = Field(
        default=[],
        description="Defines how to map each dimension of the dataset.",
    )
    apply_fraction_op: MapOperation = Field(
        default=MapOperation(
            name="apply_fraction_op",
            handle_data_skew=False,
            persist=False,
        ),
        description="Defines handling of the query that applies the from_fraction value after "
        "mapping all dimensions.",
    )
    apply_scaling_factor_op: MapOperation = Field(
        default=MapOperation(
            name="apply_scaling_factor_op",
            handle_data_skew=False,
            persist=False,
        ),
        description="Defines handling of the query that applies the scaling factor, if one exists. "
        "This happens after apply_fraction_op.",
    )
    convert_units_op: MapOperation = Field(
        default=MapOperation(
            name="convert_units_op",
            handle_data_skew=False,
            persist=False,
        ),
        description="Defines handling of the query that converts units. This happens after "
        "apply_fraction_op and before mapping time. It is strongly recommended to not persist "
        "this table because the code currently always persists before mapping time.",
    )
    map_time_op: MapOperation = Field(
        default=MapOperation(
            name="map_time",
            handle_data_skew=False,
            persist=False,
        ),
        description="Defines handling of the query that maps the time dimension. This happens after "
        "convert_units_op. Unlike the other dimension mappings, this does not use the generic "
        "mapping code. It relies on specific handling in chronify by time type.",
    )
    keep_intermediate_files: bool = Field(
        default=False,
        description="If True, keep the intermediate tables created during the mapping process. "
        "This is useful for debugging and benchmarking, but will consume more disk space.",
    )

    @model_validator(mode="after")
    def check_names(self) -> Self:
        names = [x.name for x in self.mappings] + [
            self.apply_fraction_op.name,
            self.apply_scaling_factor_op.name,
            self.convert_units_op.name,
            self.map_time_op.name,
        ]
        check_uniqueness(names, "name")
        return self

    def list_mapping_operations(self) -> list[MapOperation]:
        """List all mapping operations in the plan, in order."""
        return self.mappings + [
            self.apply_fraction_op,
            self.apply_scaling_factor_op,
            self.convert_units_op,
            self.map_time_op,
        ]

    def compute_hash(self) -> str:
        """Compute a hash of the mapping plan."""
        return compute_hash(
            bytes(self.model_dump_json(exclude={"keep_intermediate_files"}).encode("utf-8"))
        )
