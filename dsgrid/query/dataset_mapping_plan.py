import copy
import logging
from pathlib import Path

from pydantic import Field, field_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import compute_hash
from dsgrid.utils.utilities import check_uniqueness
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel


logger = logging.getLogger(__name__)


class MapDimensionOperation(DSGBaseModel):

    dimension_name: str = Field(
        description="Name of the project dimension to which the dataset will be mapped.",
    )
    handle_data_skew: bool | None = Field(
        default=None,
        description="Use a salting technique to handle data skew in this mapping "
        "operation. This will automatically trigger a persist to the filesystem. "
        "If the value is None, dsgrid will determine what to do at runtime.",
    )
    persist: bool = Field(
        default=False,
        description="Persist the query to the filesystem after completing this operation.",
    )
    mapping_reference: DimensionMappingReferenceModel | None = Field(
        default=None,
        description="Reference to the model used to map the dimension. "
        "Set at runtime by dsgrid.",
    )


class MappingJournal(DSGBaseModel):
    """Defines mapping operations that completed."""

    completed_mappings: list[str] = Field(
        default=[], description="Names of dimensions that completed their mapping operations."
    )
    table_path: Path | None = None

    def add_completed_mapping(self, dimension_name: str, path: Path) -> None:
        """Add a completed mapping to the journal."""
        self.completed_mappings.append(dimension_name)
        self.table_path = path
        logger.info("Add completed mapping for %s at %s", dimension_name, path)


class DatasetMappingPlan(DSGBaseModel):
    """Defines how to map a dataset to a list of dimensions."""

    dataset_id: str
    mappings: list[MapDimensionOperation]
    journal: MappingJournal = MappingJournal()

    @field_validator("mappings")
    @classmethod
    def check_dimension_names(
        cls, mappings: list[MapDimensionOperation]
    ) -> list[MapDimensionOperation]:
        check_uniqueness((x.dimension_name for x in mappings), "dimension_name")
        return mappings

    def make_model_hash(self) -> tuple[str, str]:
        """Return a hash that uniquely identifies this map operation.
        The output can be used to determine if a cached operation can be used.
        """
        model = copy.deepcopy(self)
        model.journal = MappingJournal()
        for mapping in model.mappings:
            mapping.handle_data_skew = None
            mapping.mapping_reference = None

        text = model.model_dump_json(indent=2)
        hash_value = compute_hash(text.encode())
        return text, hash_value
