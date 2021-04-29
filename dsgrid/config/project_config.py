"""
# ******************************************
# RUNNING LIST OF PROJECT CONFIG TODOS
# ******************************************

- we need to establish relationships across project-dimensions (to/from or
    some kind of mapper)
- need to establish dsgrid types standard relationship mappers
- need to use said mapper to run compatibility checks
- need to better establish expected fields/types in the dimension dataclasses
    in dsgrid.dimension.standard
- enforce supplemental dimensions to have a from/to mapping to the project
    dimension of the same type?
- I think we need to add the association table to
    dimensions.associations.project_dimensions in the config

- Add registry details
- need to generate input data config

"""
import itertools
import logging
from typing import Dict, List, Optional, Union

from pydantic import Field
from pydantic import root_validator, validator
from semver import VersionInfo

from .config_base import ConfigBase
from .dimension_mapping_base import DimensionMappingReferenceListModel
from .dimensions import (
    DimensionReferenceModel,
    DimensionType,
)
from dsgrid.exceptions import DSGInvalidField, DSGInvalidDimensionMapping
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import DatasetRegistryStatus, check_config_id_2

from dsgrid.utils.utilities import check_uniqueness
from dsgrid.utils.versioning import handle_version_or_str

# from dsgrid.dimension.time import (
#     LeapDayAdjustmentType, Period, TimeValueMeasurement, TimeFrequency,
#     TimezoneType
#     )

LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"

logger = logging.getLogger(__name__)


class DimensionsModel(DSGBaseModel):
    """Contains dimensions defined by a dataset"""

    project_dimensions: List[DimensionReferenceModel] = Field(
        title="project_dimensions",
        description="dimensions defined by the project",
    )
    supplemental_dimensions: Optional[List[DimensionReferenceModel]] = Field(
        title="supplemental_dimensions",
        description="supplemental dimensions",
        default=[],
    )

    @validator("project_dimensions")
    def check_project_dimension(cls, val):
        """Validate project_dimensions types"""
        dimension_types = [i.dimension_type for i in val]
        required_dim_types = list(DimensionType)
        # validate required dimensions for project_dimensions
        for i in required_dim_types:
            if i not in dimension_types:
                raise ValueError(
                    f"Required project dimension {i} is not in project ",
                    "config project.project_dimensions",
                )
        check_uniqueness(dimension_types, "project_dimension")
        return val

    @root_validator
    def check_dimension_mappings(cls, values: dict) -> dict:
        """validates that a
        check that keys exist in both jsons
        check that all from_keys have a match in the to_keys json

        """
        # supplemental_mapping = {
        #   x.name: x.cls for x in values["supplemental_dimensions"]}
        # Should already have been checked.
        # assert len(supplemental_mapping) == \
        #   len(values["supplemental_dimensions"])
        # for dim in values["project_dimensions"]:
        #    mappings = getattr(dim, "mappings", [])
        #    # TODO: other mapping types
        #    for mapping in (
        #       x for x in mappings if isinstance(x, DimensionDirectMapping)):
        #        to_dim = supplemental_mapping.get(mapping.to_dimension)
        #        if to_dim is None:
        #            raise ValueError(
        #               f"dimension {mapping.to_dimension} is not stored in"
        #               f"supplemental_dimensions"
        #            )
        #        mapping.to_dimension = to_dim

        return values


class InputDatasetModel(DSGBaseModel):
    """Defines an input dataset"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset ID",
    )
    dataset_type: str = Field(  # TODO this needs to be ENUM
        title="dataset_type",
        description="Dataset Type",
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="version of the dataset",
    )
    status: Optional[DatasetRegistryStatus] = Field(
        title="status",
        description="registration status of the dataset",
        default=DatasetRegistryStatus.REGISTERED,
    )
    # TODO this model_sector must be validated in the dataset_config
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    # TODO: is this needed?
    # sectors: List[str] = Field(
    #    title="sectors",
    #    description="sectors used in the project",
    # )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)


class InputDatasetsModel(DSGBaseModel):
    """Defines all input datasets for a project"""

    # TODO: incorrect
    benchmark: List[str] = Field(
        title="benchmark",
        default=[],
        description="benchmark",
    )
    # TODO: incorrect
    historical: List[str] = Field(
        title="historical",
        default=[],
        description="historical",
    )
    datasets: List[InputDatasetModel] = Field(
        title="datasets",
        description="project input datasets",
    )

    # TODO:
    #   - Check for unique dataset IDs
    #   - check model_name, model_sector, sectors are all expected and align
    #   with the dimension records


class DimensionMappingsModel(DSGBaseModel):
    """Defines intra-project and dataset-to-project dimension mappings."""

    project: Optional[List[DimensionMappingReferenceListModel]] = Field(
        title="project",
        description="intra-project mappings",
        default=[],
    )
    datasets: Optional[Dict[str, DimensionMappingReferenceListModel]] = Field(
        title="datasets",
        description="dataset-to-project mappings for each registered dataset",
        default={},
    )


class ProjectConfigModel(DSGBaseModel):
    """Represents project configurations"""

    project_id: str = Field(
        title="project_id",
        description="project identifier",
    )
    name: str = Field(
        title="name",
        description="project name",
    )
    input_datasets: InputDatasetsModel = Field(
        title="input_datasets",
        description="input datasets for the project",
    )
    dimensions: DimensionsModel = Field(
        title="dimensions",
        description="dimensions",
    )
    dimension_mappings: Optional[DimensionMappingsModel] = Field(
        title="dimension_mappings",
        description="intra-project and dataset-to-project dimension mappings",
        default=DimensionMappingsModel(),
    )
    registration: Optional[Dict] = Field(
        title="registration",
        description="registration information",
    )
    description: str = Field(
        title="description",
        description="describe project in details",
    )

    @validator("project_id")
    def check_project_id_handle(cls, project_id):
        """Check for valid characters in project id"""
        # TODO: any other invalid character for the project_id?
        # TODO: may want to check for pre-existing project_id
        #       (e.g., LA100 Run 1 vs. LA100 Run 0 kind of thing)
        if "-" in project_id:
            raise ValueError('invalid character "-" in project id')

        check_config_id_2(project_id, "Project")
        return project_id


class ProjectConfig(ConfigBase):
    """Provides an interface to a ProjectConfigModel."""

    def __init__(self, model):
        super().__init__(model)
        self._project_dimensions = {}
        self._supplemental_dimensions = {}

    @staticmethod
    def model_class():
        return ProjectConfigModel

    @classmethod
    def load(cls, config_file, dimension_manager):
        config = cls._load(config_file)
        config.load_dimensions(dimension_manager)
        return config

    def load_dimensions(self, dimension_manager):
        """Load all project dimensions.

        Parameters
        ----------
        dimension_manager : DimensionRegistryManager

        """
        project_dimensions = dimension_manager.load_dimensions(
            self.model.dimensions.project_dimensions
        )
        supplemental_dimensions = dimension_manager.load_dimensions(
            self.model.dimensions.supplemental_dimensions
        )
        dims = itertools.chain(project_dimensions.values(), supplemental_dimensions.values())
        check_uniqueness((x.name for x in dims), "dimension name")
        check_uniqueness((getattr(x, "cls") for x in dims), "dimension cls")

        self._project_dimensions.update(project_dimensions)
        self._supplemental_dimensions.update(supplemental_dimensions)

    def add_dataset_dimension_mappings(self, dataset_config, references):
        """Add a dataset's dimension mappings to the project.

        Parameters
        ----------
        dataset_config : DatasetConfig
        references : list
            list of DimensionMappingReferenceModel

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if a requirement is violated.

        """
        self.check_dataset_dimension_mappings(dataset_config, references)
        if dataset_config.model.dataset_id not in self.model.dimension_mappings:
            self.model.dimension_mappings.datasets[
                dataset_config.model.dataset_id
            ] = DimensionMappingReferenceListModel(references=[])
        mappings = self.model.dimension_mappings.datasets[dataset_config.model.dataset_id]
        existing_ids = set((x.mapping_id for x in mappings.references))
        for reference in references:
            if reference.mapping_id not in existing_ids:
                mappings.references.append(reference)
                logger.info(
                    "Added dimension mapping for dataset=%s: %s",
                    dataset_config.model.dataset_id,
                    reference.mapping_id,
                )

    def check_dataset_dimension_mappings(
        self, dataset_config, references: DimensionMappingReferenceListModel
    ):
        """Check that a dataset provides required mappings to the project.

        Parameters
        ----------
        dataset_config : DatasetConfig
        references : list
            list of DimensionMappingReferenceModel

        Raises
        ------
        DSGInvalidDimensionMapping
            Raised if a requirement is violated.

        """
        # The dataset has to have each project dimension or provide a mapping.
        project_keys = set(self.project_dimensions.keys())
        dataset_keys = set(dataset_config.dimensions)
        requires_mapping = project_keys.difference(dataset_keys)
        # TODO: re-enable when dimension mappings are fixed
        # if requires_mapping:
        #    raise DSGInvalidDimensionMapping(
        #        f"dataset {dataset_config.model.dataset_id} has missing dimension mappings: {requires_mapping}"
        #    )

        # TODO: handle dimension_mappings

    def get_dataset(self, dataset_id):
        """Return a dataset by ID."""
        for dataset in self.model.input_datasets.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset

        raise DSGInvalidField(f"no dataset with dataset_id={dataset_id}")

    def has_dataset(self, dataset_id):
        """Return True if the dataset_id is present in the configuration."""
        for _id in self.iter_dataset_ids():
            if _id == dataset_id:
                return True

        # TODO DT: what about benchmark and historical?
        return False

    def iter_datasets(self):
        for dataset in self.model.input_datasets.datasets:
            yield dataset

    def iter_dataset_ids(self):
        for dataset in self.model.input_datasets.datasets:
            yield dataset.dataset_id

    @property
    def project_dimensions(self):
        """Return the project dimensions.

        Returns
        -------
        dict
            dict of DimensionBaseModel keyed by DimensionKey

        """
        return self._project_dimensions

    @property
    def supplemental_dimensions(self):
        """Return the supplemental dimensions.

        Returns
        -------
        dict
            dict of DimensionBaseModel keyed by DimensionKey

        """
        return self._supplemental_dimensions
